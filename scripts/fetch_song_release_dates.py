#!/usr/bin/env python3
# scripts/fetch_song_release_dates.py
# For each Wikipedia song page, extract a release date.
# Strategy:
#   1) Get QID from pageprops; query Wikidata P577.
#   2) Fallback: fetch page wikitext and parse infobox "released" field.
#   3) Normalize to YYYY-MM-DD when possible; else YYYY-MM or YYYY.
#
# Input CSV must contain at least a "source_url" (Wikipedia page URL).
# Optional "title" column; if missing, title is derived from the URL path.
#
# Output: input columns + release_date, month, day, date_source.

import os
import re
import csv
import time
import json
import argparse
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote

import requests

IN_PATH_DEFAULT = "data/songs.csv"
OUT_PATH_DEFAULT = "data/songs_with_dates.csv"

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"

DATE_RX_ISO = re.compile(r"^\s*(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?\s*$")

STARTDATE_TMPL_RX = re.compile(
    r"\{\{\s*start[- _]?date(?:[^|}]*)\|(?P<y>\d{3,4})(?:\|(?P<m>\d{1,2}))?(?:\|(?P<d>\d{1,2}))?",
    flags=re.IGNORECASE,
)

# Typical infobox key names that hold release values
RELEASE_KEYS = (
    "released", "release_date", "released_date", "date", "release"
)

def ua_contact() -> str:
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")

def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": f"StrumSongDates/1.0 (+{ua_contact()})",
        "Accept": "application/json",
    })
    return s

def backoff_get(s: requests.Session, url: str, params: Dict = None, max_retries: int = 6, base_sleep: float = 0.8):
    sleep = base_sleep
    for attempt in range(1, max_retries + 1):
        r = s.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r
        if r.status_code in (400, 401, 403, 404, 405, 410):
            r.raise_for_status()
        if r.status_code in (429, 500, 502, 503, 504):
            if attempt == max_retries:
                r.raise_for_status()
            time.sleep(sleep)
            sleep *= 1.7
            continue
        r.raise_for_status()
    return None

def derive_title_from_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        # Expect /wiki/Page_Title
        parts = p.path.split("/")
        if len(parts) >= 3 and parts[1] == "wiki":
            return unquote(parts[2])
        # Mobile or other subpaths, try last segment
        if parts:
            return unquote(parts[-1])
    except Exception:
        pass
    return None

def mw_get_qid_for_title(s: requests.Session, title: str) -> Optional[str]:
    params = {
        "action": "query",
        "format": "json",
        "prop": "pageprops",
        "titles": title,
        "redirects": 1,
        "formatversion": 2,
    }
    r = backoff_get(s, WIKIPEDIA_API, params=params)
    data = r.json()
    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return None
    page = pages[0]
    if "pageprops" in page and "wikibase_item" in page["pageprops"]:
        return page["pageprops"]["wikibase_item"]
    return None

def wd_get_p577_date(s: requests.Session, qid: str) -> Optional[str]:
    url = WIKIDATA_ENTITY.format(qid=qid)
    r = backoff_get(s, url)
    data = r.json()
    entities = data.get("entities", {})
    ent = entities.get(qid)
    if not ent:
        return None
    claims = ent.get("claims", {})
    p577 = claims.get("P577")
    if not p577:
        return None
    # choose earliest time among P577 statements
    best_iso = None
    for st in p577:
        mainsnak = st.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        if datavalue.get("type") != "time":
            continue
        v = datavalue.get("value", {})
        time_str = v.get("time")  # like "+1991-05-20T00:00:00Z"
        precision = v.get("precision")  # 9=year, 10=month, 11=day
        if not time_str:
            continue
        iso = normalize_wikidata_time(time_str, precision)
        if iso:
            if (best_iso is None) or (iso < best_iso):
                best_iso = iso
    return best_iso

def normalize_wikidata_time(time_str: str, precision: int) -> Optional[str]:
    # Strip leading '+' and trailing 'T...'
    t = time_str.lstrip("+")
    if "T" in t:
        t = t.split("T", 1)[0]
    # t is now like 1991-05-20
    m = DATE_RX_ISO.match(t)
    if not m:
        return None
    y, mm, dd = m.groups()
    if precision >= 11 and dd:
        return f"{y}-{mm}-{dd}"
    if precision == 10 and mm:
        return f"{y}-{mm}"
    if precision == 9:
        return y
    # fallback
    return t

def mw_get_wikitext(s: requests.Session, title: str) -> Optional[str]:
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
        "titles": title,
        "redirects": 1,
        "formatversion": 2,
    }
    r = backoff_get(s, WIKIPEDIA_API, params=params)
    data = r.json()
    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return None
    revs = pages[0].get("revisions", [])
    if not revs:
        return None
    slot = revs[0].get("slots", {}).get("main", {})
    return slot.get("content")

def parse_release_from_wikitext(wikitext: str) -> Optional[str]:
    if not wikitext:
        return None

    # 1) Find infobox block (Infobox single, song, musical work…)
    #    We scan a window after the infobox header to reduce noise.
    infobox_start = re.search(r"\{\{\s*Infobox[^}]*\n", wikitext, flags=re.IGNORECASE)
    block = wikitext
    if infobox_start:
        start = infobox_start.start()
        # take a reasonable window (first 2000 chars from infobox start)
        block = wikitext[start:start+2000]

    # 2) Try start date templates inside the block
    m = STARTDATE_TMPL_RX.search(block)
    if m:
        y = m.group("y")
        mm = m.group("m")
        dd = m.group("d")
        if y and mm and dd:
            return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"
        if y and mm:
            return f"{int(y):04d}-{int(mm):02d}"
        if y:
            return f"{int(y):04d}"

    # 3) Try lines like: | released = 20 May 1991
    #    We capture after '=' up to line end and try a few date patterns.
    line_rx = re.compile(
        r"^\s*\|\s*(?:%s)\s*=\s*(.+)$" % "|".join([re.escape(k) for k in RELEASE_KEYS]),
        flags=re.IGNORECASE | re.MULTILINE,
    )
    m2 = line_rx.search(block)
    if m2:
        raw = clean_markup(m2.group(1))
        iso = sniff_human_date_to_iso(raw)
        if iso:
            return iso

    # 4) Last resort: search anywhere for a start date template
    m3 = STARTDATE_TMPL_RX.search(wikitext)
    if m3:
        y = m3.group("y")
        mm = m3.group("m")
        dd = m3.group("d")
        if y and mm and dd:
            return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"
        if y and mm:
            return f"{int(y):04d}-{int(mm):02d}"
        if y:
            return f"{int(y):04d}"

    return None

def clean_markup(text: str) -> str:
    t = text or ""
    # Remove refs and templates
    t = re.sub(r"<ref[^>]*>.*?</ref>", " ", t, flags=re.DOTALL|re.IGNORECASE)
    t = re.sub(r"<ref[^/>]*/>", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\{\{.*?\}\}", " ", t)  # naive but effective for most cases
    # Strip brackets for links [[...|...]]
    t = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", t)
    # Collapse whitespace
    t = " ".join(t.split())
    return t.strip(" ,;")

def sniff_human_date_to_iso(text: str) -> Optional[str]:
    # Try a few formats cheaply without external libs:
    # Patterns like: 20 May 1991, May 1991, 1991
    # Also: 1991-05-20, 1991-05, 1991
    t = text.strip()
    m = DATE_RX_ISO.match(t)
    if m:
        y, mm, dd = m.groups()
        if dd:
            return f"{y}-{mm}-{dd}"
        if mm:
            return f"{y}-{mm}"
        return y

    # Day Month Year
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", t)
    if m:
        d, mon, y = m.groups()
        mm = month_to_num(mon)
        if mm:
            return f"{int(y):04d}-{mm:02d}-{int(d):02d}"

    # Month Year
    m = re.match(r"^([A-Za-z]+)\s+(\d{4})$", t)
    if m:
        mon, y = m.groups()
        mm = month_to_num(mon)
        if mm:
            return f"{int(y):04d}-{mm:02d}"

    # Just year
    m = re.match(r"^(\d{4})$", t)
    if m:
        return m.group(1)

    return None

def month_to_num(mon: str) -> Optional[int]:
    mon = mon.strip().lower()
    months = {
        "january":1,"jan":1,
        "february":2,"feb":2,
        "march":3,"mar":3,
        "april":4,"apr":4,
        "may":5,
        "june":6,"jun":6,
        "july":7,"jul":7,
        "august":8,"aug":8,
        "september":9,"sep":9,"sept":9,
        "october":10,"oct":10,
        "november":11,"nov":11,
        "december":12,"dec":12,
    }
    return months.get(mon)

def add_md_columns(iso: Optional[str]) -> Tuple[str, str]:
    if not iso:
        return ("","")
    m = DATE_RX_ISO.match(iso)
    if not m:
        # If precision is year or year-month we may not have day
        parts = iso.split("-")
        if len(parts) == 1:
            return ("","")
        if len(parts) == 2:
            return (parts[1].zfill(2), "")
        return ("","")
    y, mm, dd = m.groups()
    return (mm or "", dd or "")

def process_row(s: requests.Session, row: Dict, throttle: float) -> Dict:
    src_url = row.get("source_url","").strip()
    title = (row.get("title") or "").strip()
    if not title:
        title = derive_title_from_url(src_url) or ""

    release_date = None
    date_source = ""

    # 1) Wikidata P577
    try:
        if title:
            qid = mw_get_qid_for_title(s, title)
            if qid:
                wd_date = wd_get_p577_date(s, qid)
                if wd_date:
                    release_date = wd_date
                    date_source = "wikidata:P577"
    except Exception as e:
        # keep going
        pass

    # 2) Fallback to wikitext parsing
    if not release_date and title:
        try:
            wikitext = mw_get_wikitext(s, title)
            wt_date = parse_release_from_wikitext(wikitext)
            if wt_date:
                release_date = wt_date
                date_source = "wikitext:released"
        except Exception:
            pass

    # 3) If still none and URL looks valid, try deriving title again from URL path quirks
    if not release_date and not title and src_url:
        t2 = derive_title_from_url(src_url)
        if t2 and t2 != title:
            try:
                qid = mw_get_qid_for_title(s, t2)
                if qid:
                    wd_date = wd_get_p577_date(s, qid)
                    if wd_date:
                        release_date = wd_date
                        date_source = "wikidata:P577"
                if not release_date:
                    wikitext = mw_get_wikitext(s, t2)
                    wt_date = parse_release_from_wikitext(wikitext)
                    if wt_date:
                        release_date = wt_date
                        date_source = "wikitext:released"
            except Exception:
                pass

    mm, dd = add_md_columns(release_date)

    out = dict(row)
    out["release_date"] = release_date or ""
    out["month"] = mm
    out["day"] = dd
    out["date_source"] = date_source
    time.sleep(throttle)
    return out

def read_csv(path: str) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)

def write_csv(path: str, rows: List[Dict]) -> None:
    if not rows:
        # nothing to write; still create file with header from expected fields
        fieldnames = ["title","source_url","release_date","month","day","date_source"]
    else:
        # union of keys across rows to preserve original columns
        keys = set()
        for r in rows:
            keys.update(r.keys())
        # pin standard columns last in a stable order
        std = ["release_date","month","day","date_source"]
        fieldnames = [k for k in rows[0].keys() if k not in std]
        for k in rows:
            pass
        for s in std:
            if s not in fieldnames:
                fieldnames.append(s)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k,"") for k in fieldnames})

def main():
    ap = argparse.ArgumentParser(description="Fetch release dates for Wikipedia song pages.")
    ap.add_argument("--in", dest="in_path", default=IN_PATH_DEFAULT, help="Input CSV path (default data/songs.csv)")
    ap.add_argument("--out", dest="out_path", default=OUT_PATH_DEFAULT, help="Output CSV path (default data/songs_with_dates.csv)")
    ap.add_argument("--start", dest="start_index", type=int, default=0, help="Start row index (0-based) for resuming")
    ap.add_argument("--limit", dest="limit", type=int, default=0, help="Process at most N rows (0 = all)")
    ap.add_argument("--throttle", type=float, default=0.25, help="Seconds sleep between items")
    args = ap.parse_args()

    s = http_session()

    rows = read_csv(args.in_path)

    # Only process rows missing a release_date
    rows = [r for r in rows if not (r.get("release_date") or "").strip()]

    n = len(rows)
    if n == 0:
        print("No missing release dates found — nothing to do.")
        return
    if args.start_index < 0 or args.start_index >= n:
        start = 0
    else:
        start = args.start_index

    if args.limit and args.limit > 0:
        end = min(n, start + args.limit)
    else:
        end = n

    out_rows: List[Dict] = []
    for i, row in enumerate(rows):
        if i < start or i >= end:
            # pass-through unaffected rows
            out_rows.append(dict(row))
            continue
        try:
            out_row = process_row(s, row, throttle=args.throttle)
        except Exception as e:
            out_row = dict(row)
            out_row["release_date"] = ""
            out_row["month"] = ""
            out_row["day"] = ""
            out_row["date_source"] = f"error:{type(e).__name__}"
        out_rows.append(out_row)
        if (i - start + 1) % 25 == 0:
            print(f"Processed {i - start + 1} rows...")

    write_csv(args.out_path, out_rows)
    print(f"Wrote {args.out_path} rows={len(out_rows)} (processed {end-start} rows)")

if __name__ == "__main__":
    main()
