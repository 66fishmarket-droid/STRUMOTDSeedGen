#!/usr/bin/env python3
# scripts/fetch_song_release_dates.py
# For each Wikipedia song page, extract a release date.
#
# Strategy:
#   1) Derive the exact Wikipedia page title from source_url when possible.
#   2) Get QID from pageprops; query Wikidata P577.
#   3) Fallback/augment: fetch page wikitext and parse infobox "released" field.
#   4) Normalize to YYYY-MM-DD when possible; else YYYY-MM or YYYY.
#
# Input CSV: Top 10 songs with columns
#   work_type,title,byline,release_date,month,day,extra,source_url,
#   entry_date,peak_date,peak_position,date_source
#
# Output CSV: same file, with release_date/month/day/date_source filled for:
#   - rows where release_date is blank, or
#   - rows where release_date is just a 4-digit year.
#
# Delta-safe:
# - If data/songs_top10_us_with_dates.csv is missing, seed from data/songs_top10_us.csv.
# - Processes only target rows, then writes the full file back.

import os
import re
import csv
import time
import argparse
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote

import requests

IN_PATH_DEFAULT = "data/songs_top10_us_with_dates.csv"
OUT_PATH_DEFAULT = "data/songs_top10_us_with_dates.csv"
SEED_FROM = "data/songs_top10_us.csv"  # used only if the with_dates file is missing

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

YEAR_ONLY_RX = re.compile(r"^\s*\d{4}\s*$")

# ---------- HTTP/session ----------

def ua_contact() -> str:
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")

def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": f"StrumSongDates/1.0 (+{ua_contact()})",
        "Accept": "application/json",
    })
    return s

def backoff_get(
    s: requests.Session,
    url: str,
    params: Dict = None,
    max_retries: int = 6,
    base_sleep: float = 0.8,
):
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

# ---------- Wikipedia utilities ----------

def derive_title_from_url(url: str) -> Optional[str]:
    """
    Given a Wikipedia URL like:
      https://en.wikipedia.org/wiki/Pink_Pony_Club
    return the article title portion: 'Pink_Pony_Club'.
    """
    try:
        p = urlparse(url)
        parts = p.path.split("/")
        if len(parts) >= 3 and parts[1] == "wiki":
            seg = unquote(parts[2])
        else:
            seg = unquote(parts[-1]) if parts else ""
        if not seg or seg.lower().startswith("list_of_"):
            return None
        return seg
    except Exception:
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
    ent = data.get("entities", {}).get(qid)
    if not ent:
        return None
    p577 = ent.get("claims", {}).get("P577")
    if not p577:
        return None
    best_iso = None
    for st in p577:
        v = st.get("mainsnak", {}).get("datavalue", {})
        if v.get("type") != "time":
            continue
        t = v.get("value", {}).get("time")  # like "+1991-05-20T00:00:00Z"
        precision = v.get("value", {}).get("precision")  # 9=year, 10=month, 11=day
        if not t:
            continue
        iso = normalize_wikidata_time(t, precision)
        if iso and (best_iso is None or iso < best_iso):
            best_iso = iso
    return best_iso

def normalize_wikidata_time(time_str: str, precision: int) -> Optional[str]:
    t = time_str.lstrip("+")
    if "T" in t:
        t = t.split("T", 1)[0]
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

# ---------- Wikitext parsing ----------

def parse_release_from_wikitext(wikitext: str) -> Optional[str]:
    """
    Look inside the infobox first (preferred), then the wider article,
    for either a {{start date|...}} template or a 'released =' line.
    """
    if not wikitext:
        return None

    # Try to limit to the Infobox block first
    infobox_start = re.search(r"\{\{\s*Infobox[^}]*\n", wikitext, flags=re.IGNORECASE)
    block = wikitext
    if infobox_start:
        start = infobox_start.start()
        block = wikitext[start:start + 2000]

    # 1) Look for a start date template in the infobox
    m = STARTDATE_TMPL_RX.search(block)
    if m:
        y = m.group("y"); mm = m.group("m"); dd = m.group("d")
        if y and mm and dd:
            return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"
        if y and mm:
            return f"{int(y):04d}-{int(mm):02d}"
        if y:
            return f"{int(y):04d}"

    # 2) Look for a "| released = ..." line (or similar keys)
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

    # 3) As a last resort, look for a start date template anywhere
    m3 = STARTDATE_TMPL_RX.search(wikitext)
    if m3:
        y = m3.group("y"); mm = m3.group("m"); dd = m3.group("d")
        if y and mm and dd:
            return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"
        if y and mm:
            return f"{int(y):04d}-{int(mm):02d}"
        if y:
            return f"{int(y):04d}"

    return None

def clean_markup(text: str) -> str:
    t = text or ""
    # Strip <ref>...</ref> and self-closing refs
    t = re.sub(r"<ref[^>]*>.*?</ref>", " ", t, flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r"<ref[^/>]*/>", " ", t, flags=re.IGNORECASE)
    # Remove simple templates
    t = re.sub(r"\{\{.*?\}\}", " ", t)
    # Replace [[link|text]] / [[text]] with 'text'
    t = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", t)
    # Collapse whitespace and trim punctuation
    t = " ".join(t.split())
    return t.strip(" ,;")

def sniff_human_date_to_iso(text: str) -> Optional[str]:
    t = text.strip()

    # Already ISO-ish
    m = DATE_RX_ISO.match(t)
    if m:
        y, mm, dd = m.groups()
        if dd:
            return f"{y}-{mm}-{dd}"
        if mm:
            return f"{y}-{mm}"
        return y

    # Formats like "21 October 1985"
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", t)
    if m:
        d, mon, y = m.groups()
        mm = month_to_num(mon)
        if mm:
            return f"{int(y):04d}-{mm:02d}-{int(d):02d}"

    # Formats like "October 1985"
    m = re.match(r"^([A-Za-z]+)\s+(\d{4})$", t)
    if m:
        mon, y = m.groups()
        mm = month_to_num(mon)
        if mm:
            return f"{int(y):04d}-{mm:02d}"

    # Just a year "1985"
    m = re.match(r"^(\d{4})$", t)
    if m:
        return m.group(1)

    return None

def month_to_num(mon: str) -> Optional[int]:
    mon = mon.strip().lower()
    months = {
        "january": 1, "jan": 1,
        "february": 2, "feb": 2,
        "march": 3, "mar": 3,
        "april": 4, "apr": 4,
        "may": 5,
        "june": 6, "jun": 6,
        "july": 7, "jul": 7,
        "august": 8, "aug": 8,
        "september": 9, "sep": 9, "sept": 9,
        "october": 10, "oct": 10,
        "november": 11, "nov": 11,
        "december": 12, "dec": 12,
    }
    return months.get(mon)

def add_md_columns(iso: Optional[str]) -> Tuple[str, str]:
    if not iso:
        return ("", "")
    m = DATE_RX_ISO.match(iso)
    if not m:
        parts = iso.split("-")
        if len(parts) == 2:
            return (parts[1].zfill(2), "")
        return ("", "")
    _, mm, dd = m.groups()
    return (mm or "", dd or "")

def iso_precision_level(iso: str) -> int:
    """
    Rough precision: 3 = YYYY-MM-DD, 2 = YYYY-MM, 1 = YYYY, 0 = unknown.
    """
    m = DATE_RX_ISO.match(iso or "")
    if not m:
        return 0
    y, mm, dd = m.groups()
    if dd:
        return 3
    if mm:
        return 2
    if y:
        return 1
    return 0

def is_more_precise(new_iso: str, old_iso: str) -> bool:
    return iso_precision_level(new_iso) > iso_precision_level(old_iso)

# ---------- Row processor ----------

def process_row(s: requests.Session, row: Dict, throttle: float) -> Dict:
    src_url = row.get("source_url", "").strip()
    csv_title = (row.get("title") or "").strip()
    artist = (row.get("byline") or "").strip()  # unused for now, but kept for possible future logic

    # Prefer the exact article title derived from the URL
    page_title = derive_title_from_url(src_url)
    if page_title:
        title = page_title
    else:
        title = csv_title

    release_date = None
    date_source = ""

    # Try Wikidata P577 via QID for the specific page title
    qid = None
    try:
        if title:
            qid = mw_get_qid_for_title(s, title)
    except Exception:
        qid = None

    try:
        if qid:
            wd_date = wd_get_p577_date(s, qid)
            if wd_date:
                release_date = wd_date
                date_source = "wikidata:P577"
    except Exception:
        pass

    # Wikitext fallback (or refinement: get more precise than just a year/month)
    need_wikitext = (not release_date) or iso_precision_level(release_date) < 3
    if title and need_wikitext:
        try:
            wikitext = mw_get_wikitext(s, title)
            wt_date = parse_release_from_wikitext(wikitext)
            if wt_date:
                if (not release_date) or is_more_precise(wt_date, release_date):
                    release_date = wt_date
                    date_source = "wikitext:released"
        except Exception:
            pass

    mm, dd = add_md_columns(release_date)

    out = dict(row)
    out["release_date"] = release_date or row.get("release_date", "") or ""
    out["month"] = mm or row.get("month", "") or ""
    out["day"] = dd or row.get("day", "") or ""
    out["date_source"] = date_source or row.get("date_source", "") or ""
    time.sleep(throttle)
    return out

# ---------- CSV IO ----------

def read_csv(path: str) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)

def write_csv(path: str, rows: List[Dict]) -> None:
    fieldnames = [
        "work_type", "title", "byline", "release_date", "month", "day",
        "extra", "source_url", "entry_date", "peak_date",
        "peak_position", "date_source",
    ]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            out = {k: row.get(k, "") for k in fieldnames}
            w.writerow(out)

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(
        description="Fetch release dates for Wikipedia song pages (delta mode)."
    )
    ap.add_argument("--in", dest="in_path", default=IN_PATH_DEFAULT, help="Input CSV path")
    ap.add_argument("--out", dest="out_path", default=OUT_PATH_DEFAULT, help="Output CSV path")
    ap.add_argument("--throttle", type=float, default=0.3, help="Seconds sleep between items")
    args = ap.parse_args()

    # Auto-seed: if the with_dates file does not exist, copy from the raw Top 10 file
    if not os.path.exists(args.in_path):
        if os.path.exists(SEED_FROM):
            os.makedirs(os.path.dirname(args.in_path), exist_ok=True)
            with open(SEED_FROM, "r", encoding="utf-8") as src, open(args.in_path, "w", encoding="utf-8") as dst:
                dst.write(src.read())
            print(f"Seeded {args.in_path} from {SEED_FROM}")
        else:
            print(f"Input file not found: {args.in_path}")
            return

    s = http_session()

    # Read full file; if it has no data rows, reseed from the raw file
    all_rows = read_csv(args.in_path)

    if not all_rows and os.path.exists(SEED_FROM):
        print(f"{args.in_path} has no data rows, reseeding from {SEED_FROM}")
        with open(SEED_FROM, "r", encoding="utf-8") as src, open(args.in_path, "w", encoding="utf-8") as dst:
            dst.write(src.read())
        all_rows = read_csv(args.in_path)

    # Ensure expected columns exist
    required = [
        "work_type", "title", "byline", "release_date", "month", "day",
        "extra", "source_url", "entry_date", "peak_date",
        "peak_position", "date_source",
    ]
    if all_rows:
        for r in all_rows:
            for k in required:
                r.setdefault(k, "")

    # Target rows:
    #   - missing release_date entirely; or
    #   - release_date is just a plain 4-digit year.
    target_idxs: List[int] = []
    for i, r in enumerate(all_rows):
        rd = (r.get("release_date") or "").strip()
        if not rd or YEAR_ONLY_RX.match(rd):
            target_idxs.append(i)

    if not target_idxs:
        print("No missing or year-only release dates found — nothing to do.")
        return

    print(
        f"Processing {len(target_idxs)} rows needing release_date out of {len(all_rows)} total..."
    )

    for count, i in enumerate(target_idxs, start=1):
        try:
            updated = process_row(s, all_rows[i], throttle=args.throttle)
            all_rows[i] = updated
        except Exception as e:
            all_rows[i]["date_source"] = f"error:{type(e).__name__}"
        if count % 25 == 0:
            print(f"Processed {count}/{len(target_idxs)} rows...")

    write_csv(args.out_path, all_rows)
    print(f"Wrote {args.out_path} rows={len(all_rows)} (updated {len(target_idxs)})")


if __name__ == "__main__":
    main()
