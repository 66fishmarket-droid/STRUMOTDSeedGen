#!/usr/bin/env python3
# scripts/fetch_song_release_dates.py
# Improved: adds Wikipedia search, encoding, and title disambiguation.
# Much higher accuracy for release date detection.

import os
import re
import csv
import time
import argparse
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote, quote

import requests

IN_PATH_DEFAULT = "data/songs_top10_us_with_dates.csv"
OUT_PATH_DEFAULT = "data/songs_top10_us_with_dates.csv"
SEED_FROM = "data/songs_top10_us.csv"

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"

DATE_RX_ISO = re.compile(r"^\s*(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?\s*$")

STARTDATE_TMPL_RX = re.compile(
    r"\{\{\s*start[- _]?date(?:[^|}]*)\|(?P<y>\d{3,4})(?:\|(?P<m>\d{1,2}))?(?:\|(?P<d>\d{1,2}))?",
    flags=re.IGNORECASE,
)

RELEASE_KEYS = (
    "released", "release_date", "released_date", "date", "release"
)

YEAR_ONLY_RX = re.compile(r"^\s*\d{4}\s*$")

# ---------------- HTTP helpers --------------------

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

# ---------------- Wikipedia utilities --------------------

def derive_title_from_url(url: str) -> Optional[str]:
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

def encode_title(title: str) -> str:
    # Replace spaces with underscores and encode special chars
    return quote(title.replace(" ", "_"), safe="/")

def mw_get_qid_for_title(s: requests.Session, raw_title: str) -> Optional[str]:
    title = encode_title(raw_title)
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
    if "missing" in page:
        return None
    props = page.get("pageprops", {})
    return props.get("wikibase_item")

def mw_page_exists(s: requests.Session, raw_title: str) -> bool:
    title = encode_title(raw_title)
    params = {
        "action": "query",
        "format": "json",
        "titles": title,
        "redirects": 1,
        "formatversion": 2,
    }
    r = backoff_get(s, WIKIPEDIA_API, params=params)
    pages = r.json().get("query", {}).get("pages", [])
    if not pages:
        return False
    return "missing" not in pages[0]

def mw_search_best_title(s: requests.Session, song: str, artist: str) -> Optional[str]:
    query = f"{song} {artist} song"
    params = {
        "action": "query",
        "list": "search",
        "format": "json",
        "srsearch": query,
        "srlimit": 1,
    }
    r = backoff_get(s, WIKIPEDIA_API, params=params)
    hits = r.json().get("query", {}).get("search", [])
    if not hits:
        return None
    return hits[0]["title"]

def wd_get_p577_date(s: requests.Session, qid: str) -> Optional[str]:
    url = WIKIDATA_ENTITY.format(qid=qid)
    r = backoff_get(s, url)
    data = r.json()
    ent = data.get("entities", {}).get(qid)
    if not ent:
        return None
    claims = ent.get("claims", {})
    p577 = claims.get("P577")
    if not p577:
        return None

    best = None
    for st in p577:
        dv = st.get("mainsnak", {}).get("datavalue", {})
        if dv.get("type") != "time":
            continue
        v = dv.get("value", {})
        t = v.get("time")
        precision = v.get("precision")
        if not t:
            continue
        iso = normalize_wikidata_time(t, precision)
        if iso and (best is None or iso < best):
            best = iso
    return best

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

def mw_get_wikitext(s: requests.Session, raw_title: str) -> Optional[str]:
    title = encode_title(raw_title)
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

# --------- Wikitext parsing --------------

def parse_release_from_wikitext(wikitext: str) -> Optional[str]:
    if not wikitext:
        return None

    infobox_start = re.search(r"\{\{\s*Infobox[^}]*\n", wikitext, flags=re.IGNORECASE)
    block = wikitext
    if infobox_start:
        block = wikitext[infobox_start.start():infobox_start.start() + 2000]

    m = STARTDATE_TMPL_RX.search(block)
    if m:
        y = m.group("y"); mm = m.group("m"); dd = m.group("d")
        if y and mm and dd:
            return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"
        if y and mm:
            return f"{int(y):04d}-{int(mm):02d}"
        if y:
            return f"{int(y):04d}"

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
    t = re.sub(r"<ref[^>]*>.*?</ref>", " ", t, flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r"<ref[^/>]*/>", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\{\{.*?\}\}", " ", t)
    t = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", t)
    t = " ".join(t.split())
    return t.strip(" ,;")

def sniff_human_date_to_iso(text: str) -> Optional[str]:
    t = text.strip()
    m = DATE_RX_ISO.match(t)
    if m:
        y, mm, dd = m.groups()
        if dd:
            return f"{y}-{mm}-{dd}"
        if mm:
            return f"{y}-{mm}"
        return y

    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$", t)
    if m:
        d, mon, y = m.groups()
        mm = month_to_num(mon)
        if mm:
            return f"{int(y):04d}-{mm:02d}-{int(d):02d}"

    m = re.match(r"^([A-Za-z]+)\s+(\d{4})$", t)
    if m:
        mon, y = m.groups()
        mm = month_to_num(mon)
        if mm:
            return f"{int(y):04d}-{mm:02d}"

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

# ---------------- Row logic --------------------

def try_title_variants(s: requests.Session, base_title: str, artist: str) -> Optional[str]:
    variants = [
        base_title,
        base_title.replace(" ", "_"),
        f"{base_title}_(song)",
        f"{base_title.replace(' ', '_')}_(song)",
        f"{base_title}_(single)",
        f"{base_title}_(track)",
        f"{base_title}_(artist_song)",
        f"{base_title.replace(' ', '_')}_(artist_song)",
    ]

    for t in variants:
        if mw_page_exists(s, t):
            return t

    search_hit = mw_search_best_title(s, base_title, artist)
    if search_hit and mw_page_exists(s, search_hit):
        return search_hit

    return None

def process_row(s: requests.Session, row: Dict, throttle: float) -> Dict:
    src_url = row.get("source_url", "").strip()
    csv_title = (row.get("title") or "").strip()
    artist = (row.get("byline") or "").strip()

    derived = derive_title_from_url(src_url)
    base_title = derived if derived else csv_title

    title = try_title_variants(s, base_title, artist)
    if not title:
        out = dict(row)
        out["date_source"] = "error:no_title_found"
        return out

    release_date = None
    date_source = ""

    qid = None
    try:
        qid = mw_get_qid_for_title(s, title)
    except Exception:
        pass

    try:
        if qid:
            wd_date = wd_get_p577_date(s, qid)
            if wd_date:
                release_date = wd_date
                date_source = "wikidata:P577"
    except Exception:
        pass

    need_wikitext = (not release_date) or iso_precision_level(release_date) < 3
    if need_wikitext:
        try:
            wikitext = mw_get_wikitext(s, title)
            wt_date = parse_release_from_wikitext(wikitext)
            if wt_date and is_more_precise(wt_date, release_date or ""):
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

# ---------------- CSV IO --------------------

def read_csv(path: str) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

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
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

# ---------------- Main --------------------

def main():
    ap = argparse.ArgumentParser(description="Fetch release dates for Wikipedia song pages (improved).")
    ap.add_argument("--in", dest="in_path", default=IN_PATH_DEFAULT, help="Input CSV path")
    ap.add_argument("--out", dest="out_path", default=OUT_PATH_DEFAULT, help="Output CSV path")
    ap.add_argument("--throttle", type=float, default=0.3, help="Seconds sleep per row")
    args = ap.parse_args()

    if not os.path.exists(args.in_path):
        if os.path.exists(SEED_FROM):
            os.makedirs(os.path.dirname(args.in_path), exist_ok=True)
            with open(SEED_FROM, "r", encoding="utf-8") as src, open(args.in_path, "w", encoding="utf-8") as dst:
                dst.write(src.read())
            print(f"Seeded {args.in_path} from {SEED_FROM}")
        else:
            print(f"Missing input file: {args.in_path}")
            return

    s = http_session()
    all_rows = read_csv(args.in_path)

    if not all_rows and os.path.exists(SEED_FROM):
        print(f"{args.in_path} empty, reseeding from {SEED_FROM}")
        with open(SEED_FROM, "r", encoding="utf-8") as src, open(args.in_path, "w", encoding="utf-8") as dst:
            dst.write(src.read())
        all_rows = read_csv(args.in_path)

    required = [
        "work_type", "title", "byline", "release_date", "month", "day",
        "extra", "source_url", "entry_date", "peak_date",
        "peak_position", "date_source",
    ]
    for r in all_rows:
        for k in required:
            r.setdefault(k, "")

    target = []
    for i, r in enumerate(all_rows):
        rd = (r.get("release_date") or "").strip()
        if not rd or YEAR_ONLY_RX.match(rd):
            target.append(i)

    if not target:
        print("Nothing to do.")
        return

    print(f"Total rows in file: {len(all_rows)}")

    already_had_dates = len(all_rows) - len(target)
    print(f"Rows already containing release dates: {already_had_dates}")
    print(f"Rows needing update (missing or year-only): {len(target)}")

    updated_count = 0
    error_count = 0

    print(f"Processing {len(target)} rows...")

    for count, i in enumerate(target, start=1):
        try:
            before = all_rows[i].get("release_date", "").strip()
            updated_row = process_row(s, all_rows[i], throttle=args.throttle)
            after = updated_row.get("release_date", "").strip()

            all_rows[i] = updated_row

            if after and after != before:
                updated_count += 1

        except Exception as e:
            error_count += 1
            all_rows[i]["date_source"] = f"error:{type(e).__name__}"

        if count % 25 == 0:
            print(f"Processed {count}/{len(target)} rows...")

    print("")
    print("==== Release Date Update Summary ====")
    print(f"Total rows: {len(all_rows)}")
    print(f"Already had valid release_date: {already_had_dates}")
    print(f"Rows requiring update: {len(target)}")
    print(f"Successfully updated release_date: {updated_count}")
    print(f"Failed/error rows: {error_count}")

    remaining_missing = sum(
        1 for r in all_rows if not r.get("release_date", "").strip()
    )
    print(f"Remaining missing after run: {remaining_missing}")
    print("====================================")
    print("")

    write_csv(args.out_path, all_rows)
    print(f"Wrote {args.out_path} rows={len(all_rows)} updated={len(target)}")


if __name__ == "__main__":
    main()

