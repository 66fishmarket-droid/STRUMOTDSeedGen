#!/usr/bin/env python3
# scripts/build_arts_on_this_day.py
# Phase 2: real dataset for songs_top10_us via Wikipedia year pages (coverage-first).
# Albums (RIAA) and Movies (OMDb) will be added next; Wikidata enrichment later.

import csv
import os
import time
from datetime import datetime
from typing import List, Dict, Tuple

import requests
import pandas as pd

FIELDS = [
    "work_type","title","byline","release_date",
    "month","day","extra","source_url"
]

OUT_SONGS = "data/songs_top10_us.csv"
OUT_ALBUMS = "data/albums_us_1m.csv"   # placeholder for now
OUT_MOVIES = "data/movies_rt80.csv"    # placeholder for now

WIKI_PAGE_TPL = "https://en.wikipedia.org/wiki/List_of_Billboard_Hot_100_top-ten_singles_in_{year}"

def ua_contact() -> str:
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")

def http_get(url: str, max_retries: int = 5, backoff: float = 1.5) -> requests.Response:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": f"StrumOTD/1.0 (+{ua_contact()})",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    for attempt in range(1, max_retries + 1):
        r = sess.get(url, timeout=30)
        if r.status_code == 200:
            return r
        if r.status_code in (403, 429, 500, 502, 503, 504):
            if attempt == max_retries:
                r.raise_for_status()
            time.sleep(backoff)
            backoff *= 2
            continue
        r.raise_for_status()
    return r  # never reached

def parse_first_date(text: str, year_hint: int) -> str:
    """
    Try to parse a date like 'January 12', 'Jan 12', '2011-06-21', etc.
    Return YYYY-MM-DD or ''.
    """
    if not text:
        return ""
    text = str(text).strip()
    # ISO already
    try:
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            dt = datetime.strptime(text[:10], "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    # Add the year hint and try common formats
    candidates = [
        f"{text} {year_hint}",
        f"{text} {year_hint-1}",  # some entries spill over year change
        f"{text} {year_hint+1}",
    ]
    fmts = ["%B %d %Y", "%b %d %Y", "%d %B %Y", "%Y %B %d", "%B %Y %d"]
    for c in candidates:
        for fmt in fmts:
            try:
                dt = datetime.strptime(c, fmt)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                continue
    return ""

def month_day(date_str: str) -> Tuple[str, str]:
    if len(date_str) >= 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str[5:7], date_str[8:10]
    return "", ""

def harvest_songs() -> List[Dict]:
    rows: List[Dict] = []
    # Billboard Hot 100 started in 1958
    current_year = datetime.utcnow().year
    for year in range(1958, current_year + 1):
        url = WIKI_PAGE_TPL.format(year=year)
        try:
            resp = http_get(url)
        except Exception as e:
            print(f"Warning: failed to fetch {url}: {e}")
            continue

        # Read all tables on the page, then pick those with likely columns
        try:
            tables = pd.read_html(resp.text)
        except Exception as e:
            print(f"Warning: no tables parsed for {url}: {e}")
            continue

        # Different years name columns slightly differently. We map heuristically.
        picked_any = False
        for df in tables:
            cols = [str(c).strip().lower() for c in df.columns]
            # We need columns for song title and artist(s); date may be "entry date", "peak date", or similar
            if any("single" in c or "song" in c or "title" in c for c in cols) and any("artist" in c for c in cols):
                # Try to find title col
                title_col = next((c for c in df.columns if "ingle" in str(c).lower() or "title" in str(c).lower() or "song" in str(c).lower()), None)
                artist_col = next((c for c in df.columns if "artist" in str(c).lower()), None)
                # Prefer an entry/peak date-like column
                date_col = next(
                    (c for c in df.columns
                     if any(k in str(c).lower() for k in ["entry", "debut", "first", "peak", "top ten", "date"])),
                    None
                )
                if not title_col or not artist_col:
                    continue

                for _, r in df.iterrows():
                    title = str(r.get(title_col, "")).strip().strip('"')
                    byline = str(r.get(artist_col, "")).strip()
                    if not title or title.lower() in ("single", "title", "song"):
                        continue

                    raw_date = str(r.get(date_col, "")).strip() if date_col else ""
                    release_date = parse_first_date(raw_date, year)
                    # Fallback: if no date parsed, use year end as YYYY-12-31 (we will enrich later)
                    if not release_date:
                        release_date = f"{year}-12-31"

                    mm, dd = month_day(release_date)
                    rows.append({
                        "work_type": "song",
                        "title": title,
                        "byline": byline,
                        "release_date": release_date,
                        "month": mm,
                        "day": dd,
                        "extra": "US Top 10",
                        "source_url": url
                    })
                picked_any = True

        if not picked_any:
            print(f"Note: no matching tables used for {url}")

        # Be polite between pages
        time.sleep(0.6)

    # Deduplicate by (title.lower(), byline.lower()) keeping earliest date
    dedup: Dict[Tuple[str, str], Dict] = {}
    for r in rows:
        key = (r["title"].lower(), r["byline"].lower())
        if key not in dedup or (dedup[key]["release_date"] and r["release_date"] < dedup[key]["release_date"]):
            dedup[key] = r

    rows = list(dedup.values())
    # Sort stable: by release_date then title
    rows.sort(key=lambda x: (x["release_date"], x["title"].lower()))
    return rows

def write_csv(path: str, rows: List[Dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})

def main():
    # Build songs (coverage-first)
    songs = harvest_songs()
    write_csv(OUT_SONGS, songs)

    # Keep placeholders for the other outputs (to be filled next)
    write_csv(OUT_ALBUMS, [])
    write_csv(OUT_MOVIES, [])

    print(f"Wrote {OUT_SONGS} [{len(songs)} rows]")
    print(f"Initialized placeholders: {OUT_ALBUMS}, {OUT_MOVIES}")

if __name__ == "__main__":
    main()
