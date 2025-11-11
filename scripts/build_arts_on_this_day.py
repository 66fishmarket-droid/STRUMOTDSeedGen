#!/usr/bin/env python3
# scripts/build_arts_on_this_day.py
# Songs: Billboard Hot 100 Top-10 incremental harvest with clean parsing.
# Albums (RIAA) and Movies (OMDb) remain placeholders for now.

import csv
import os
import re
import time
import json
from typing import List, Dict, Tuple, Optional
from datetime import datetime

import requests
import pandas as pd

# Standard calendar fields (used by calendar_index.csv)
FIELDS = [
    "work_type","title","byline","release_date",
    "month","day","extra","source_url"
]

# Songs file carries extended chart context
SONG_FIELDS = FIELDS + ["entry_date","peak_date","peak_position"]

OUT_SONGS  = "data/songs_top10_us.csv"
OUT_ALBUMS = "data/albums_us_1m.csv"   # placeholder
OUT_MOVIES = "data/movies_rt80.csv"    # placeholder
STATE_SONGS = "data/state_songs.json"

WIKI_PAGE_TPL = "https://en.wikipedia.org/wiki/List_of_Billboard_Hot_100_top-ten_singles_in_{year}"

# -------- HTTP helpers --------

def ua_contact() -> str:
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")

def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": f"StrumOTD/1.0 (+{ua_contact()})",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s

def http_get_conditional(s: requests.Session, url: str, ims: Optional[str], max_retries: int = 5, backoff: float = 1.5) -> Optional[requests.Response]:
    # Returns Response(200) if fresh content, None if 304 Not Modified.
    headers = {}
    if ims:
        headers["If-Modified-Since"] = ims
    for attempt in range(1, max_retries + 1):
        r = s.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            return r
        if r.status_code == 304:
            return None
        if r.status_code in (403, 429, 500, 502, 503, 504):
            if attempt == max_retries:
                r.raise_for_status()
            time.sleep(backoff); backoff *= 2
            continue
        r.raise_for_status()
    return None

# -------- date helpers --------

def parse_first_date(text: str, year_hint: int) -> str:
    if not text:
        return ""
    t = str(text).strip()
    # ISO already?
    try:
        if len(t) >= 10 and t[4] == "-" and t[7] == "-":
            dt = datetime.strptime(t[:10], "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    # Try common "Month Day" formats with nearby years
    candidates = [f"{t} {year_hint}", f"{t} {year_hint-1}", f"{t} {year_hint+1}"]
    fmts = ["%B %d %Y", "%b %d %Y", "%d %B %Y", "%Y %B %d", "%B %Y %d"]
    for c in candidates:
        for fmt in fmts:
            try:
                dt = datetime.strptime(c, fmt)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                continue
    return ""

def month_day(date_str: str) -> Tuple[str,str]:
    if len(date_str) >= 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str[5:7], date_str[8:10]
    return "",""

# -------- state + IO --------

def load_state() -> Dict[str,str]:
    if os.path.exists(STATE_SONGS):
        try:
            with open(STATE_SONGS, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_state(state: Dict[str,str]) -> None:
    os.makedirs(os.path.dirname(STATE_SONGS), exist_ok=True)
    with open(STATE_SONGS, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)

def read_existing_songs() -> List[Dict]:
    if not os.path.exists(OUT_SONGS):
        return []
    rows = []
    with open(OUT_SONGS, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # normalize keys into SONG_FIELDS superset
            norm = {k: row.get(k, "") for k in SONG_FIELDS}
            rows.append(norm)
    return rows

def write_songs(rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(OUT_SONGS), exist_ok=True)
    with open(OUT_SONGS, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SONG_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in SONG_FIELDS})

def write_empty_standard(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()

# -------- parsing helpers (robust column mapping + cleaning) --------

MONTHS_RX = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
BRACKET_RX = re.compile(r"\[[^\]]*\]")     # [A], [27], etc.
SUPMARK_RX = re.compile(r"[↑↓*†‡]")        # markers
QUOTES_RX  = re.compile(r'^[\'"]+|[\'"]+$')# leading/trailing quotes

def norm_cell(x: str) -> str:
    s = str(x or "").strip()
    s = BRACKET_RX.sub("", s)
    s = SUPMARK_RX.sub("", s)
    s = QUOTES_RX.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_dateish(s: str) -> bool:
    if not s:
        return False
    t = s.lower().strip()
    if re.search(MONTHS_RX, t):
        return True
    return bool(re.match(r"\d{4}-\d{2}-\d{2}$", t)) or bool(re.match(r"^[a-z]{3}\s+\d{1,2}$", t))

def map_columns(cols_original) -> Dict[str, Optional[str]]:
    cols = [str(c) for c in cols_original]
    low  = [c.lower().strip() for c in cols]

    def pick(cands: List[str]) -> Optional[str]:
        for cand in cands:
            for i,name in enumerate(low):
                if cand in name:
                    return cols[i]
        return None

    title_col  = pick(["single", "song", "title", "track"])
    artist_col = pick(["artist", "artists", "performer"])
    entry_col  = pick([
        "entry date", "debut", "first week in top ten", "first week", "top ten entry",
        "top-ten entry", "top ten debut", "top-ten debut", "first top ten", "first top-ten"
    ])
    week_col   = pick(["week of", "date"])  # fallback
    peak_col   = pick(["peak", "peak pos", "peak position", "highest pos"])
    peak_date_col = pick(["peak date", "date peaked"])

    return {
        "title": title_col,
        "artist": artist_col,
        "entry": entry_col,
        "week": week_col,
        "peak": peak_col,
        "peak_date": peak_date_col
    }

def parse_year_page(resp_text: str, year: int, url: str) -> List[Dict]:
    out: List[Dict] = []
    try:
        tables = pd.read_html(resp_text)
    except Exception:
        return out

    got_rows = False
    for df in tables:
        cols = map_columns(df.columns)
        if not cols["title"] or not cols["artist"]:
            continue

        for _, row in df.iterrows():
            raw_title  = norm_cell(row.get(cols["title"], ""))
            raw_artist = norm_cell(row.get(cols["artist"], ""))
            raw_entry  = norm_cell(row.get(cols["entry"], "")) if cols["entry"] else ""
            raw_week   = norm_cell(row.get(cols["week"], ""))  if cols["week"]  else ""
            raw_peak   = norm_cell(row.get(cols["peak"], ""))  if cols["peak"]  else ""
            raw_peak_d = norm_cell(row.get(cols["peak_date"], "")) if cols["peak_date"] else ""

            # skip header-like and junk rows
            if not raw_title or raw_title.lower() in ("single", "song", "title"):
                continue
            if not raw_artist or raw_artist.lower() in ("artist", "artists"):
                continue
            if is_dateish(raw_title) or re.fullmatch(r"\d+", raw_title):
                continue

            entry_date = parse_first_date(raw_entry, year) or parse_first_date(raw_week, year) or f"{year}-12-31"
            peak_date  = parse_first_date(raw_peak_d, year) if raw_peak_d else ""

            mm, dd = month_day(entry_date)

            out.append({
                # standard columns
                "work_type": "song",
                "title": raw_title,
                "byline": raw_artist,
                "release_date": entry_date,   # true release will be enriched later
                "month": mm,
                "day": dd,
                "extra": "US Top 10",
                "source_url": url,
                # extended fields
                "entry_date": entry_date,
                "peak_date": peak_date,
                "peak_position": raw_peak
            })
            got_rows = True

    if not got_rows:
        print(f"Note: no matching tables used for {url}")

    return out

# -------- combine/dedupe/incremental --------

def dedupe_keep_earliest(rows: List[Dict]) -> List[Dict]:
    # dedupe by (title, byline) keeping earliest entry_date
    dedup: Dict[Tuple[str,str], Dict] = {}
    for r in rows:
        key = (r["title"].lower(), r["byline"].lower())
        if key not in dedup:
            dedup[key] = r
        else:
            old = dedup[key]
            a = old.get("entry_date","")
            b = r.get("entry_date","")
            if a and b and b < a:
                dedup[key] = r
    out = list(dedup.values())
    out.sort(key=lambda x: (x.get("entry_date",""), x["title"].lower()))
    return out

def years_to_fetch(full_build: bool) -> List[int]:
    cy = datetime.utcnow().year
    if full_build:
        return list(range(1958, cy + 1))
    return [y for y in [cy, cy-1, cy-2] if y >= 1958]

def year_from_url(u: str) -> Optional[int]:
    # .../in_YYYY
    try:
        return int(u.rsplit("_", 1)[-1])
    except Exception:
        return None

def harvest_songs_incremental(full_build: bool) -> List[Dict]:
    s = session()
    state = load_state()  # { "YYYY": "Last-Modified" }
    existing = read_existing_songs()

    target_years = years_to_fetch(full_build)
    fresh_rows: List[Dict] = []

    for year in target_years:
        url = WIKI_PAGE_TPL.format(year=year)
        ims = state.get(str(year))
        try:
            resp = http_get_conditional(s, url, ims)
        except Exception as e:
            print(f"Warning: fetch error for {url}: {e}")
            continue

        if resp is None:
            # 304: reuse existing rows from this year
            reused = [r for r in existing if year_from_url(r.get("source_url","")) == year]
            fresh_rows.extend(reused)
            print(f"{year}: not modified, reused {len(reused)} rows")
            continue

        parsed = parse_year_page(resp.text, year, url)
        fresh_rows.extend(parsed)
        lm = resp.headers.get("Last-Modified")
        if lm:
            state[str(year)] = lm
        print(f"{year}: parsed {len(parsed)} rows")
        time.sleep(0.4)

    # Keep rows from years we didn't touch
    keep_years = set(target_years)
    untouched = [r for r in existing if (year_from_url(r.get("source_url","")) or 0) not in keep_years]

    combined = dedupe_keep_earliest(fresh_rows + untouched)
    save_state(state)
    return combined

# -------- main --------

def main():
    full_build = os.getenv("FULL_BUILD", "false").lower() == "true"

    # Songs dataset
    songs = harvest_songs_incremental(full_build=full_build)
    write_songs(songs)

    # Placeholders to keep workflow green
    write_empty_standard(OUT_ALBUMS)
    write_empty_standard(OUT_MOVIES)

    print(f"Wrote {OUT_SONGS} [{len(songs)} rows] (full_build={full_build})")
    print(f"Initialized placeholders: {OUT_ALBUMS}, {OUT_MOVIES}")

if __name__ == "__main__":
    main()
