#!/usr/bin/env python3
# scripts/build_arts_on_this_day.py
# Songs: Billboard Hot 100 Top-10 incremental harvest (robust lxml parser).
# Albums (RIAA) and Movies (OMDb) remain placeholders for now.

import csv
import os
import re
import time
import json
from typing import List, Dict, Tuple, Optional
from datetime import datetime

import requests
from lxml import html

# Standard calendar fields (used by calendar_index.csv)
FIELDS = [
    "work_type","title","byline","release_date",
    "month","day","extra","source_url"
]

# Songs file carries extended chart context
# NOTE: We do NOT infer release_date from chart dates. Leave release_date/month/day blank.
SONG_FIELDS = FIELDS + ["entry_date","peak_date","peak_position","date_source"]

OUT_SONGS  = "data/songs_top10_us.csv"
OUT_ALBUMS = "data/albums_us_1m.csv"    # placeholder
OUT_MOVIES = "data/movies_rt80.csv"     # placeholder
STATE_SONGS = "data/state_songs.json"

WIKI_PAGE_TPL = "https://en.wikipedia.org/wiki/List_of_Billboard_Hot_100_top-ten_singles_in_{year}"

# ---------- HTTP helpers ----------

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

# ---------- date helpers (chart dates ONLY) ----------

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
    # Try common formats around year_hint
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

# ---------- state + IO ----------

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
            # Normalize and ensure all fields exist
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

# ---------- parsing helpers ----------

BRACKET_RX = re.compile(r"\[[^\]]*\]")         # [A], [27], etc.
SUPMARK_RX = re.compile(r"[↑↓*†‡]")            # arrows, asterisks, daggers
QUOTES_RX  = re.compile(r'^[\'"]+|[\'"]+$')    # leading/trailing quotes
WS_RX      = re.compile(r"\s+")

def clean_text(s: str) -> str:
    s = (s or "").strip()
    s = BRACKET_RX.sub("", s)
    s = SUPMARK_RX.sub("", s)
    s = QUOTES_RX.sub("", s)
    s = WS_RX.sub(" ", s).strip()
    return s

def cell_text(td) -> str:
    # Prefer anchor text for title; else the full cell text
    a = td.xpath(".//a[1]/text()") if td is not None else []
    if a:
        return clean_text(a[0])
    return clean_text("".join(td.itertext())) if td is not None else ""

def cell_num(td) -> str:
    t = clean_text("".join(td.itertext())) if td is not None else ""
    m = re.search(r"\d+", t)
    return m.group(0) if m else ""

def looks_like_proper_table(header_cells: List[str]) -> bool:
    h = [clean_text(x).lower() for x in header_cells]
    has_title  = any(k in " ".join(h) for k in ["single", "song", "title"])
    has_artist = any("artist" in x for x in h)
    has_peak   = any("peak" in x for x in h)
    has_date   = any(("entry" in x) or ("week of" in x) or (x.strip() == "date") for x in h)
    return has_title and has_artist and has_peak and has_date

def text_without_sup(el) -> str:
    # join all text nodes that are NOT inside <sup>
    return clean_text("".join(el.xpath('.//text()[not(ancestor::sup)]'))) if el is not None else ""

def parse_year_page(resp_text: str, year: int, url: str) -> List[Dict]:
    out: List[Dict] = []
    tree = html.fromstring(resp_text)
    tables = tree.xpath('//table[contains(@class,"wikitable")]')

    for tbl in tables:
        # header cells
        ths = tbl.xpath(".//tr[1]/th")
        if not ths:
            continue
        header_texts = ["".join(th.itertext()).strip() for th in ths]

        # ensure this is one of the main data tables (has title, artist, date, peak)
        if not looks_like_proper_table(header_texts):
            continue

        ncols = len(ths)
        col_names = [clean_text("".join(th.itertext())).lower() for th in ths]

        def idx(options: List[str]) -> Optional[int]:
            for opt in options:
                for i, name in enumerate(col_names):
                    if opt in name:
                        return i
            return None

        i_title = idx(["single","song","title","track"])
        i_artist = idx(["artist"])
        i_entry = idx([
            "entry date","debut","first week in top ten","first week",
            "top ten entry","top-ten entry","top ten debut","top-ten debut",
            "first top ten","first top-ten"
        ])
        i_week = idx(["week of","date"])  # fallback if no explicit entry col
        i_peak = idx(["peak","peak pos","peak position","highest pos"])
        i_peak_date = idx(["peak date","date peaked"])

        if i_title is None or i_artist is None or (i_entry is None and i_week is None) or i_peak is None:
            continue

        last_entry_text = ""  # forward-fill entry date when the first column uses rowspan

        # iterate body rows
        for tr in tbl.xpath(".//tr[position()>1]"):
            # Skip section header rows like "Singles from 2024/2025"
            tr_ths = tr.xpath("./th")
            tr_tds = tr.xpath("./td")
            if (tr_ths and not tr_tds) or (len(tr_tds) == 1 and tr_tds[0].get("colspan")):
                continue

            if not tr_tds:
                continue

            # Detect rowspan deficit (entry date missing in this row -> columns shift left by 1)
            deficit = ncols - len(tr_tds) if len(tr_tds) < ncols else 0

            def td_at(col_idx: Optional[int]):
                if col_idx is None:
                    return None
                if deficit == 0:
                    return tr_tds[col_idx] if col_idx < len(tr_tds) else None
                # If a previous row is rowspanning the first column, everything after col 0 shifts left by 1
                if col_idx == 0:
                    return None  # not present in this row; rely on forward fill
                adj = col_idx - 1
                return tr_tds[adj] if 0 <= adj < len(tr_tds) else None

            title_td  = td_at(i_title)
            artist_td = td_at(i_artist)
            entry_td  = td_at(i_entry) if i_entry is not None else None
            week_td   = td_at(i_week)  if i_week  is not None else None
            peak_td   = td_at(i_peak)
            peakd_td  = td_at(i_peak_date) if i_peak_date is not None else None

            title  = cell_text(title_td)
            artist = text_without_sup(artist_td)

            # guard against junk
            if not title or not artist:
                continue
            if re.fullmatch(r"\d+(\.\d+)?", title) or re.fullmatch(r"\d+(\.\d+)?", artist):
                continue

            # Resolve entry date with forward fill
            entry_raw = cell_text(entry_td) if entry_td is not None else ""
            if not entry_raw and i_entry is not None:
                entry_raw = last_entry_text
            elif entry_raw:
                last_entry_text = entry_raw

            week_raw  = cell_text(week_td)  if week_td  is not None else ""
            peak_raw  = cell_num(peak_td)   if peak_td  is not None else ""
            peakd_raw = cell_text(peakd_td) if peakd_td is not None else ""

            entry_date = parse_first_date(entry_raw, year) or parse_first_date(week_raw, year) or f"{year}-12-31"
            peak_date  = parse_first_date(peakd_raw, year) if peakd_raw else ""

            # IMPORTANT: Do NOT set release_date/month/day here. That is filled later by the date fetcher.
            out.append({
                "work_type": "song",
                "title": title,
                "byline": artist,
                "release_date": "",         # leave blank (real release date fetched later)
                "month": "",                # derived from release_date later
                "day": "",                  # derived from release_date later
                "extra": "US Top 10",
                "source_url": url,
                "entry_date": entry_date,
                "peak_date": peak_date,
                "peak_position": peak_raw,
                "date_source": ""           # set later by date fetcher (wikidata/wikitext)
            })

    return out

# ---------- combine/dedupe/incremental ----------

def dedupe_keep_earliest(rows: List[Dict]) -> List[Dict]:
    # dedupe by (title, byline) keeping earliest entry_date
    dedup: Dict[Tuple[str,str], Dict] = {}
    for r in rows:
        key = (r["title"].lower(), r["byline"].lower())
        if key not in dedup:
            dedup[key] = r
        else:
            a = dedup[key].get("entry_date","")
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

    keep_years = set(target_years)
    untouched = [r for r in existing if (year_from_url(r.get("source_url","")) or 0) not in keep_years]

    combined = dedupe_keep_earliest(fresh_rows + untouched)
    save_state(state)
    return combined

# ---------- main ----------

def main():
    full_build = os.getenv("FULL_BUILD", "false").lower() == "true"

    songs = harvest_songs_incremental(full_build=full_build)
    write_songs(songs)

    # Placeholders to keep workflow green
    write_empty_standard(OUT_ALBUMS)
    write_empty_standard(OUT_MOVIES)

    print(f"Wrote {OUT_SONGS} [{len(songs)} rows] (full_build={full_build})")
    print(f"Initialized placeholders: {OUT_ALBUMS}, {OUT_MOVIES}")

if __name__ == "__main__":
    main()
