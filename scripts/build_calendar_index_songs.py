#!/usr/bin/env python3
# scripts/build_calendar_index_songs.py
#
# Build / update a unified calendar_index.csv with song-based
# "On This Day in the Arts" factoids, using songs_top10_us_with_dates.csv
#
# Policy:
# - fact_domain = "arts"
# - fact_category = "song"
# - fact_tags in {"release", "chart_entry", "no1"}
# - If a song has a no.1 peak, we do NOT create a chart_entry fact for it.
# - Only full dates (YYYY-MM-DD) produce day-specific facts.

import os
import re
import csv
import argparse
from datetime import datetime, date
from typing import Dict, List, Optional

SONGS_IN_DEFAULT = "data/songs_top10_us_with_dates.csv"
CALENDAR_DEFAULT = "data/calendar_index.csv"

SONG_SOURCE_SYSTEM = "songs_top10_us"
FULL_DATE_RX = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")

CALENDAR_FIELDS = [
    "key_mmdd",
    "year",
    "iso_date",
    "fact_domain",
    "fact_category",
    "fact_tags",
    "title",
    "byline",
    "role",
    "work_type",
    "summary_template",
    "source_url",
    "source_system",
    "source_id",
    "country",
    "language",
    "extra",
    "used_on",
    "use_count",
    "added_on",
]

# ---------- Helpers ----------

def parse_full_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    m = FULL_DATE_RX.match(s)
    if not m:
        return None
    y, mm, dd = m.groups()
    try:
        return date(int(y), int(mm), int(dd))
    except ValueError:
        return None

def read_csv_if_exists(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        # Handle possible empty file
        peek = f.read(1)
        if not peek:
            return []
        f.seek(0)
        reader = csv.DictReader(f)
        return list(reader)

def write_calendar(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CALENDAR_FIELDS)
        w.writeheader()
        for r in rows:
            # Ensure all fields exist as strings
            out = {}
            for k in CALENDAR_FIELDS:
                v = r.get(k, "")
                if v is None:
                    v = ""
                out[k] = str(v)
            w.writerow(out)

def compute_song_labels(row: Dict) -> List[str]:
    """Determine which labels conceptually apply to this song overall."""
    labels: List[str] = []

    release = (row.get("release_date") or "").strip()
    entry   = (row.get("entry_date") or "").strip()
    peak    = (row.get("peak_date") or "").strip()
    peak_raw = (row.get("peak_position") or "").strip()

    has_full_release = bool(FULL_DATE_RX.match(release))
    has_full_entry   = bool(FULL_DATE_RX.match(entry))
    has_full_peak    = bool(FULL_DATE_RX.match(peak))

    # Normalise peak position like "1" / "1.0" to an int
    peak_val = None
    if peak_raw:
        try:
            peak_val = int(float(peak_raw))
        except ValueError:
            peak_val = None

    is_no1 = (peak_val == 1)

    if has_full_release:
        labels.append("release")
    if has_full_entry:
        labels.append("chart_entry")
    if is_no1 and has_full_peak:
        labels.append("no1")

    # Ranking rule: if it has a no1 label, drop chart_entry entirely
    if "no1" in labels and "chart_entry" in labels:
        labels.remove("chart_entry")

    return labels

def build_extra_field(row: Dict) -> str:
    """Build a simple extra metadata string."""
    parts = []

    # We know these are all US Top 10
    parts.append("chart=US Top 10")

    for key in ("release_date", "entry_date", "peak_date", "peak_position", "date_source"):
        val = (row.get(key) or "").strip()
        if val:
            parts.append(f"{key}={val}")

    return ";".join(parts)

def build_song_facts(rows: List[Dict]) -> List[Dict]:
    """Turn songs CSV rows into calendar_index-like rows (songs only)."""
    facts: List[Dict] = []

    for r in rows:
        title = (r.get("title") or "").strip()
        byline = (r.get("byline") or "").strip()
        source_url = (r.get("source_url") or "").strip()
        work_type = (r.get("work_type") or "song").strip() or "song"
        peak_pos = (r.get("peak_position") or "").strip()

        labels = compute_song_labels(r)
        if not labels:
            continue

        release_d = parse_full_date(r.get("release_date") or "")
        entry_d   = parse_full_date(r.get("entry_date") or "")
        peak_d    = parse_full_date(r.get("peak_date") or "")

        extra = build_extra_field(r)

        # 1) Release fact
        if "release" in labels and release_d:
            mmdd = release_d.strftime("%m-%d")
            year = release_d.year

            template = (
                f'On this day in {year}, "{title}" by {byline} was released.'
            )
            # Optionally mention chart if we know it peaked high
            if peak_pos:
                template = (
                    f'On this day in {year}, "{title}" by {byline} was released. '
                    f'It later reached #{peak_pos} on the US charts.'
                )

            facts.append({
                "key_mmdd": mmdd,
                "year": str(year),
                "iso_date": release_d.isoformat(),
                "fact_domain": "arts",
                "fact_category": "song",
                "fact_tags": "release",
                "title": title,
                "byline": byline,
                "role": "artist",  # generic, can refine later
                "work_type": work_type,
                "summary_template": template,
                "source_url": source_url,
                "source_system": SONG_SOURCE_SYSTEM,
                "source_id": "",  # placeholder for future MBID etc.
                "country": "US",  # based on chart, not artist nationality
                "language": "en",  # good default for Billboard data
                "extra": extra,
                "used_on": "",
                "use_count": "0",
                "added_on": date.today().isoformat(),
            })

        # 2) Chart-entry fact (only if labels allow it)
        if "chart_entry" in labels and entry_d:
            mmdd = entry_d.strftime("%m-%d")
            year = entry_d.year

            base = f'On this day in {year}, "{title}" by {byline} entered the US Top 10.'
            if peak_pos:
                template = (
                    f'{base} It went on to reach #{peak_pos} on the chart.'
                )
            else:
                template = base

            facts.append({
                "key_mmdd": mmdd,
                "year": str(year),
                "iso_date": entry_d.isoformat(),
                "fact_domain": "arts",
                "fact_category": "song",
                "fact_tags": "chart_entry",
                "title": title,
                "byline": byline,
                "role": "artist",
                "work_type": work_type,
                "summary_template": template,
                "source_url": source_url,
                "source_system": SONG_SOURCE_SYSTEM,
                "source_id": "",
                "country": "US",
                "language": "en",
                "extra": extra,
                "used_on": "",
                "use_count": "0",
                "added_on": date.today().isoformat(),
            })

        # 3) Number-one fact
        if "no1" in labels and peak_d:
            mmdd = peak_d.strftime("%m-%d")
            year = peak_d.year

            template = (
                f'On this day in {year}, "{title}" by {byline} hit number one '
                f'on the US Top 10 chart.'
            )

            facts.append({
                "key_mmdd": mmdd,
                "year": str(year),
                "iso_date": peak_d.isoformat(),
                "fact_domain": "arts",
                "fact_category": "song",
                "fact_tags": "no1",
                "title": title,
                "byline": byline,
                "role": "artist",
                "work_type": work_type,
                "summary_template": template,
                "source_url": source_url,
                "source_system": SONG_SOURCE_SYSTEM,
                "source_id": "",
                "country": "US",
                "language": "en",
                "extra": extra,
                "used_on": "",
                "use_count": "0",
                "added_on": date.today().isoformat(),
            })

    return facts

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Build calendar_index.csv entries for songs.")
    ap.add_argument("--songs", dest="songs_path", default=SONGS_IN_DEFAULT, help="Songs CSV input path")
    ap.add_argument("--calendar", dest="calendar_path", default=CALENDAR_DEFAULT, help="Calendar index CSV path")
    args = ap.parse_args()

    if not os.path.exists(args.songs_path):
        raise SystemExit(f"Missing songs input file: {args.songs_path}")

    # Read songs
    songs_rows = []
    with open(args.songs_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        songs_rows = list(reader)

    print(f"Loaded {len(songs_rows)} song rows from {args.songs_path}")

    # Build song facts
    song_facts = build_song_facts(songs_rows)
    print(f"Built {len(song_facts)} song fact rows for calendar index")

    # Read existing calendar index (if any)
    existing = read_csv_if_exists(args.calendar_path)
    print(f"Loaded {len(existing)} existing calendar rows from {args.calendar_path}")

    # Keep only non-song rows (source_system != SONG_SOURCE_SYSTEM)
    kept_existing: List[Dict] = []
    for r in existing:
        if (r.get("source_system") or "").strip() != SONG_SOURCE_SYSTEM:
            kept_existing.append(r)

    print(f"Kept {len(kept_existing)} existing non-song rows")

    # Merge: existing non-song + new song facts
    merged = kept_existing + song_facts

    # Optional: sort by key_mmdd then year then fact_category for readability
    def sort_key(r: Dict):
        return (
            (r.get("key_mmdd") or ""),
            int(r.get("year") or 0),
            (r.get("fact_category") or ""),
            (r.get("title") or ""),
        )

    merged.sort(key=sort_key)

    print(f"Writing {len(merged)} rows back to {args.calendar_path}")
    write_calendar(args.calendar_path, merged)
    print("Done.")

if __name__ == "__main__":
    main()
