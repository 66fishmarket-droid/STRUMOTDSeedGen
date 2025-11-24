#!/usr/bin/env python3
# scripts/add_albums_to_calendar_index.py
#
# Take the daily albums_release_delta.csv and fold it into
# data/calendar_index.csv as "album release" facts.
#
# Assumptions:
#   - data/albums_release_delta.csv is produced by
#       scripts/build_albums_release_delta.py
#   - calendar_index.csv uses the same schema as your OTD Repo:
#       key_mmdd,year,iso_date,fact_domain,fact_category,fact_tags,
#       title,byline,role,work_type,summary_template,source_url,
#       source_system,source_id,country,language,extra,used_on,
#       use_count,added_on
#
# Deduplication key:
#   (work_type, iso_date, lower(title), lower(byline))
#
# Existing rows win; we only append genuinely new album rows.

import os
import argparse
from typing import Dict, Tuple, List

import pandas as pd

CALENDAR_INDEX_DEFAULT = "data/calendar_index.csv"
ALBUMS_DELTA_DEFAULT = "data/albums_release_delta.csv"

CAL_BASE_COLS = [
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


def load_calendar_index(path: str) -> pd.DataFrame:
    """Load or initialize calendar_index.csv with the expected columns."""
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception:
            df = pd.DataFrame(columns=CAL_BASE_COLS)
    else:
        df = pd.DataFrame(columns=CAL_BASE_COLS)

    # Ensure all columns exist
    for col in CAL_BASE_COLS:
        if col not in df.columns:
            # use_count numeric, others as string
            if col == "use_count":
                df[col] = 0
            else:
                df[col] = ""

    # Coerce use_count to numeric (default 0)
    df["use_count"] = pd.to_numeric(df["use_count"], errors="coerce").fillna(0).astype(int)

    return df[CAL_BASE_COLS]


def load_albums_delta(path: str) -> pd.DataFrame:
    """Load the albums_release_delta.csv; returns empty frame if missing."""
    if not os.path.exists(path):
        print(f"No albums delta file found at {path}; nothing to do.")
        return pd.DataFrame(
            columns=[
                "work_type",
                "title",
                "byline",
                "release_date",
                "month",
                "day",
                "extra",
                "source_url",
                "sales_raw",
                "shipments_units",
                "date_source",
                "added_on",
            ]
        )

    df = pd.read_csv(path, encoding="utf-8")

    # Ensure expected columns exist
    for col in [
        "work_type",
        "title",
        "byline",
        "release_date",
        "month",
        "day",
        "source_url",
        "sales_raw",
        "shipments_units",
        "added_on",
    ]:
        if col not in df.columns:
            df[col] = ""

    return df


def make_summary_template(title: str, byline: str, iso_date: str, sales_raw: str, shipments_units) -> str:
    """
    Build a human-friendly summary template with sales and platinum info.
    We lean on shipments_units for the numeric part, but keep sales_raw
    as the phrasing when present.
    """
    # Year from ISO date
    year = ""
    if isinstance(iso_date, str) and len(iso_date) >= 4:
        year = iso_date[:4]

    # Clean up sales_raw a bit (remove citation brackets like [5])
    sales_raw = str(sales_raw or "").strip()
    if sales_raw:
        import re

        sales_clean = re.sub(r"\[[^\]]*\]", "", sales_raw).strip()
    else:
        sales_clean = ""

    # Best-effort numeric shipments
    try:
        units = int(float(shipments_units))
    except Exception:
        units = 0

    # Decide how to say the sales figure
    if sales_clean:
        sales_phrase = sales_clean
    elif units > 0:
        sales_phrase = f"{units:,}"
    else:
        sales_phrase = "millions of"

    # Platinum multiplier (floor)
    platinum_x = units // 1_000_000 if units > 0 else 0

    base = f"On this day in {year}, the album '{title}'"
    if byline:
        base += f" by {byline}"
    base += " was released."

    # Sales sentence
    sales_sentence = f" To date, over {sales_phrase} copies have been sold."

    # Platinum sentence (only if we have a reasonable number)
    if platinum_x > 0:
        platinum_sentence = f" This makes it a {platinum_x}× platinum seller."
    else:
        platinum_sentence = ""

    return (base + sales_sentence + platinum_sentence).strip()


def build_album_calendar_rows(delta: pd.DataFrame) -> pd.DataFrame:
    """Convert albums_release_delta rows into calendar_index-style rows."""
    if delta.empty:
        return pd.DataFrame(columns=CAL_BASE_COLS)

    rows: List[Dict[str, object]] = []

    for _, r in delta.iterrows():
        iso_date = str(r.get("release_date", "")).strip()
        title = str(r.get("title", "")).strip()
        byline = str(r.get("byline", "")).strip()
        month = r.get("month", "")
        day = r.get("day", "")

        if not iso_date or not title:
            continue

        try:
            year_int = int(iso_date.split("-")[0])
        except Exception:
            year_int = ""

        try:
            mm_int = int(month)
            dd_int = int(day)
        except Exception:
            # Fall back to parsing from iso_date if month/day missing
            try:
                parts = iso_date.split("-")
                mm_int = int(parts[1])
                dd_int = int(parts[2])
            except Exception:
                continue

        # If your key_mmdd uses "MMDD" (e.g. 0101), use this:
        key_mmdd = f"{mm_int:02d}{dd_int:02d}"

        sales_raw = r.get("sales_raw", "")
        shipments_units = r.get("shipments_units", 0)
        summary = make_summary_template(title, byline, iso_date, sales_raw, shipments_units)

        source_url = str(r.get("source_url", "")).strip()
        added_on = str(r.get("added_on", "")).strip()

        # Build a simple deterministic source_id
        source_id = f"album::{iso_date}::{title.lower().strip()}::{byline.lower().strip()}"

        rows.append(
            {
                "key_mmdd": key_mmdd,
                "year": year_int,
                "iso_date": iso_date,
                "fact_domain": "music",
                "fact_category": "album_release",
                "fact_tags": "music;album;best_selling",
                "title": title,
                "byline": byline,
                "role": "artist",
                "work_type": "album",
                "summary_template": summary,
                "source_url": source_url,
                "source_system": "albums_canon",
                "source_id": source_id,
                "country": "",
                "language": "en",
                "extra": "",
                "used_on": "",
                "use_count": 0,
                "added_on": added_on,
            }
        )

    return pd.DataFrame(rows, columns=CAL_BASE_COLS)


def merge_calendar(existing: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new album rows into calendar_index, deduping by:
      (work_type, iso_date, lower(title), lower(byline))
    Existing rows win; we only append rows that don't already exist.
    """
    if new_rows.empty:
        return existing

    existing = existing.copy()
    new_rows = new_rows.copy()

    def key_tuple(row) -> Tuple[str, str, str, str]:
        return (
            str(row.get("work_type", "")).strip().lower(),
            str(row.get("iso_date", "")).strip(),
            str(row.get("title", "")).strip().lower(),
            str(row.get("byline", "")).strip().lower(),
        )

    if not existing.empty:
        existing["_key"] = existing.apply(key_tuple, axis=1)
        existing_keys = set(existing["_key"])
    else:
        existing["_key"] = []
        existing_keys = set()

    to_append: List[int] = []
    new_rows["_key"] = new_rows.apply(key_tuple, axis=1)

    for idx, row in new_rows.iterrows():
        k = row["_key"]
        if k in existing_keys:
            continue
        to_append.append(idx)

    if to_append:
        append_df = new_rows.loc[to_append, CAL_BASE_COLS]
        existing = pd.concat(
            [existing.drop(columns=["_key"], errors="ignore"), append_df],
            ignore_index=True,
        )
    else:
        existing = existing.drop(columns=["_key"], errors="ignore")

    # Stable sort: by iso_date then work_type then title
    existing = existing.sort_values(
        by=["iso_date", "work_type", "title"],
        ascending=[True, True, True],
        ignore_index=True,
    )

    # Return with columns in the standard order
    return existing[CAL_BASE_COLS]


def main():
    ap = argparse.ArgumentParser(
        description="Add albums from albums_release_delta.csv into calendar_index.csv."
    )
    ap.add_argument(
        "--calendar",
        dest="calendar_path",
        default=CALENDAR_INDEX_DEFAULT,
        help="Path to calendar_index.csv (default: data/calendar_index.csv)",
    )
    ap.add_argument(
        "--albums-delta",
        dest="albums_delta_path",
        default=ALBUMS_DELTA_DEFAULT,
        help="Path to albums_release_delta.csv (default: data/albums_release_delta.csv)",
    )
    args = ap.parse_args()

    cal = load_calendar_index(args.calendar_path)
    print(f"Loaded {len(cal)} rows from {args.calendar_path}")

    delta = load_albums_delta(args.albums_delta_path)
    print(f"Loaded {len(delta)} album delta rows from {args.albums_delta_path}")

    album_rows = build_album_calendar_rows(delta)
    print(f"Transformed into {len(album_rows)} calendar rows")

    merged = merge_calendar(cal, album_rows)
    print(f"Calendar index total rows after merge: {len(merged)}")

    out_dir = os.path.dirname(args.calendar_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    merged.to_csv(args.calendar_path, index=False, encoding="utf-8")
    print(f"Wrote {args.calendar_path}")


if __name__ == "__main__":
    main()
