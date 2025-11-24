#!/usr/bin/env python3
# scripts/build_albums_release_delta.py
#
# Build/refresh data/albums_release_delta.csv from data/albums_canon.csv.
#
# We DO NOT filter by added_on here; instead we export all albums that:
#   - have a full day-level release date (YYYY-MM-DD) in mb_release_date_iso
#
# The "delta" behaviour (only using new rows) is handled downstream by
# Make.com / Sheets using the added_on column, just like the other pipelines.
#
# Output columns:
#   work_type       -> "album"
#   title           -> album
#   byline          -> artist
#   release_date    -> mb_release_date_iso (YYYY-MM-DD)
#   month           -> MM
#   day             -> DD
#   extra           -> optional info (e.g. certification)
#   source_url      -> source_url from albums_canon
#   sales_raw       -> sales_raw from albums_canon
#   shipments_units -> shipments_units from albums_canon
#   date_source     -> "musicbrainz:first-release-date"
#   added_on        -> added_on from albums_canon
#
# This file is then used by add_albums_to_calendar_index.py.

import os
import re
import argparse
from typing import List

import pandas as pd

IN_PATH_DEFAULT = "data/albums_canon.csv"
OUT_PATH_DEFAULT = "data/albums_release_delta.csv"

OUT_COLS = [
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


def load_albums_canon(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"albums_canon not found at {path}")

    df = pd.read_csv(path, encoding="utf-8")

    # Ensure expected columns exist
    expected = [
        "artist",
        "album",
        "sales_raw",
        "certification",
        "shipments_units",
        "source_url",
        "mb_release_date_iso",
        "mb_release_year",
        "mb_country",
        "added_on",
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = ""

    return df


def is_full_iso_date(s: str) -> bool:
    """Return True if s looks like YYYY-MM-DD."""
    if not isinstance(s, str):
        return False
    s = s.strip()
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", s))


def build_release_delta(df: pd.DataFrame) -> pd.DataFrame:
    """
    From albums_canon, build the albums_release_delta structure:
    one row per album with a full YYYY-MM-DD release date.
    """
    if df.empty:
        return pd.DataFrame(columns=OUT_COLS)

    rows: List[dict] = []

    for _, r in df.iterrows():
        album = str(r.get("album", "")).strip()
        artist = str(r.get("artist", "")).strip()
        rel_iso = str(r.get("mb_release_date_iso", "")).strip()

        if not album or not artist:
            continue

        if not is_full_iso_date(rel_iso):
            # Skip partial dates (e.g. "1984" or "1984-05")
            continue

        year_str, mm_str, dd_str = rel_iso.split("-")

        sales_raw = r.get("sales_raw", "")
        shipments_units = r.get("shipments_units", 0)
        certification = str(r.get("certification", "")).strip()
        source_url = str(r.get("source_url", "")).strip()
        added_on = str(r.get("added_on", "")).strip()

        # Extra: you can tuck certification in here if useful
        extra = ""
        if certification:
            extra = f"certification={certification}"

        rows.append(
            {
                "work_type": "album",
                "title": album,
                "byline": artist,
                "release_date": rel_iso,
                "month": int(mm_str),
                "day": int(dd_str),
                "extra": extra,
                "source_url": source_url,
                "sales_raw": sales_raw,
                "shipments_units": shipments_units,
                "date_source": "musicbrainz:first-release-date",
                "added_on": added_on,
            }
        )

    if not rows:
        return pd.DataFrame(columns=OUT_COLS)

    out = pd.DataFrame(rows, columns=OUT_COLS)

    # Sort by release_date then title for stability
    out = out.sort_values(by=["release_date", "title"], ascending=[True, True], ignore_index=True)

    return out


def main():
    ap = argparse.ArgumentParser(
        description="Build albums_release_delta.csv from albums_canon.csv."
    )
    ap.add_argument(
        "--in",
        dest="in_path",
        default=IN_PATH_DEFAULT,
        help="Input albums_canon.csv path (default: data/albums_canon.csv)",
    )
    ap.add_argument(
        "--out",
        dest="out_path",
        default=OUT_PATH_DEFAULT,
        help="Output albums_release_delta.csv path (default: data/albums_release_delta.csv)",
    )
    args = ap.parse_args()

    canon = load_albums_canon(args.in_path)
    print(f"Loaded {len(canon)} rows from {args.in_path}")

    delta = build_release_delta(canon)
    print(f"Built {len(delta)} album release rows")

    out_dir = os.path.dirname(args.out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    delta.to_csv(args.out_path, index=False, encoding="utf-8")
    print(f"Wrote {args.out_path}")


if __name__ == "__main__":
    main()
