#!/usr/bin/env python3
# scripts/build_albums_release_delta.py
#
# Build a daily delta of album release rows from data/albums_canon.csv.
#
# Intended use:
#   - albums_canon.csv is maintained nightly by build_albums_canon.py
#   - This script selects rows whose added_on == today and which have a
#     full mb_release_date_iso (YYYY-MM-DD).
#   - It outputs a small CSV with one row per album release including
#     sales info (sales_raw, shipments_units), ready for ingestion into
#     your OTD pipeline (e.g. into Google Sheets, then into calendar_index).
#
# Output columns:
#   work_type       "album"
#   title           album
#   byline          artist
#   release_date    mb_release_date_iso
#   month           MM from release_date
#   day             DD from release_date
#   extra           (blank for now)
#   source_url      albums_canon.source_url
#   sales_raw       albums_canon.sales_raw
#   shipments_units albums_canon.shipments_units
#   date_source     "musicbrainz:first-release-date"
#   added_on        albums_canon.added_on
#
# You can then use sales_raw when building summary_template lines like:
#   "with sales of over {sales_raw}."

import os
import argparse
from datetime import date
from typing import List

import pandas as pd

ALBUMS_CANON_DEFAULT = "data/albums_canon.csv"
OUT_PATH_DEFAULT = "data/albums_release_delta.csv"


def load_albums_canon(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"No albums canon file found at {path}")
        return pd.DataFrame(
            columns=[
                "year",
                "artist",
                "album",
                "label",
                "sales_raw",
                "certification",
                "country",
                "shipments_units",
                "list_source",
                "source_url",
                "musicbrainz_id",
                "mb_release_date_iso",
                "mb_release_year",
                "mb_country",
                "added_on",
            ]
        )

    df = pd.read_csv(path, encoding="utf-8")

    # Ensure expected columns exist
    for col in [
        "artist",
        "album",
        "sales_raw",
        "shipments_units",
        "source_url",
        "mb_release_date_iso",
        "added_on",
    ]:
        if col not in df.columns:
            df[col] = ""

    return df


def is_full_iso_date(value: str) -> bool:
    """
    Very simple check for YYYY-MM-DD. We only want full dates
    (not YYYY or YYYY-MM) for OTD usage.
    """
    if not isinstance(value, str):
        return False
    v = value.strip()
    if len(v) != 10:
        return False
    if v[4] != "-" or v[7] != "-":
        return False
    yyyy, mm, dd = v.split("-")
    if not (yyyy.isdigit() and mm.isdigit() and dd.isdigit()):
        return False
    return True


def build_release_delta(canon: pd.DataFrame) -> pd.DataFrame:
    today_str = date.today().isoformat()

    # Filter to rows added/updated today
    mask_added_today = canon["added_on"].astype(str).str.strip() == today_str
    df = canon[mask_added_today].copy()

    if df.empty:
        print("No albums with added_on == today; nothing to output.")
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

    # Only keep albums with a full YYYY-MM-DD release date
    df["mb_release_date_iso"] = df["mb_release_date_iso"].astype(str).str.strip()
    mask_full_date = df["mb_release_date_iso"].apply(is_full_iso_date)
    df = df[mask_full_date].copy()

    if df.empty:
        print("No albums with full mb_release_date_iso for today; nothing to output.")
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

    # Build the output structure
    out_rows: List[dict] = []

    for _, row in df.iterrows():
        rel = str(row.get("mb_release_date_iso", "")).strip()
        yyyy, mm, dd = rel.split("-")

        out_rows.append(
            {
                "work_type": "album",
                "title": str(row.get("album", "")).strip(),
                "byline": str(row.get("artist", "")).strip(),
                "release_date": rel,
                "month": int(mm),
                "day": int(dd),
                "extra": "",
                "source_url": str(row.get("source_url", "")).strip(),
                "sales_raw": str(row.get("sales_raw", "")).strip(),
                "shipments_units": row.get("shipments_units", ""),
                "date_source": "musicbrainz:first-release-date",
                "added_on": str(row.get("added_on", "")).strip(),
            }
        )

    out_df = pd.DataFrame(out_rows)

    # Stable sort: by release_date then title
    out_df = out_df.sort_values(
        by=["release_date", "title"],
        ascending=[True, True],
        ignore_index=True,
    )

    return out_df


def main():
    ap = argparse.ArgumentParser(
        description="Build a daily albums release delta CSV from albums_canon.csv."
    )
    ap.add_argument(
        "--in",
        dest="in_path",
        default=ALBUMS_CANON_DEFAULT,
        help="Input albums canon CSV (default: data/albums_canon.csv)",
    )
    ap.add_argument(
        "--out",
        dest="out_path",
        default=OUT_PATH_DEFAULT,
        help="Output delta CSV path (default: data/albums_release_delta.csv)",
    )
    args = ap.parse_args()

    canon = load_albums_canon(args.in_path)
    print(f"Loaded {len(canon)} rows from {args.in_path}")

    delta = build_release_delta(canon)
    print(f"Delta rows to write: {len(delta)}")

    out_dir = os.path.dirname(args.out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    delta.to_csv(args.out_path, index=False, encoding="utf-8")
    print(f"Wrote {args.out_path}")


if __name__ == "__main__":
    main()
