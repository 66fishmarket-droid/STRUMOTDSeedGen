#!/usr/bin/env python3
# scripts/build_albums_us_1m.py
#
# Build/refresh otd/albums_us_1m.csv from Wikipedia:
# "List of best-selling albums in the United States".
#
# Strategy:
#   - Fetch the page HTML.
#   - Use pandas.read_html to extract tables that have Album + Artist columns.
#   - Normalise and combine to a single DataFrame.
#   - Extract numeric shipment units where possible.
#   - Merge into existing otd/albums_us_1m.csv (if present) using (artist, album) as key.
#   - Write updated CSV.

import os
import re
import argparse
from typing import Dict, Tuple

import requests
import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_best-selling_albums_in_the_United_States"

OUT_PATH_DEFAULT = "otd/albums_us_1m.csv"

# --------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------

def ua_contact() -> str:
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")

def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": f"StrumAlbums/1.0 (+{ua_contact()})",
            "Accept": "text/html,application/xhtml+xml",
        }
    )
    return s

# --------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------

def extract_units(text: str) -> int:
    """
    Best-effort extraction of shipment/sales units from strings like:
      "(15,550,000)"
      "15,000,000"
      "5,000,000+"
      "10× Platinum"
    Returns an integer number of units when obvious, else 0.
    """
    if not isinstance(text, str):
        return 0
    t = text.strip()
    # First, look for something that looks like a number with commas
    m = re.search(r"(\d[\d,]*)", t)
    if not m:
        return 0
    num_txt = m.group(1).replace(",", "")
    try:
        return int(num_txt)
    except ValueError:
        return 0

def normalise_colnames(cols):
    """
    Normalise column names to lowercase with simple tokens.
    """
    norm = []
    for c in cols:
        c_str = str(c).strip().lower()
        c_str = c_str.replace("\xa0", " ")
        norm.append(c_str)
    return norm

def looks_like_album_table(df: pd.DataFrame) -> bool:
    cols = normalise_colnames(df.columns)
    return ("album" in cols) and ("artist" in cols)

# --------------------------------------------------------------------
# Core builder
# --------------------------------------------------------------------

def fetch_album_tables() -> pd.DataFrame:
    """
    Fetch the Wikipedia page and return a combined DataFrame
    of all album rows we care about.
    """
    s = http_session()
    resp = s.get(WIKI_URL, timeout=30)
    resp.raise_for_status()
    html = resp.text

    tables = pd.read_html(html)
    frames = []

    for raw in tables:
        if not looks_like_album_table(raw):
            continue

        df = raw.copy()
        cols = normalise_colnames(df.columns)
        df.columns = cols

        # Map a few likely column names to our canonical ones
        col_map = {}
        for c in df.columns:
            if c.startswith("year"):
                col_map[c] = "year"
            elif c.startswith("artist"):
                col_map[c] = "artist"
            elif c.startswith("album"):
                col_map[c] = "album"
            elif "label" in c:
                col_map[c] = "label"
            elif "shipment" in c or "sales" in c:
                col_map[c] = "shipments_raw"
            elif "certification" in c:
                col_map[c] = "certification"

        df = df.rename(columns=col_map)

        # Keep only useful columns
        keep = ["year", "artist", "album", "label", "shipments_raw", "certification"]
        for k in keep:
            if k not in df.columns:
                df[k] = ""

        df = df[keep].copy()

        # Clean up text
        for col in ["year", "artist", "album", "label", "shipments_raw", "certification"]:
            df[col] = df[col].astype(str).str.strip()

        # Extract shipment units
        df["shipments_units"] = df["shipments_raw"].apply(extract_units)

        # Drop clearly empty rows (no artist or album)
        df = df[(df["artist"] != "") & (df["album"] != "")]
        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError("No album tables found on Wikipedia page")

    combined = pd.concat(frames, ignore_index=True)

    # Add source URL for traceability
    combined["source_url"] = WIKI_URL

    return combined

def load_existing(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(
            columns=[
                "year",
                "artist",
                "album",
                "label",
                "shipments_raw",
                "shipments_units",
                "certification",
                "source_url",
            ]
        )
    return pd.read_csv(path, encoding="utf-8")

def key_from_row(row: pd.Series) -> Tuple[str, str]:
    return (str(row.get("artist", "")).strip().lower(), str(row.get("album", "")).strip().lower())

def merge_albums(existing: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
    """
    Merge fresh scraped rows into existing, using (artist, album) as key.
    Prefer fresh values where they are non-empty / non-zero.
    """
    existing = existing.copy()
    fresh = fresh.copy()

    existing["_key"] = existing.apply(key_from_row, axis=1)
    fresh["_key"] = fresh.apply(key_from_row, axis=1)

    existing_map: Dict[Tuple[str, str], int] = {k: i for i, k in enumerate(existing["_key"])}

    new_rows = []
    updated_count = 0
    unchanged_count = 0

    for _, row in fresh.iterrows():
        k = row["_key"]
        if k in existing_map:
            idx = existing_map[k]
            old = existing.loc[idx]

            # Decide if anything meaningful changed
            changed = False
            for col in ["year", "label", "shipments_raw", "shipments_units", "certification"]:
                old_val = old.get(col, "")
                new_val = row.get(col, "")
                if pd.isna(old_val):
                    old_val = ""
                if pd.isna(new_val):
                    new_val = ""
                if str(new_val).strip() and str(new_val) != str(old_val):
                    existing.at[idx, col] = new_val
                    changed = True

            if changed:
                updated_count += 1
            else:
                unchanged_count += 1
        else:
            new_rows.append(row)

    # Append any brand-new rows
    if new_rows:
        existing = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)

    # Drop helper column
    if "_key" in existing.columns:
        existing = existing.drop(columns=["_key"])

    # Sort for stable output
    existing = existing.sort_values(by=["year", "artist", "album"], ascending=[True, True, True], ignore_index=True)

    return existing, updated_count, unchanged_count, len(new_rows)

# --------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Build albums_us_1m.csv from Wikipedia best-selling US albums.")
    ap.add_argument("--out", dest="out_path", default=OUT_PATH_DEFAULT, help="Output CSV path (default: otd/albums_us_1m.csv)")
    args = ap.parse_args()

    print(f"Fetching album tables from {WIKI_URL} ...")
    fresh = fetch_album_tables()
    print(f"Fetched {len(fresh)} rows from Wikipedia.")

    existing = load_existing(args.out_path)
    print(f"Existing rows in {args.out_path}: {len(existing)}")

    merged, updated_count, unchanged_count, new_count = merge_albums(existing, fresh)

    print("")
    print("==== Albums US 1M Update Summary ====")
    print(f"Total rows after merge: {len(merged)}")
    print(f"New albums added:      {new_count}")
    print(f"Existing albums updated: {updated_count}")
    print(f"Unchanged albums:      {unchanged_count}")
    print("====================================")
    print("")

    os.makedirs(os.path.dirname(args.out_path), exist_ok=True)
    merged.to_csv(args.out_path, index=False, encoding="utf-8")
    print(f"Wrote {args.out_path}")

if __name__ == "__main__":
    main()
