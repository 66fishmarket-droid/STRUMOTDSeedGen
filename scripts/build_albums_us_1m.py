#!/usr/bin/env python3
# scripts/build_albums_us_1m.py
#
# Build/refresh otd/albums_us_1m.csv from Wikipedia:
# "List of best-selling albums in the United States",
# and enrich with MusicBrainz release-group IDs, release date, and country.
#
# Strategy:
#   - Fetch the page HTML.
#   - Use pandas.read_html to extract tables that have Album + Artist columns.
#   - Normalise and combine to a single DataFrame.
#   - Extract numeric shipment units where possible.
#   - Merge into existing otd/albums_us_1m.csv (if present) using (artist, album) as key.
#   - For rows missing musicbrainz_id, query MusicBrainz for a release-group match.
#   - For rows with musicbrainz_id but missing mb_release_date_iso / mb_country,
#     query MusicBrainz release-group details.
#   - Write updated CSV.

import os
import re
import time
import argparse
from typing import Dict, Tuple, Optional

import requests
import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_best-selling_albums_in_the_United_States"
MB_SEARCH_BASE = "https://musicbrainz.org/ws/2/release-group"
MB_RG_BASE = "https://musicbrainz.org/ws/2/release-group/{mbid}"

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
            "User-Agent": f"StrumAlbums/1.2 (+{ua_contact()})",
            "Accept": "text/html,application/xhtml+xml,application/json",
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
    Returns an integer number of units when obvious, else 0.
    """
    if not isinstance(text, str):
        return 0
    t = text.strip()
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
# Core builder: Wikipedia
# --------------------------------------------------------------------

def fetch_album_tables(sess: requests.Session) -> pd.DataFrame:
    """
    Fetch the Wikipedia page and return a combined DataFrame
    of all album rows we care about.
    """
    resp = sess.get(WIKI_URL, timeout=30)
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
        for col in keep:
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
                "musicbrainz_id",
                "mb_release_date_iso",
                "mb_release_year",
                "mb_country",
            ]
        )
    df = pd.read_csv(path, encoding="utf-8")

    # Ensure enrichment columns exist
    for col in ["musicbrainz_id", "mb_release_date_iso", "mb_release_year", "mb_country"]:
        if col not in df.columns:
            df[col] = ""

    return df

def key_from_row(row: pd.Series) -> Tuple[str, str]:
    return (str(row.get("artist", "")).strip().lower(), str(row.get("album", "")).strip().lower())

def merge_albums(existing: pd.DataFrame, fresh: pd.DataFrame) -> Tuple[pd.DataFrame, int, int, int]:
    """
    Merge fresh scraped rows into existing, using (artist, album) as key.
    Prefer fresh values where they are non-empty / non-zero.
    """
    existing = existing.copy()
    fresh = fresh.copy()

    # Ensure enrichment columns exist
    for col in ["musicbrainz_id", "mb_release_date_iso", "mb_release_year", "mb_country"]:
        if col not in existing.columns:
            existing[col] = ""

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

            changed = False
            for col in ["year", "label", "shipments_raw", "shipments_units", "certification", "source_url"]:
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
            # New row; ensure enrichment columns exist, initialise empty
            row = row.copy()
            for col in ["musicbrainz_id", "mb_release_date_iso", "mb_release_year", "mb_country"]:
                if col not in row:
                    row[col] = ""
            new_rows.append(row)

    if new_rows:
        existing = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)

    if "_key" in existing.columns:
        existing = existing.drop(columns=["_key"])

    # Sort for stable output
    existing = existing.sort_values(by=["year", "artist", "album"], ascending=[True, True, True], ignore_index=True)

    return existing, updated_count, unchanged_count, len(new_rows)

# --------------------------------------------------------------------
# MusicBrainz enrichment
# --------------------------------------------------------------------

def mb_search_release_group(sess: requests.Session, album: str, artist: str) -> Optional[str]:
    """
    Query MusicBrainz for a release-group (album) match and return its MBID.
    We prefer results where primary-type == "Album" and with the highest score.
    """
    if not album or not artist:
        return None

    query = f'release:"{album}" AND artist:"{artist}" AND primarytype:album'
    params = {
        "query": query,
        "fmt": "json",
        "limit": 5,
    }

    try:
        r = sess.get(MB_SEARCH_BASE, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None

    groups = data.get("release-groups", [])
    if not groups:
        return None

    # Sort by (primary-type priority, score desc)
    def score_key(g):
        primary = g.get("primary-type") or ""
        score = g.get("score", 0)
        is_album = 1 if primary.lower() == "album" else 0
        return (is_album, score)

    groups_sorted = sorted(groups, key=score_key, reverse=True)
    best = groups_sorted[0]
    mbid = best.get("id")
    return mbid

def mb_get_release_group_details(sess: requests.Session, mbid: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Given a MusicBrainz release-group MBID, return (release_date_iso, country).
    We use first-release-date and, if possible, a country from the earliest release.
    """
    if not mbid:
        return None, None

    params = {
        "fmt": "json",
        "inc": "releases",
    }

    try:
        r = sess.get(MB_RG_BASE.format(mbid=mbid), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None, None

    # Release date
    rel_date = data.get("first-release-date") or ""

    # Country: look at earliest release with a country; else first country we see
    releases = data.get("releases", []) or []
    country = None
    best_date = None

    for rel in releases:
        ctry = rel.get("country") or ""
        date_str = rel.get("date") or ""
        if not ctry:
            continue

        if date_str:
            # Prefer the earliest dated release with a country
            if best_date is None or date_str < best_date:
                best_date = date_str
                country = ctry
        elif country is None:
            # Fallback: first country seen
            country = ctry

    # Normalise release date to ISO-ish (YYYY-MM-DD or partial)
    release_date_iso = rel_date.strip() or None

    return release_date_iso, country

def enrich_mbids(sess: requests.Session, df: pd.DataFrame, throttle: float = 1.1) -> Tuple[pd.DataFrame, int, int]:
    """
    For rows missing musicbrainz_id, query MusicBrainz and fill in MBIDs.
    Returns (df, num_filled, num_failed).
    """
    df = df.copy()
    if "musicbrainz_id" not in df.columns:
        df["musicbrainz_id"] = ""

    filled = 0
    failed = 0

    mask = (df["musicbrainz_id"].isna()) | (df["musicbrainz_id"].astype(str).str.strip() == "")
    candidates = df[mask]

    total = len(candidates)
    if total == 0:
        return df, filled, failed

    print(f"Attempting MusicBrainz ID enrichment for {total} albums...")

    for idx, row in candidates.iterrows():
        album = str(row.get("album", "")).strip()
        artist = str(row.get("artist", "")).strip()
        if not album or not artist:
            continue

        mbid = None
        try:
            mbid = mb_search_release_group(sess, album, artist)
        except Exception:
            mbid = None

        if mbid:
            df.at[idx, "musicbrainz_id"] = mbid
            filled += 1
        else:
            failed += 1

        time.sleep(throttle)

        if filled > 0 and filled % 25 == 0:
            print(f"  Filled {filled} MusicBrainz IDs so far...")

    return df, filled, failed

def enrich_mb_details(sess: requests.Session, df: pd.DataFrame, throttle: float = 1.1) -> Tuple[pd.DataFrame, int, int]:
    """
    For rows with musicbrainz_id but missing mb_release_date_iso or mb_country,
    query MusicBrainz release-group details and fill in.
    Returns (df, num_filled, num_failed).
    """
    df = df.copy()

    for col in ["mb_release_date_iso", "mb_release_year", "mb_country"]:
        if col not in df.columns:
            df[col] = ""

    def needs_details(row):
        mbid = str(row.get("musicbrainz_id", "")).strip()
        if not mbid:
            return False
        date_empty = not str(row.get("mb_release_date_iso", "")).strip()
        country_empty = not str(row.get("mb_country", "")).strip()
        return date_empty or country_empty

    mask = df.apply(needs_details, axis=1)
    candidates = df[mask]

    total = len(candidates)
    if total == 0:
        return df, 0, 0

    print(f"Attempting MusicBrainz detail enrichment for {total} albums...")

    filled = 0
    failed = 0

    for idx, row in candidates.iterrows():
        mbid = str(row.get("musicbrainz_id", "")).strip()
        if not mbid:
            continue

        rel_date_iso, country = mb_get_release_group_details(sess, mbid)

        if rel_date_iso or country:
            if rel_date_iso:
                df.at[idx, "mb_release_date_iso"] = rel_date_iso
                # Derive year
                year = rel_date_iso.split("-")[0]
                df.at[idx, "mb_release_year"] = year
            if country:
                df.at[idx, "mb_country"] = country
            filled += 1
        else:
            failed += 1

        time.sleep(throttle)

        if filled > 0 and filled % 25 == 0:
            print(f"  Filled details for {filled} albums so far...")

    return df, filled, failed

# --------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Build albums_us_1m.csv from Wikipedia best-selling US albums and enrich with MusicBrainz IDs and details."
    )
    ap.add_argument("--out", dest="out_path", default=OUT_PATH_DEFAULT, help="Output CSV path (default: otd/albums_us_1m.csv)")
    ap.add_argument("--mb-throttle", type=float, default=1.1, help="Seconds sleep between MusicBrainz calls (default: 1.1)")
    args = ap.parse_args()

    sess = http_session()

    print(f"Fetching album tables from {WIKI_URL} ...")
    fresh = fetch_album_tables(sess)
    print(f"Fetched {len(fresh)} rows from Wikipedia.")

    existing = load_existing(args.out_path)
    print(f"Existing rows in {args.out_path}: {len(existing)}")

    merged, updated_count, unchanged_count, new_count = merge_albums(existing, fresh)

    print("")
    print("==== Albums US 1M Merge Summary ====")
    print(f"Total rows after merge:    {len(merged)}")
    print(f"New albums added:          {new_count}")
    print(f"Existing albums updated:   {updated_count}")
    print(f"Unchanged albums:          {unchanged_count}")
    print("====================================")
    print("")

    # Enrich MusicBrainz IDs
    merged, mbid_filled, mbid_failed = enrich_mbids(sess, merged, throttle=args.mb_throttle)

    print("")
    print("==== MusicBrainz ID Enrichment Summary ====")
    print(f"Albums with new MBIDs:     {mbid_filled}")
    print(f"Lookup failures/empty:     {mbid_failed}")
    print("===========================================")
    print("")

    # Enrich MusicBrainz details (release date + country)
    merged, mbdet_filled, mbdet_failed = enrich_mb_details(sess, merged, throttle=args.mb_throttle)

    print("")
    print("==== MusicBrainz Detail Enrichment Summary ====")
    print(f"Albums with new details:   {mbdet_filled}")
    print(f"Detail lookup failures:    {mbdet_failed}")
    print("===============================================")
    print("")

    os.makedirs(os.path.dirname(args.out_path), exist_ok=True)
    merged.to_csv(args.out_path, index=False, encoding="utf-8")
    print(f"Wrote {args.out_path}")

if __name__ == "__main__":
    main()
