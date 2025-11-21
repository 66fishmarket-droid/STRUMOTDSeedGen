#!/usr/bin/env python3
# scripts/build_albums_canon.py
#
# Build/refresh data/albums_canon.csv from multiple Wikipedia
# "best-selling albums" style lists, plus a manual seed file,
# then enrich with MusicBrainz release-group IDs, release dates,
# and countries.
#
# Strategy:
#   - Load existing data/albums_canon.csv (if any).
#   - Merge in manual seed from data/best_selling_albums_enriched.csv.
#   - For each Wikipedia URL:
#       - Fetch HTML
#       - Extract tables that have Album + Artist columns
#       - Normalise and keep year/artist/album/label/sales/certification
#       - Tag each row with list_source + source_url
#   - Combine all wiki rows into a single DataFrame and dedupe by (artist, album),
#     keeping row with highest shipments_units.
#   - Merge deduped wiki data into existing canon using (artist, album):
#       - New rows: added_on = today
#       - Updated rows: bump added_on = today
#       - Unchanged rows: keep existing added_on
#   - Enrich missing musicbrainz_id via MusicBrainz search.
#   - Enrich missing mb_release_date_iso / mb_country via MusicBrainz details.
#   - Write updated CSV.

import os
import re
import time
import argparse
from io import StringIO
from typing import Dict, Tuple, Optional, List
from datetime import date

import requests
import pandas as pd

# Wikipedia lists to use as canonical album sources.
# You can add/remove URLs here as needed.
WIKI_ALBUM_URLS = [
    # Worldwide best-selling albums
    "https://en.wikipedia.org/wiki/List_of_best-selling_albums",
    # US-specific and diamond albums
    "https://en.wikipedia.org/wiki/List_of_best-selling_albums_in_the_United_States",
    "https://en.wikipedia.org/wiki/List_of_Diamond-certified_albums_in_the_United_States",
    # Best-selling by decade
    "https://en.wikipedia.org/wiki/List_of_best-selling_albums_of_the_1970s",
    "https://en.wikipedia.org/wiki/List_of_best-selling_albums_of_the_1980s",
    "https://en.wikipedia.org/wiki/List_of_best-selling_albums_of_the_1990s",
    "https://en.wikipedia.org/wiki/List_of_best-selling_albums_of_the_2000s",
    "https://en.wikipedia.org/wiki/List_of_best-selling_albums_of_the_2010s",
    "https://en.wikipedia.org/wiki/List_of_best-selling_albums_of_the_2020s",
]

MB_SEARCH_BASE = "https://musicbrainz.org/ws/2/release-group"
MB_RG_BASE = "https://musicbrainz.org/ws/2/release-group/{mbid}"

OUT_PATH_DEFAULT = "data/albums_canon.csv"

# --------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------


def ua_contact() -> str:
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")


def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": f"StrumAlbumsCanon/1.0 (+{ua_contact()})",
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
      "30,000,000"
      "15 million"
      "21× Platinum (US)"
    Returns an integer approximate number of units when obvious, else 0.
    """
    if not isinstance(text, str):
        return 0
    t = text.strip().lower()

    # First, try something like "15,000,000"
    m = re.search(r"(\d[\d,]*)", t)
    units = 0
    if m:
        num_txt = m.group(1).replace(",", "")
        try:
            units = int(num_txt)
        except ValueError:
            units = 0

    # Heuristic: handle "x million"
    if units == 0:
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*million", t)
        if m2:
            try:
                units = int(float(m2.group(1)) * 1_000_000)
            except ValueError:
                units = 0

    # Heuristic: x times platinum
    # (very rough; we just treat "x" platinum as x * 1,000,000)
    if units == 0 and "platinum" in t:
        m3 = re.search(r"(\d+)\s*[×x]\s*platinum", t)
        mult = 1
        if m3:
            try:
                mult = int(m3.group(1))
            except ValueError:
                mult = 1
        units = mult * 1_000_000

    return units


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
    return ("album" in cols or "title" in cols) and (
        "artist" in cols or "singer" in cols or "performer" in cols
    )


# --------------------------------------------------------------------
# Core builder: Wikipedia
# --------------------------------------------------------------------


def fetch_album_tables_for_url(
    sess: requests.Session, url: str, list_label: str
) -> pd.DataFrame:
    """
    Fetch a single Wikipedia page and return a DataFrame of album rows
    found on that page.
    """
    print(f"Fetching tables from {url} ...")
    try:
        resp = sess.get(url, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"  Skipping {url} due to HTTP error: {e}")
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
            ]
        )

    html = resp.text
    tables = pd.read_html(StringIO(html))
    frames: List[pd.DataFrame] = []

    for raw in tables:
        if not looks_like_album_table(raw):
            continue

        df = raw.copy()
        cols = normalise_colnames(df.columns)
        df.columns = cols

        # Map columns to canonical names
        col_map: Dict[str, str] = {}
        for c in df.columns:
            if c.startswith("year"):
                col_map[c] = "year"
            elif c.startswith("artist") or "singer" in c or "performer" in c:
                col_map[c] = "artist"
            elif c.startswith("album") or c.startswith("title"):
                col_map[c] = "album"
            elif "label" in c:
                col_map[c] = "label"
            elif "sales" in c or "copies" in c or "units" in c or "shipment" in c:
                col_map[c] = "sales_raw"
            elif "certification" in c or "certifications" in c:
                col_map[c] = "certification"
            elif "country" in c:
                col_map[c] = "country"

        df = df.rename(columns=col_map)

        # Drop duplicate columns after renaming
        df = df.loc[:, ~pd.Index(df.columns).duplicated()]

        # Ensure standard columns exist
        keep = ["year", "artist", "album", "label", "sales_raw", "certification", "country"]
        for k in keep:
            if k not in df.columns:
                df[k] = ""

        df = df[keep].copy()

        # Clean up text
        for col in keep:
            df[col] = df[col].astype(str).str.strip()

        # Compute numeric approximate units
        df["shipments_units"] = df["sales_raw"].apply(extract_units)

        # Remove obviously empty rows
        df = df[(df["artist"] != "") & (df["album"] != "")]
        if df.empty:
            continue

        df["list_source"] = list_label
        df["source_url"] = url

        frames.append(df)

    if not frames:
        print(f"  No album-like tables found on {url}")
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
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    print(f"  Found {len(combined)} album rows on {url}")
    return combined


def fetch_all_wiki_albums(sess: requests.Session) -> pd.DataFrame:
    """
    Fetch album tables from all configured Wikipedia URLs and combine them.
    """
    frames: List[pd.DataFrame] = []
    for url in WIKI_ALBUM_URLS:
        label = url.split("/wiki/")[-1]
        df = fetch_album_tables_for_url(sess, url, list_label=label)
        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError("No album rows found on any configured Wikipedia URLs")

    combined = pd.concat(frames, ignore_index=True)
    print(f"Total raw Wikipedia album rows (pre-dedup): {len(combined)}")
    return combined


# --------------------------------------------------------------------
# Existing file loading + seed bootstrap
# --------------------------------------------------------------------


def load_existing(path: str) -> pd.DataFrame:
    base_cols = [
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

    # 1) Load existing albums_canon.csv if it exists
    if os.path.exists(path):
        try:
            existing = pd.read_csv(path, encoding="utf-8")
        except Exception:
            existing = pd.DataFrame(columns=base_cols)

        if existing.empty and len(existing.columns) == 0:
            existing = pd.DataFrame(columns=base_cols)
    else:
        existing = pd.DataFrame(columns=base_cols)

    # Ensure all expected columns exist in existing
    for col in base_cols:
        if col not in existing.columns:
            existing[col] = ""

    # 2) Try to merge in the manual seed: data/best_selling_albums_enriched.csv
    seed_path = "data/best_selling_albums_enriched.csv"
    if os.path.exists(seed_path):
        seed_raw = pd.read_csv(seed_path, encoding="utf-8")

        # Expecting columns:
        # album,artist,release_year_hint,units_sold_raw,musicbrainz_id,
        # mb_release_date_iso,mb_release_year,mb_country

        seed = pd.DataFrame()
        seed["artist"] = seed_raw.get("artist", "").fillna("")
        seed["album"] = seed_raw.get("album", "").fillna("")
        seed["year"] = seed_raw.get("release_year_hint", "").fillna("")
        seed["label"] = ""
        seed["sales_raw"] = seed_raw.get("units_sold_raw", "").fillna("")
        seed["certification"] = ""
        seed["country"] = ""

        # Reuse extract_units to get numeric shipments
        seed["shipments_units"] = seed["sales_raw"].apply(extract_units)

        seed["list_source"] = "best_selling_seed"
        seed["source_url"] = ""

        seed["musicbrainz_id"] = seed_raw.get("musicbrainz_id", "").fillna("")
        seed["mb_release_date_iso"] = seed_raw.get("mb_release_date_iso", "").fillna("")
        seed["mb_release_year"] = seed_raw.get("mb_release_year", "").fillna("")
        seed["mb_country"] = seed_raw.get("mb_country", "").fillna("")

        today_str = date.today().isoformat()
        seed["added_on"] = today_str

        # Ensure all base columns exist on the seed frame
        for col in base_cols:
            if col not in seed.columns:
                seed[col] = ""

        # Merge seed into existing by (artist, album)
        def key_tuple(artist: str, album: str) -> Tuple[str, str]:
            return (str(artist).strip().lower(), str(album).strip().lower())

        existing_keys: Dict[Tuple[str, str], int] = {}
        if not existing.empty:
            existing["_key"] = existing.apply(
                lambda r: key_tuple(r.get("artist", ""), r.get("album", "")), axis=1
            )
            existing_keys = {k: i for i, k in enumerate(existing["_key"])}
        else:
            existing["_key"] = []

        new_rows: List[pd.Series] = []

        for _, row in seed.iterrows():
            artist = row.get("artist", "")
            album = row.get("album", "")
            if not str(artist).strip() or not str(album).strip():
                continue

            k = key_tuple(artist, album)

            if k in existing_keys:
                # Album already in canon: optionally fill missing MB data from the seed
                idx = existing_keys[k]
                for col in ["musicbrainz_id", "mb_release_date_iso", "mb_release_year", "mb_country"]:
                    old_val = str(existing.at[idx, col]).strip()
                    new_val = str(row.get(col, "")).strip()
                    if not old_val and new_val:
                        existing.at[idx, col] = new_val
            else:
                new_rows.append(row)

        if new_rows:
            # Append seed-only albums to existing
            existing = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)

        if "_key" in existing.columns:
            existing = existing.drop(columns=["_key"])

    # Final column ordering / guarantee
    for col in base_cols:
        if col not in existing.columns:
            existing[col] = ""

    return existing[base_cols]


# --------------------------------------------------------------------
# Dedupe + merge helpers
# --------------------------------------------------------------------


def key_from_row(row: pd.Series) -> Tuple[str, str]:
    return (
        str(row.get("artist", "")).strip().lower(),
        str(row.get("album", "")).strip().lower(),
    )


def dedupe_wiki_albums(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate raw Wikipedia album rows by (artist, album),
    keeping the row with the highest shipments_units (and roughly
    most complete data).
    """
    if raw.empty:
        return raw

    raw = raw.copy()
    raw["_key"] = raw.apply(key_from_row, axis=1)

    groups: List[pd.Series] = []
    for key, grp in raw.groupby("_key"):
        if len(grp) == 1:
            groups.append(grp.iloc[0])
            continue
        # Pick row with max shipments_units; if tie, first
        grp_sorted = grp.sort_values(by=["shipments_units"], ascending=False)
        groups.append(grp_sorted.iloc[0])

    deduped = pd.DataFrame(groups).reset_index(drop=True)
    if "_key" in deduped.columns:
        deduped = deduped.drop(columns=["_key"])

    print(f"Deduped across lists: {len(raw)} -> {len(deduped)} unique (artist, album)")
    return deduped


def merge_with_existing(
    existing: pd.DataFrame, fresh: pd.DataFrame
) -> Tuple[pd.DataFrame, int, int, int]:
    """
    Merge fresh Wikipedia data with existing albums_canon.csv using (artist, album) as key.
    Prefer fresh values for core fields when they are non-empty / non-zero.
    Returns (merged_df, new_count, updated_count, unchanged_count).

    added_on acts as a last-updated stamp:
      - New albums get added_on = today.
      - Existing albums with changes get added_on bumped to today.
      - Unchanged albums keep their existing added_on.
    """
    existing = existing.copy()
    fresh = fresh.copy()

    # Ensure enrichment / timestamp columns exist in existing
    for col in ["musicbrainz_id", "mb_release_date_iso", "mb_release_year", "mb_country", "added_on"]:
        if col not in existing.columns:
            existing[col] = ""

    today_str = date.today().isoformat()

    existing["_key"] = existing.apply(key_from_row, axis=1)
    fresh["_key"] = fresh.apply(key_from_row, axis=1)

    existing_map: Dict[Tuple[str, str], int] = {k: i for i, k in enumerate(existing["_key"])}

    new_rows: List[pd.Series] = []
    updated_count = 0
    unchanged_count = 0

    for _, row in fresh.iterrows():
        k = row["_key"]
        if k in existing_map:
            idx = existing_map[k]
            old = existing.loc[idx]

            changed = False
            # Update core descriptive fields if new data is better
            for col in [
                "year",
                "label",
                "sales_raw",
                "shipments_units",
                "certification",
                "country",
                "list_source",
                "source_url",
            ]:
                old_val = old.get(col, "")
                new_val = row.get(col, "")
                if pd.isna(old_val):
                    old_val = ""
                if pd.isna(new_val):
                    new_val = ""

                if col == "shipments_units":
                    # For shipments_units, take max
                    try:
                        old_units = int(old_val)
                    except Exception:
                        old_units = 0
                    try:
                        new_units = int(new_val)
                    except Exception:
                        new_units = 0
                    if new_units > old_units:
                        existing.at[idx, col] = new_units
                        changed = True
                else:
                    if str(new_val).strip() and str(new_val) != str(old_val):
                        existing.at[idx, col] = new_val
                        changed = True

            if changed:
                # Row has materially changed; bump added_on so downstream
                # pipelines (e.g. Google Sheets) see it as fresh.
                existing.at[idx, "added_on"] = today_str
                updated_count += 1
            else:
                unchanged_count += 1
        else:
            # Brand new album row
            row = row.copy()
            for col in ["musicbrainz_id", "mb_release_date_iso", "mb_release_year", "mb_country", "added_on"]:
                if col not in row:
                    row[col] = ""

            # Only set added_on if it is empty, so we do not overwrite any pre-seeded values.
            if not str(row.get("added_on", "")).strip():
                row["added_on"] = today_str

            new_rows.append(row)

    if new_rows:
        existing = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)

    if "_key" in existing.columns:
        existing = existing.drop(columns=["_key"])

    # Sort: by artist then year then album for stable output
    existing = existing.sort_values(
        by=["artist", "year", "album"],
        ascending=[True, True, True],
        ignore_index=True,
    )

    return existing, len(new_rows), updated_count, unchanged_count


# --------------------------------------------------------------------
# MusicBrainz enrichment
# --------------------------------------------------------------------


def mb_search_release_group(sess: requests.Session, album: str, artist: str) -> Optional[str]:
    """
    Query MusicBrainz for a release-group (album) match and return its MBID.
    Prefer results where primary-type == "Album" and with the highest score.
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

    def score_key(g):
        primary = (g.get("primary-type") or "").lower()
        score = g.get("score", 0)
        is_album = 1 if primary == "album" else 0
        return (is_album, score)

    groups_sorted = sorted(groups, key=score_key, reverse=True)
    best = groups_sorted[0]
    return best.get("id")


def mb_get_release_group_details(sess: requests.Session, mbid: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Given a MusicBrainz release-group MBID, return (release_date_iso, country).
    Using first-release-date and earliest dated country release where possible.
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

    rel_date = data.get("first-release-date") or ""
    releases = data.get("releases", []) or []

    country = None
    best_date = None

    for rel in releases:
        ctry = rel.get("country") or ""
        date_str = rel.get("date") or ""
        if not ctry:
            continue
        if date_str:
            if best_date is None or date_str < best_date:
                best_date = date_str
                country = ctry
        elif country is None:
            country = ctry

    release_date_iso = rel_date.strip() or None
    return release_date_iso, country


def enrich_mbids(sess: requests.Session, df: pd.DataFrame, throttle: float = 1.1) -> Tuple[pd.DataFrame, int, int]:
    """
    For rows missing musicbrainz_id, query MusicBrainz and fill MBIDs.
    Returns (df, num_filled, num_failed).
    """
    df = df.copy()
    if "musicbrainz_id" not in df.columns:
        df["musicbrainz_id"] = ""

    mask = (df["musicbrainz_id"].isna()) | (df["musicbrainz_id"].astype(str).str.strip() == "")
    candidates = df[mask]

    total = len(candidates)
    if total == 0:
        return df, 0, 0

    print(f"Attempting MusicBrainz ID enrichment for {total} albums...")

    filled = 0
    failed = 0

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
        if filled > 0 and filled % 50 == 0:
            print(f"  Filled {filled} MusicBrainz IDs so far...")

    return df, filled, failed


def enrich_mb_details(sess: requests.Session, df: pd.DataFrame, throttle: float = 1.1) -> Tuple[pd.DataFrame, int, int]:
    """
    For rows with musicbrainz_id but missing mb_release_date_iso or mb_country,
    query MusicBrainz release-group details.
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
                year = rel_date_iso.split("-")[0]
                df.at[idx, "mb_release_year"] = year
            if country:
                df.at[idx, "mb_country"] = country
            filled += 1
        else:
            failed += 1

        time.sleep(throttle)
        if filled > 0 and filled % 50 == 0:
            print(f"  Filled details for {filled} albums so far...")

    return df, filled, failed


# --------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(
        description="Build albums_canon.csv from multiple Wikipedia album lists and a seed file, then enrich with MusicBrainz data."
    )
    ap.add_argument(
        "--out",
        dest="out_path",
        default=OUT_PATH_DEFAULT,
        help="Output CSV path (default: data/albums_canon.csv)",
    )
    ap.add_argument(
        "--mb-throttle",
        type=float,
        default=1.1,
        help="Seconds sleep between MusicBrainz calls (default: 1.1)",
    )
    args = ap.parse_args()

    sess = http_session()

    # Fetch + dedupe from Wikipedia
    wiki_raw = fetch_all_wiki_albums(sess)
    wiki_deduped = dedupe_wiki_albums(wiki_raw)

    print(f"Deduped Wikipedia albums: {len(wiki_deduped)}")

    # Load existing canon file (if any), including seed bootstrap
    existing = load_existing(args.out_path)
    print(f"Existing rows in {args.out_path}: {len(existing)}")

    merged, new_count, updated_count, unchanged_count = merge_with_existing(
        existing, wiki_deduped
    )

    print("")
    print("==== Albums Canon Merge Summary ====")
    print(f"Total rows after merge:    {len(merged)}")
    print(f"New albums added:          {new_count}")
    print(f"Existing albums updated:   {updated_count}")
    print(f"Unchanged albums:          {unchanged_count}")
    print("===================================")
    print("")

    # Enrich MusicBrainz IDs
    merged, mbid_filled, mbid_failed = enrich_mbids(
        sess, merged, throttle=args.mb_throttle
    )

    print("")
    print("==== MusicBrainz ID Enrichment Summary ====")
    print(f"Albums with new MBIDs:     {mbid_filled}")
    print(f"Lookup failures/empty:     {mbid_failed}")
    print("===========================================")
    print("")

    # Enrich MusicBrainz details
    merged, mbdet_filled, mbdet_failed = enrich_mb_details(
        sess, merged, throttle=args.mb_throttle
    )

    print("")
    print("==== MusicBrainz Detail Enrichment Summary ====")
    print(f"Albums with new details:   {mbdet_filled}")
    print(f"Detail lookup failures:    {mbdet_failed}")
    print("===============================================")
    print("")

    out_dir = os.path.dirname(args.out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    merged.to_csv(args.out_path, index=False, encoding="utf-8")
    print(f"Wrote {args.out_path}")


if __name__ == "__main__":
    main()
