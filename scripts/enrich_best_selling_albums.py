#!/usr/bin/env python3
# scripts/enrich_best_selling_albums.py
#
# Parse a manually scraped "Best Selling albums" Excel where:
#   - Album rows have: Rank, Album, Release Year, Units sold
#   - Artist rows appear a couple of rows below with only Album filled
#
# Then enrich with MusicBrainz:
#   - musicbrainz_id
#   - mb_release_date_iso
#   - mb_release_year
#   - mb_country
#
# Output: data/best_selling_albums_enriched.csv

import os
import time
import argparse
from typing import Optional, Tuple, List

import pandas as pd
import requests

IN_XLSX_DEFAULT = "data/best_selling_albums.xlsx"
OUT_CSV_DEFAULT = "data/best_selling_albums_enriched.csv"

MB_SEARCH_BASE = "https://musicbrainz.org/ws/2/release-group"
MB_RG_BASE = "https://musicbrainz.org/ws/2/release-group/{mbid}"


def ua_contact() -> str:
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")


def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": f"StrumBestSellingAlbums/1.0 (+{ua_contact()})",
            "Accept": "application/json,text/html,application/xhtml+xml",
        }
    )
    return s


def load_raw_xlsx(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input Excel not found: {path}")
    df = pd.read_excel(path)
    # Normalise column names a bit
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_album_artist_table(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Build a clean table:
      album, artist, release_year_hint, units_sold_raw
    from a messy layout where the artist name appears a couple of rows
    below the album row.
    """
    # Try to detect columns by fuzzy name
    cols = {c.lower(): c for c in raw.columns}

    def find_col(candidates: List[str]) -> Optional[str]:
        for key, orig in cols.items():
            for cand in candidates:
                if cand in key:
                    return orig
        return None

    col_rank = find_col(["rank"])
    col_album = find_col(["album"])
    col_year = find_col(["release year", "year"])
    col_units = find_col(["units", "sold", "sales"])

    if not col_album or not col_year:
        raise ValueError("Could not detect Album / Release Year columns in Excel.")

    # We treat an "album row" as a row where:
    #   - album text is non-empty
    #   - release year is not NaN
    albums = []

    used_as_artist = set()  # indices of rows already consumed as artist rows

    n = len(raw)
    for i in range(n):
        row = raw.iloc[i]
        album_val = str(row[col_album]).strip() if not pd.isna(row[col_album]) else ""
        year_val = row[col_year]

        if not album_val:
            continue
        if pd.isna(year_val):
            # Probably not an album row (might be an artist row or garbage)
            continue

        # Candidate album row
        release_year_hint = None
        try:
            release_year_hint = int(year_val)
        except Exception:
            # keep as None if not parseable
            pass

        units_raw = ""
        if col_units and not pd.isna(row[col_units]):
            units_raw = str(row[col_units]).strip()

        # Find artist row within the next couple of rows
        artist = ""
        for j in range(i + 1, min(i + 4, n)):
            if j in used_as_artist:
                continue
            r2 = raw.iloc[j]
            album2 = str(r2[col_album]).strip() if not pd.isna(r2[col_album]) else ""

            # artist row heuristic:
            # - album2 non-empty
            # - year NaN
            # - units NaN or empty
            year2 = r2[col_year] if col_year in raw.columns else None
            units2 = r2[col_units] if col_units and col_units in raw.columns else None

            year_is_nan = pd.isna(year2)
            units_is_nan_or_empty = (col_units is None) or pd.isna(units2) or (str(units2).strip() == "")

            if album2 and year_is_nan and units_is_nan_or_empty:
                artist = album2
                used_as_artist.add(j)
                break

        albums.append(
            {
                "album": album_val,
                "artist": artist,
                "release_year_hint": release_year_hint if release_year_hint is not None else "",
                "units_sold_raw": units_raw,
            }
        )

    df = pd.DataFrame(albums)
    # Drop obvious duplicates, just in case
    df = df.drop_duplicates(subset=["album", "artist", "release_year_hint"], keep="first").reset_index(drop=True)
    return df


# -------------------- MusicBrainz helpers --------------------


def mb_search_release_group(sess: requests.Session, album: str, artist: str) -> Optional[str]:
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

    best = sorted(groups, key=score_key, reverse=True)[0]
    return best.get("id")


def mb_get_release_group_details(sess: requests.Session, mbid: str) -> Tuple[Optional[str], Optional[str]]:
    if not mbid:
        return None, None

    params = {"fmt": "json", "inc": "releases"}

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


def enrich_with_musicbrainz(sess: requests.Session, df: pd.DataFrame, throttle: float = 1.1) -> pd.DataFrame:
    df = df.copy()
    for col in ["musicbrainz_id", "mb_release_date_iso", "mb_release_year", "mb_country"]:
        if col not in df.columns:
            df[col] = ""

    print(f"Attempting MusicBrainz enrichment for {len(df)} albums...")

    mbid_filled = 0
    mbid_failed = 0
    det_filled = 0
    det_failed = 0

    for idx, row in df.iterrows():
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
            mbid_filled += 1

            rel_date_iso, country = mb_get_release_group_details(sess, mbid)
            if rel_date_iso or country:
                if rel_date_iso:
                    df.at[idx, "mb_release_date_iso"] = rel_date_iso
                    year = rel_date_iso.split("-")[0]
                    df.at[idx, "mb_release_year"] = year
                if country:
                    df.at[idx, "mb_country"] = country
                det_filled += 1
            else:
                det_failed += 1
        else:
            mbid_failed += 1

        time.sleep(throttle)
        if mbid_filled > 0 and mbid_filled % 25 == 0:
            print(f"  Filled {mbid_filled} MBIDs so far...")

    print("")
    print("==== MusicBrainz Enrichment Summary ====")
    print(f"Albums with new MBIDs:     {mbid_filled}")
    print(f"MBID lookup failures:      {mbid_failed}")
    print(f"Albums with details filled:{det_filled}")
    print(f"Detail lookup failures:    {det_failed}")
    print("========================================")
    print("")

    return df


def main():
    ap = argparse.ArgumentParser(
        description="Clean manually scraped Best Selling albums Excel and enrich with MusicBrainz."
    )
    ap.add_argument(
        "--in-xlsx",
        dest="in_xlsx",
        default=IN_XLSX_DEFAULT,
        help="Input Excel path (default: data/best_selling_albums.xlsx)",
    )
    ap.add_argument(
        "--out",
        dest="out_csv",
        default=OUT_CSV_DEFAULT,
        help="Output CSV path (default: data/best_selling_albums_enriched.csv)",
    )
    ap.add_argument(
        "--mb-throttle",
        type=float,
        default=1.1,
        help="Seconds sleep between MusicBrainz calls (default: 1.1)",
    )
    args = ap.parse_args()

    raw = load_raw_xlsx(args.in_xlsx)
    clean = build_album_artist_table(raw)

    print(f"Reconstructed {len(clean)} album rows from messy Excel layout.")

    sess = http_session()
    enriched = enrich_with_musicbrainz(sess, clean, throttle=args.mb_throttle)

    out_dir = os.path.dirname(args.out_csv)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    enriched.to_csv(args.out_csv, index=False, encoding="utf-8")
    print(f"Wrote {args.out_csv}")


if __name__ == "__main__":
    main()
