#!/usr/bin/env python3
# scripts/clean_calendar_index.py
#
# One-off (or occasional) cleaner for data/calendar_index.csv:
# - Drop birth/death rows that are not arts-related
# - Ensure a date_record_added column exists and is populated

import os
from datetime import datetime
from typing import List

import pandas as pd

# Adjust this if your path differs
CALENDAR_PATH = "data/calendar_index.csv"

ART_KEYWORDS: List[str] = [
    # Music
    "singer",
    "musician",
    "composer",
    "songwriter",
    "lyricist",
    "rapper",
    "dj",
    "disc jockey",
    "pianist",
    "guitarist",
    "bassist",
    "drummer",
    "violinist",
    "cellist",
    "conductor",
    "trumpeter",
    "saxophonist",
    "oboist",
    "clarinetist",
    "organist",
    "harpist",
    "vocalist",
    "opera singer",
    "soprano",
    "tenor",
    "baritone",
    "mezzo-soprano",

    # Acting / film / theatre / comedy
    "actor",
    "actress",
    "film director",
    "movie director",
    "television director",
    "screenwriter",
    "playwright",
    "stage director",
    "theatre director",
    "comedian",
    "comic",
    "stand-up comedian",

    # Visual arts / design / photography
    "artist",
    "painter",
    "sculptor",
    "illustrator",
    "cartoonist",
    "photographer",
    "graphic designer",
    "fashion designer",

    # Writing / literature / poetry
    "author",
    "writer",
    "novelist",
    "poet",
    "dramatist",
    "essayist",
]


def is_birth_or_death(work_type: str) -> bool:
    return str(work_type).lower() in ("birth", "death")


def is_arts_related(title: str, byline: str) -> bool:
    text = f"{title or ''} {byline or ''}".lower()
    for kw in ART_KEYWORDS:
        if kw in text:
            return True
    return False


def main():
    if not os.path.exists(CALENDAR_PATH):
        raise SystemExit(f"Calendar index not found at {CALENDAR_PATH}")

    df = pd.read_csv(CALENDAR_PATH)

    before = len(df)

    # 1) Filter: for births/deaths, keep only arts-related
    mask_birth_death = df["work_type"].astype(str).str.lower().isin(["birth", "death"])

    # For birth/death rows, apply arts filter; for others, keep as-is
    arts_mask = df.loc[mask_birth_death].apply(
        lambda r: is_arts_related(r.get("title", ""), r.get("byline", "")),
        axis=1,
    )

    # Start with everything
    keep_mask = ~mask_birth_death
    # Then allow only arts-y births/deaths back in
    keep_mask.loc[mask_birth_death] = arts_mask

    df = df.loc[keep_mask].copy()
    after = len(df)

    # 2) Ensure date_record_added column exists and is populated
    today = datetime.now().strftime("%Y-%m-%d")
    col_name = "date_record_added"  # name is Make-friendly; rename if you really want spaces

    if col_name not in df.columns:
        # For this one-off backfill, all surviving rows get "today"
        df[col_name] = today
    else:
        # Do not overwrite existing values; only fill empties
        df[col_name] = df[col_name].fillna(today)

    # 3) Write back
    df.to_csv(CALENDAR_PATH, index=False)

    print(f"Cleaned calendar_index: {before} -> {after} rows")
    print(f"date_record_added column ensured; backfilled with {today} where empty.")


if __name__ == "__main__":
    main()
