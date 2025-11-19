#!/usr/bin/env python3
# scripts/clean_births_deaths_sources.py
#
# One-off cleaner for data/births.csv and data/deaths.csv:
# - Drop non-arts rows (based on title + byline)
# - Ensure an "added_on" column exists and is populated (YYYY-MM-DD)

import os
from datetime import datetime
from typing import List

import pandas as pd

BIRTHS_PATH = "data/births.csv"
DEATHS_PATH = "data/deaths.csv"

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

def is_arts_related(title: str, byline: str) -> bool:
    text = f"{title or ''} {byline or ''}".lower()
    for kw in ART_KEYWORDS:
        if kw in text:
            return True
    return False

def clean_file(path: str) -> None:
    if not os.path.exists(path):
        print(f"Skip {path}: does not exist")
        return

    df = pd.read_csv(path)
    if df.empty:
        print(f"Skip {path}: empty")
        return

    before = len(df)

    # Filter to arts-related only, based on title + byline
    mask = df.apply(
        lambda r: is_arts_related(r.get("title", ""), r.get("byline", "")),
        axis=1,
    )
    df = df.loc[mask].copy()
    after = len(df)

    today = datetime.now().strftime("%Y-%m-%d")

    # Ensure added_on column exists and is populated
    if "added_on" not in df.columns:
        df["added_on"] = today
    else:
        df["added_on"] = df["added_on"].fillna("").replace("", today)

    df.to_csv(path, index=False)
    print(f"Cleaned {path}: {before} -> {after} rows; added_on set where missing to {today}")

def main():
    clean_file(BIRTHS_PATH)
    clean_file(DEATHS_PATH)

if __name__ == "__main__":
    main()
