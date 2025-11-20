#!/usr/bin/env python3
# scripts/add_added_on_to_songs_source.py
#
# One-off helper:
# - Ensure songs_top10_us_with_dates.csv has an "added_on" column
# - Backfill any missing values with today's date

import os
from datetime import datetime
import pandas as pd

SONGS_PATH = "data/songs_top10_us_with_dates.csv"

def main():
    if not os.path.exists(SONGS_PATH):
        raise SystemExit(f"Missing {SONGS_PATH}")

    df = pd.read_csv(SONGS_PATH)
    before = len(df)

    today = datetime.now().strftime("%Y-%m-%d")

    if "added_on" not in df.columns:
        df["added_on"] = today
    else:
        df["added_on"] = df["added_on"].fillna("").replace("", today)

    df.to_csv(SONGS_PATH, index=False)

    print(f"Updated {SONGS_PATH}: {before} rows; ensured added_on column (backfilled with {today}).")

if __name__ == "__main__":
    main()
