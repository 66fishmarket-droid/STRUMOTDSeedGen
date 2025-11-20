#!/usr/bin/env python3
# scripts/add_added_on_to_calendar_index.py
#
# One-off helper:
# - Ensure calendar_index.csv has an "added_on" column
# - Backfill any missing values with today's date

import os
from datetime import datetime

import pandas as pd

CALENDAR_PATH = "data/calendar_index.csv"

def main():
    if not os.path.exists(CALENDAR_PATH):
        raise SystemExit(f"Missing {CALENDAR_PATH}")

    df = pd.read_csv(CALENDAR_PATH)
    before = len(df)

    today = datetime.now().strftime("%Y-%m-%d")

    if "added_on" not in df.columns:
        df["added_on"] = today
    else:
        df["added_on"] = df["added_on"].fillna("").replace("", today)

    df.to_csv(CALENDAR_PATH, index=False)

    print(f"Updated {CALENDAR_PATH}: {before} rows; ensured added_on (backfilled with {today} where empty).")

if __name__ == "__main__":
    main()
