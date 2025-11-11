#!/usr/bin/env python3
# scripts/pull_births_deaths.py

import csv
import sys
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

WIKI_BASE = "https://en.wikipedia.org/api/rest_v1/feed/onthisday"

# Normalized CSV header
FIELDS = [
    "work_type", "title", "byline", "release_date",
    "month", "day", "extra", "source_url"
]

def pick_date_from_args():
    # Usage: python scripts/pull_births_deaths.py [MM DD]
    tz = ZoneInfo("Europe/London")
    today = datetime.now(tz)
    if len(sys.argv) == 3:
        mm = sys.argv[1].zfill(2)
        dd = sys.argv[2].zfill(2)
    else:
        mm = today.strftime("%m")
        dd = today.strftime("%d")
    return mm, dd

def fetch(endpoint, mm, dd):
    url = f"{WIKI_BASE}/{endpoint}/{int(mm)}/{int(dd)}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def normalize_people(items, mm, dd, kind):
    # kind in {"births","deaths"} -> work_type {"birth","death"}
    out = []
    wt = "birth" if kind == "births" else "death"
    for entry in items:
        # Each entry has "text", "year", and "pages" (array with page metadata)
        year = entry.get("year")
        text = entry.get("text", "").strip()
        pages = entry.get("pages") or []
        # Pick first page as canonical
        page_url = ""
        title = ""
        if pages:
            p = pages[0]
            title = (p.get("normalizedtitle") or p.get("displaytitle") or p.get("title") or "").strip()
            page_url = p.get("content_urls", {}).get("desktop", {}).get("page") or ""
        # Build release_date as YYYY-MM-DD using the provided year
        # If year is missing, skip (rare for OTD birth/death entries)
        if not year:
            continue
        release_date = f"{int(year):04d}-{mm}-{dd}"
        out.append({
            "work_type": wt,
            "title": title or text,
            "byline": "",
            "release_date": release_date,
            "month": mm,
            "day": dd,
            "extra": wt,  # just "birth" or "death"
            "source_url": page_url
        })
    return out

def write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            # keep only allowed keys
            w.writerow({k: r.get(k, "") for k in FIELDS})

def main():
    mm, dd = pick_date_from_args()

    births_json = fetch("births", mm, dd)
    deaths_json = fetch("deaths", mm, dd)

    births_rows = normalize_people(births_json.get("births", []), mm, dd, "births")
    deaths_rows = normalize_people(deaths_json.get("deaths", []), mm, dd, "deaths")

    write_csv("data/births.csv", births_rows)
    write_csv("data/deaths.csv", deaths_rows)

    print(f"Wrote data/births.csv [{len(births_rows)} rows] and data/deaths.csv [{len(deaths_rows)} rows] for {mm}-{dd}")

if __name__ == "__main__":
    main()

