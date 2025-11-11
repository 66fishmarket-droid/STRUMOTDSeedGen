#!/usr/bin/env python3
# scripts/pull_births_deaths.py

import csv
import sys
import os
import time
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

def get_session():
    contact = os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")
    ua = f"StrumOTD/1.0 (+{contact})"
    s = requests.Session()
    s.headers.update({
        "User-Agent": ua,
        "Accept": "application/json"
    })
    return s

def fetch(session, endpoint, mm, dd, max_retries=5):
    url = f"{WIKI_BASE}/{endpoint}/{int(mm)}/{int(dd)}"
    backoff = 1.5
    for attempt in range(1, max_retries + 1):
        r = session.get(url, timeout=20)
        if r.status_code == 200:
            return r.json()
        # Handle transient and policy responses with backoff
        if r.status_code in (403, 429, 500, 502, 503, 504):
            if attempt == max_retries:
                r.raise_for_status()
            time.sleep(backoff)
            backoff *= 2
            continue
        r.raise_for_status()

def normalize_people(items, mm, dd, kind):
    # kind in {"births","deaths"} -> work_type {"birth","death"}
    out = []
    wt = "birth" if kind == "births" else "death"
    for entry in items:
        year = entry.get("year")
        text = (entry.get("text") or "").strip()
        pages = entry.get("pages") or []
        page_url = ""
        title = ""

        if pages:
            p = pages[0]
            title = (p.get("normalizedtitle") or p.get("displaytitle") or p.get("title") or "").strip()
            page_url = p.get("content_urls", {}).get("desktop", {}).get("page") or ""

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
            "extra": wt,
            "source_url": page_url
        })
    return out

def write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})

def main():
    mm, dd = pick_date_from_args()
    s = get_session()

    births_json = fetch(s, "births", mm, dd)
    # brief pause between calls, play nice
    time.sleep(0.5)
    deaths_json = fetch(s, "deaths", mm, dd)

    births_rows = normalize_people(births_json.get("births", []), mm, dd, "births")
    deaths_rows = normalize_people(deaths_json.get("deaths", []), mm, dd, "deaths")

    write_csv("data/births.csv", births_rows)
    write_csv("data/deaths.csv", deaths_rows)

    print(f"Wrote data/births.csv [{len(births_rows)} rows] and data/deaths.csv [{len(deaths_rows)} rows] for {mm}-{dd}")

if __name__ == "__main__":
    main()
