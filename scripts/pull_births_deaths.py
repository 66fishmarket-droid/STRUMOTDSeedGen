#!/usr/bin/env python3
# scripts/pull_births_deaths.py
# Fetch Wikipedia OnThisDay births/deaths for a single day OR a rolling window.

import os
import sys
import csv
import time
import json
import argparse
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

OUT_BIRTHS = "data/births.csv"
OUT_DEATHS = "data/deaths.csv"

FIELDS = ["work_type","title","byline","release_date","month","day","extra","source_url"]

API_TPL = "https://en.wikipedia.org/api/rest_v1/feed/onthisday/{kind}/{mm}/{dd}"

def ua_contact() -> str:
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")

def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": f"StrumOTD/1.0 (+{ua_contact()})",
        "Accept": "application/json",
    })
    return s

def backoff_get(s: requests.Session, url: str, max_retries: int = 6, base_sleep: float = 1.0):
    sleep = base_sleep
    for attempt in range(1, max_retries + 1):
        r = s.get(url, timeout=30)
        if r.status_code == 200:
            return r
        if r.status_code in (403, 429, 500, 502, 503, 504):
            if attempt == max_retries:
                r.raise_for_status()
            time.sleep(sleep)
            sleep *= 1.7
            continue
        r.raise_for_status()
    return None

def fetch_day(kind: str, mm: int, dd: int, s: requests.Session) -> Dict:
    url = API_TPL.format(kind=kind, mm=f"{mm:02d}", dd=f"{dd:02d}")
    r = backoff_get(s, url)
    return r.json()

def norm_text(x) -> str:
    t = str(x or "").strip()
    # collapse whitespace
    return " ".join(t.split())

def rows_from_payload(kind: str, payload: Dict, mm: int, dd: int) -> List[Dict]:
    """
    Convert REST payload to our schema.
    We store:
      work_type: "birth" or "death"
      title: person name
      byline: short description if present (else empty)
      release_date: YYYY-MM-DD (the actual birth/death date using the 'year' field + mm/dd)
      month/day: extracted from release_date
      extra: empty (room for future occupation/links)
      source_url: canonical wiki URL if present
    """
    out: List[Dict] = []
    key = f"{kind}s"  # births / deaths
    items = payload.get(key, [])
    for it in items:
        year = it.get("year")
        pages = it.get("pages") or []
        # pick the first page as canonical
        page = pages[0] if pages else {}
        title = page.get("titles", {}).get("normalized") or page.get("title") or it.get("text") or ""
        desc = page.get("description") or ""
        href = page.get("content_urls", {}).get("desktop", {}).get("page") or page.get("extract_html") or ""
        title = norm_text(title)
        desc = norm_text(desc)
        # build YYYY-MM-DD using the event year + supplied mm/dd
        try:
            dt = datetime(year=int(year), month=mm, day=dd)
            date_str = dt.strftime("%Y-%m-%d")
        except Exception:
            # if year missing/invalid, fallback to --01-01 just to not lose row
            date_str = f"{year or '0000'}-{mm:02d}-{dd:02d}"
        out.append({
            "work_type": "birth" if kind == "birth" else "death",
            "title": title,
            "byline": desc,
            "release_date": date_str,
            "month": f"{mm:02d}",
            "day": f"{dd:02d}",
            "extra": "",
            "source_url": href
        })
    return out

def write_csv(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})

def rolling_dates(start_dt: datetime, days: int) -> List[Tuple[int,int]]:
    v = []
    for i in range(days):
        d = start_dt + timedelta(days=i)
        v.append((d.month, d.day))
    return v

def dedupe(rows: List[Dict]) -> List[Dict]:
    # simple key: (work_type, title, release_date)
    seen = set()
    out = []
    for r in rows:
        key = (r["work_type"], r["title"].lower(), r["release_date"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def main():
    parser = argparse.ArgumentParser(description="Fetch Wikipedia births/deaths for a day or rolling window.")
    parser.add_argument("mm", nargs="?", help="Month (MM) for single-day mode")
    parser.add_argument("dd", nargs="?", help="Day (DD) for single-day mode")
    parser.add_argument("--start-date", help="YYYY-MM-DD start for rolling mode (defaults to today in Europe/London)")
    parser.add_argument("--days", type=int, default=int(os.getenv("BIRTHS_DEATHS_DAYS", "32")),
                        help="Number of days forward to fetch in rolling mode (default 32)")
    args = parser.parse_args()

    s = session()

    births_all: List[Dict] = []
    deaths_all: List[Dict] = []

    if args.mm and args.dd:
        # single-day mode
        mm = int(args.mm)
        dd = int(args.dd)
        for kind in ("birth", "death"):
            payload = fetch_day(kind, mm, dd, s)
            rows = rows_from_payload(kind, payload, mm, dd)
            if kind == "birth":
                births_all.extend(rows)
            else:
                deaths_all.extend(rows)
    else:
        # rolling mode
        if args.start_date:
            start = datetime.strptime(args.start_date, "%Y-%m-%d")
        else:
            # today in Europe/London
            start = datetime.now(ZoneInfo("Europe/London")).replace(hour=0, minute=0, second=0, microsecond=0)
        dates = rolling_dates(start, args.days)
        for (mm, dd) in dates:
            for kind in ("birth", "death"):
                try:
                    payload = fetch_day(kind, mm, dd, s)
                except Exception as e:
                    print(f"Warn: {kind} {mm:02d}-{dd:02d} fetch error: {e}")
                    continue
                rows = rows_from_payload(kind, payload, mm, dd)
                if kind == "birth":
                    births_all.extend(rows)
                else:
                    deaths_all.extend(rows)
                time.sleep(0.25)  # be polite

    births_all = dedupe(births_all)
    deaths_all = dedupe(deaths_all)

    write_csv(OUT_BIRTHS, births_all)
    write_csv(OUT_DEATHS, deaths_all)

    print(f"Wrote {OUT_BIRTHS} rows={len(births_all)}")
    print(f"Wrote {OUT_DEATHS} rows={len(deaths_all)}")

if __name__ == "__main__":
    main()
