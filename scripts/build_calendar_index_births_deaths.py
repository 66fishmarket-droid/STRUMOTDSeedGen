#!/usr/bin/env python3
# scripts/build_calendar_index_births_deaths.py
#
# Build / update calendar_index.csv with birth/death "On This Day" factoids.
#
# Input format (births.csv / deaths.csv):
# work_type,title,byline,release_date,month,day,extra,source_url
#
# where:
#   work_type = "birth" or "death"
#   release_date = YYYY-MM-DD (actual date of birth/death)

import os
import re
import csv
import argparse
from datetime import date
from typing import Dict, List, Optional

BIRTHS_IN_DEFAULT = "data/births.csv"
DEATHS_IN_DEFAULT = "data/deaths.csv"
CALENDAR_DEFAULT = "data/calendar_index.csv"

BIRTHS_SOURCE_SYSTEM = "births_wiki"
DEATHS_SOURCE_SYSTEM = "deaths_wiki"

FULL_DATE_RX = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")

CALENDAR_FIELDS = [
    "key_mmdd",
    "year",
    "iso_date",
    "fact_domain",
    "fact_category",
    "fact_tags",
    "title",
    "byline",
    "role",
    "work_type",
    "summary_template",
    "source_url",
    "source_system",
    "source_id",
    "country",
    "language",
    "extra",
    "used_on",
    "use_count",
]

# ---------- Helpers ----------

def parse_full_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    m = FULL_DATE_RX.match(s)
    if not m:
        return None
    y, mm, dd = m.groups()
    try:
        return date(int(y), int(mm), int(dd))
    except ValueError:
        return None

def read_csv_if_exists(path: str) -> List[Dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        peek = f.read(1)
        if not peek:
            return []
        f.seek(0)
        reader = csv.DictReader(f)
        return list(reader)

def write_calendar(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CALENDAR_FIELDS)
        w.writeheader()
        for r in rows:
            out = {}
            for k in CALENDAR_FIELDS:
                v = r.get(k, "")
                if v is None:
                    v = ""
                out[k] = str(v)
            w.writerow(out)

def split_byline(byline: str):
    """
    Split 'American singer-songwriter (1942–2016)' into:
      desc = 'American singer-songwriter'
      paren = '1942–2016'
    or 'British tennis player (born 2002)' into:
      desc = 'British tennis player'
      paren = 'born 2002'
    """
    byline = (byline or "").strip()
    if "(" in byline and ")" in byline:
        before, rest = byline.split("(", 1)
        desc = before.strip().rstrip(",")
        paren = rest.rsplit(")", 1)[0].strip()
        return desc, paren
    return byline, ""

def build_birth_fact(row: Dict) -> Optional[Dict]:
    d = parse_full_date(row.get("release_date") or "")
    if not d:
        return None

    mmdd = d.strftime("%m-%d")
    year = d.year

    title = (row.get("title") or "").strip()
    byline = (row.get("byline") or "").strip()
    source_url = (row.get("source_url") or "").strip()
    work_type = (row.get("work_type") or "birth").strip() or "birth"

    desc, paren = split_byline(byline)

    # Example: "On this day in 2002, Emma Raducanu, British tennis player, was born."
    if desc and desc.lower() not in title.lower():
        template = f"On this day in {year}, {title}, {desc}, was born."
    else:
        template = f"On this day in {year}, {title} was born."

    extra_parts = []
    # original extra field from births.csv
    if (row.get("extra") or "").strip():
        extra_parts.append(row["extra"].strip())
    if paren:
        extra_parts.append(f"paren={paren}")
    extra = ";".join(extra_parts)

    return {
        "key_mmdd": mmdd,
        "year": str(year),
        "iso_date": d.isoformat(),
        "fact_domain": "people",           # distinct from 'arts' works
        "fact_category": "birth",
        "fact_tags": "birth",
        "title": title,
        "byline": byline,
        "role": "",                        # could later parse 'tennis player', 'singer' etc.
        "work_type": work_type,
        "summary_template": template,
        "source_url": source_url,
        "source_system": BIRTHS_SOURCE_SYSTEM,
        "source_id": "",                   # placeholder for Wikidata QID etc.
        "country": "",                     # could be inferred later
        "language": "en",                  # Wikipedia-EN feed assumption
        "extra": extra,
        "used_on": "",
        "use_count": "0",
    }

def build_death_fact(row: Dict) -> Optional[Dict]:
    d = parse_full_date(row.get("release_date") or "")
    if not d:
        return None

    mmdd = d.strftime("%m-%d")
    year = d.year

    title = (row.get("title") or "").strip()
    byline = (row.get("byline") or "").strip()
    source_url = (row.get("source_url") or "").strip()
    work_type = (row.get("work_type") or "death").strip() or "death"

    desc, paren = split_byline(byline)

    # Example: "On this day in 2016, Leon Russell, American singer-songwriter, died."
    if desc and desc.lower() not in title.lower():
        template = f"On this day in {year}, {title}, {desc}, died."
    else:
        template = f"On this day in {year}, {title} died."

    extra_parts = []
    if (row.get("extra") or "").strip():
        extra_parts.append(row["extra"].strip())
    if paren:
        extra_parts.append(f"paren={paren}")
    extra = ";".join(extra_parts)

    return {
        "key_mmdd": mmdd,
        "year": str(year),
        "iso_date": d.isoformat(),
        "fact_domain": "people",
        "fact_category": "death",
        "fact_tags": "death",
        "title": title,
        "byline": byline,
        "role": "",
        "work_type": work_type,
        "summary_template": template,
        "source_url": source_url,
        "source_system": DEATHS_SOURCE_SYSTEM,
        "source_id": "",
        "country": "",
        "language": "en",
        "extra": extra,
        "used_on": "",
        "use_count": "0",
    }

def build_people_facts(birth_rows: List[Dict], death_rows: List[Dict]) -> List[Dict]:
    facts: List[Dict] = []

    for r in birth_rows:
        wt = (r.get("work_type") or "").strip().lower()
        if wt != "birth":
            # Be defensive: only treat proper births
            continue
        fact = build_birth_fact(r)
        if fact:
            facts.append(fact)

    for r in death_rows:
        wt = (r.get("work_type") or "").strip().lower()
        if wt != "death":
            continue
        fact = build_death_fact(r)
        if fact:
            facts.append(fact)

    return facts

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Build calendar_index.csv entries for births and deaths.")
    ap.add_argument("--births", dest="births_path", default=BIRTHS_IN_DEFAULT, help="Births CSV input path")
    ap.add_argument("--deaths", dest="deaths_path", default=DEATHS_IN_DEFAULT, help="Deaths CSV input path")
    ap.add_argument("--calendar", dest="calendar_path", default=CALENDAR_DEFAULT, help="Calendar index CSV path")
    args = ap.parse_args()

    if not os.path.exists(args.births_path):
        raise SystemExit(f"Missing births input file: {args.births_path}")
    if not os.path.exists(args.deaths_path):
        raise SystemExit(f"Missing deaths input file: {args.deaths_path}")

    # Read births / deaths
    with open(args.births_path, "r", encoding="utf-8") as f:
        births_rows = list(csv.DictReader(f))
    with open(args.deaths_path, "r", encoding="utf-8") as f:
        deaths_rows = list(csv.DictReader(f))

    print(f"Loaded {len(births_rows)} birth rows from {args.births_path}")
    print(f"Loaded {len(deaths_rows)} death rows from {args.deaths_path}")

    people_facts = build_people_facts(births_rows, deaths_rows)
    print(f"Built {len(people_facts)} birth/death fact rows for calendar index")

    # Read existing calendar index (if any)
    existing = read_csv_if_exists(args.calendar_path)
    print(f"Loaded {len(existing)} existing calendar rows from {args.calendar_path}")

    # Keep only rows that are NOT from births/deaths source systems
    kept_existing: List[Dict] = []
    for r in existing:
        src = (r.get("source_system") or "").strip()
        if src not in (BIRTHS_SOURCE_SYSTEM, DEATHS_SOURCE_SYSTEM):
            kept_existing.append(r)

    print(f"Kept {len(kept_existing)} existing non-birth/death rows")

    merged = kept_existing + people_facts

    # Sort for readability
    def sort_key(r: Dict):
        return (
            (r.get("key_mmdd") or ""),
            int(r.get("year") or 0),
            (r.get("fact_domain") or ""),
            (r.get("fact_category") or ""),
            (r.get("title") or ""),
        )

    merged.sort(key=sort_key)

    print(f"Writing {len(merged)} rows back to {args.calendar_path}")
    write_calendar(args.calendar_path, merged)
    print("Done.")

if __name__ == "__main__":
    main()
