#!/usr/bin/env python3
# scripts/build_arts_on_this_day.py
# Phase 2: real dataset for songs_top10_us via Wikidata
# Albums (RIAA) and Movies (OMDb) will be added next.

import csv
import os
import time
from typing import List, Dict
from SPARQLWrapper import SPARQLWrapper, JSON

FIELDS = [
    "work_type","title","byline","release_date",
    "month","day","extra","source_url"
]

OUT_SONGS = "data/songs_top10_us.csv"
OUT_ALBUMS = "data/albums_us_1m.csv"   # placeholder for now
OUT_MOVIES = "data/movies_rt80.csv"    # placeholder for now

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

def ua_contact():
    # Provided by workflow env; safe default is your repo issues page
    return os.getenv("USER_AGENT_CONTACT", "https://github.com/OWNER/REPO/issues")

def make_sparql():
    s = SPARQLWrapper(WIKIDATA_ENDPOINT, agent=f"StrumOTD/1.0 (+{ua_contact()})")
    # Extra header for good citizenship
    try:
        s.addCustomHttpHeader("User-Agent", f"StrumOTD/1.0 (+{ua_contact()})")
    except Exception:
        pass
    s.setReturnFormat(JSON)
    return s

def run_paged(query_tmpl: str, page_size: int = 5000, sleep_s: float = 0.8) -> List[Dict]:
    s = make_sparql()
    results = []
    offset = 0
    while True:
        q = query_tmpl.format(limit=page_size, offset=offset)
        s.setQuery(q)
        data = s.query().convert()
        rows = data.get("results", {}).get("bindings", [])
        if not rows:
            break
        results.extend(rows)
        offset += page_size
        time.sleep(sleep_s)  # be nice to the endpoint
    return results

def norm_date(d: str) -> str:
    # Expect xsd:dateTime or xsd:date, use first 10 chars if present
    if not d:
        return ""
    return d[:10] if len(d) >= 10 else d

def month_day(date_str: str):
    if len(date_str) >= 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str[5:7], date_str[8:10]
    return "", ""

def write_csv(path: str, rows: List[Dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})

def build_songs_top10() -> List[Dict]:
    # SPARQL:
    # - song (Q7366)
    # - charted in (P2291) Billboard Hot 100 (Q180072) with ranking (P1352) <= 10
    # - optional release date (P577)
    # - optional performers (P175), aggregated
    query = """
    SELECT ?song ?songLabel ?date (GROUP_CONCAT(DISTINCT ?artistLabel; separator=", ") AS ?artists) (MIN(?rank) AS ?bestRank)
    WHERE {
      ?song wdt:P31 wd:Q7366 .
      ?song p:P2291 ?st .
      ?st ps:P2291 wd:Q180072 .
      ?st pq:P1352 ?rank .
      FILTER(?rank <= 10)

      OPTIONAL { ?song wdt:P577 ?date . }
      OPTIONAL { ?song wdt:P175 ?artist . }

      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    GROUP BY ?song ?songLabel ?date
    ORDER BY ?song
    LIMIT {limit} OFFSET {offset}
    """

    raw = run_paged(query, page_size=2000, sleep_s=0.7)

    # Deduplicate by QID; prefer an earliest date if duplicates appear
    by_qid = {}
    for b in raw:
        uri = b.get("song", {}).get("value", "")
        qid = uri.rsplit("/", 1)[-1] if uri else ""
        title = b.get("songLabel", {}).get("value", "")
        artists = b.get("artists", {}).get("value", "")
        date = norm_date(b.get("date", {}).get("value", ""))
        best_rank = b.get("bestRank", {}).get("value", "")
        # keep earliest date if multiple
        if qid in by_qid:
            old_date = by_qid[qid].get("release_date", "")
            if old_date and date and date < old_date:
                by_qid[qid]["release_date"] = date
                mm, dd = month_day(date)
                by_qid[qid]["month"], by_qid[qid]["day"] = mm, dd
        else:
            mm, dd = month_day(date)
            by_qid[qid] = {
                "work_type": "song",
                "title": title,
                "byline": artists,
                "release_date": date,
                "month": mm,
                "day": dd,
                "extra": f"US Top 10{(' (peak ' + best_rank + ')') if best_rank else ''}",
                "source_url": f"https://www.wikidata.org/wiki/{qid}" if qid else ""
            }

    rows = list(by_qid.values())
    # Sort for stable diffs: by title
    rows.sort(key=lambda r: (r["title"].lower(), r["release_date"]))
    return rows

def main():
    # Build real songs Top 10 dataset
    songs = build_songs_top10()
    write_csv(OUT_SONGS, songs)

    # Keep placeholders for the other outputs so downstream steps stay green
    empty = []
    write_csv(OUT_ALBUMS, empty)
    write_csv(OUT_MOVIES, empty)

    print(f"Wrote {OUT_SONGS} [{len(songs)} rows]")
    print(f"Initialized placeholders: {OUT_ALBUMS}, {OUT_MOVIES}")

if __name__ == "__main__":
    main()
