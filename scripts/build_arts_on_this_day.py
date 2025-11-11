#!/usr/bin/env python3
# scripts/build_arts_on_this_day.py
# Phase 1: Working scaffold + test data structure
# Next steps will replace mock data with Wikidata + OMDb queries.

import csv
import os
from datetime import datetime

FIELDS = [
    "work_type","title","byline","release_date",
    "month","day","extra","source_url"
]

OUT_FILES = {
    "songs": "data/songs_top10_us.csv",
    "albums": "data/albums_us_1m.csv",
    "movies": "data/movies_rt80.csv"
}

def normalize_date(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%m"), dt.strftime("%d")
    except Exception:
        return "",""

def write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    # Temporary mock data (replace with real queries later)
    mock_songs = [
        {
            "work_type": "song",
            "title": "Say Say Say",
            "byline": "Paul McCartney & Michael Jackson",
            "release_date": "1983-11-11",
            "extra": "US Top 10",
            "source_url": "https://en.wikipedia.org/wiki/Say_Say_Say"
        }
    ]

    mock_albums = [
        {
            "work_type": "album",
            "title": "Thriller",
            "byline": "Michael Jackson",
            "release_date": "1982-11-30",
            "extra": "RIAA Diamond (US)",
            "source_url": "https://en.wikipedia.org/wiki/Thriller_(album)"
        }
    ]

    mock_movies = [
        {
            "work_type": "movie",
            "title": "Pride & Prejudice",
            "byline": "Joe Wright (director)",
            "release_date": "2005-11-11",
            "extra": "Rotten Tomatoes 87%",
            "source_url": "https://www.imdb.com/title/tt0414387/"
        }
    ]

    for group, data in [
        ("songs", mock_songs),
        ("albums", mock_albums),
        ("movies", mock_movies)
    ]:
        for r in data:
            mm, dd = normalize_date(r["release_date"])
            r["month"], r["day"] = mm, dd
            # Ensure all FIELDS exist
            for k in FIELDS:
                r.setdefault(k, "")
        write_csv(OUT_FILES[group], data)

    print("Mock arts datasets written (songs, albums, movies).")

if __name__ == "__main__":
    main()

