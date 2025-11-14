# STRUM OTD / Arts / Song Dates  
## Automated Data Pipeline Overview

This repository maintains several independent data-generation pipelines used to build:

- Births & deaths datasets  
- Arts datasets (music, film, books, events)  
- Calendar index  
- Song release dates for chart tracks  

These pipelines run on a nightly schedule and update the CSV files in the `data/` directory.

Below is a breakdown of each GitHub Actions workflow and what it controls.

---

# 1. build-births-deaths.yml  
**Purpose:** Generate and update two CSV datasets:

- `data/births.csv`  
- `data/deaths.csv`  

**Sources:** Wikipedia REST API (OnThisDay feed)

**Runs:**  
- Automatically every night at 03:15 UTC  
- Manually on demand (optionally specifying month/day)

**Primary script:**  
```
scripts/pull_births_deaths.py
```

**Output:**  
Daily refreshed OTD births and deaths for all historical years.

---

# 2. build_song_dates.yml  
**Purpose:** Fill or improve missing song release dates for Top 10 US chart entries.

This workflow performs a delta update:  
Only rows missing release dates (or containing only a year) are processed.

**Sources:**  
- Wikipedia Search API  
- Wikipedia article wikitext  
- Wikidata P577 release date

**Runs:**  
- Nightly at 01:15 UTC  
- On demand

**Primary script:**  
```
scripts/fetch_song_release_dates.py
```

**Output:**  
`data/songs_top10_us_with_dates.csv`

This includes newly discovered release dates, parsed day/month fields, and the source of the information.

---

# 3. build_arts_calendar.yml  
**Purpose:** Build the daily arts dataset and update the global calendar index.

This workflow handles:

- Music / film / book / theatre “On This Day” events  
- Integration with OMDB for media metadata  
- Generation of the calendar-wide index for quick lookup

**Runs:**  
- Nightly at 03:15 UTC  
- Manually on demand

**Primary scripts:**  
```
scripts/build_arts_on_this_day.py
scripts/build_calendar_index.py
```

**Output:**  
- `data/arts/*.csv`  
- `data/calendar_index.csv`  

This is the canonical builder for all non-birth/death OTD content.

---

# Architecture Summary

The automated system is now organized into **three isolated pipelines**, each responsible for a specific dataset:

```
+-------------------------+
|  build-births-deaths    |
|-------------------------|
|  births.csv             |
|  deaths.csv             |
+-------------------------+

+-------------------------+
|  build_song_dates       |
|-------------------------|
|  songs_top10_us_with_   |
|  dates.csv              |
+-------------------------+

+-------------------------------+
|  build_arts_calendar          |
|-------------------------------|
|  arts/*.csv                   |
|  calendar_index.csv           |
+-------------------------------+
```

Each workflow:

1. Runs independently  
2. Writes only to its own part of `data/`  
3. Commits changes only when needed  
4. Avoids collisions by using rebase + conflict-safe commits  

---

# Removed / Deprecated Workflows (Cleanup)

As of the cleanup, the following legacy workflows were deleted:

- `backfill-month.yml` (deprecated JSON builder)  
- `otd.yml` (Node-based OTD JSON builder)  
- `full.yml` (superseded by the new arts+calendar workflow)  

These older workflows generated JSON artefacts no longer used and duplicated work done by the Python-based system.

---

# Summary

The repository now operates with a clean, efficient, and maintainable automation structure.  
Every dataset is:

- Built by the correct workflow  
- Kept up to date nightly  
- Free from duplication  
- Clearly separated in purpose  

If you make changes to any of the scripts under `scripts/`, the corresponding workflow will pick them up automatically on the next scheduled run.

