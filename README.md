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
# STRUM OTD Arts Canon Datasets  
## Overview of Current CSV Files and Planned Expansion

The `otd/` directory contains seed datasets representing culturally significant works across multiple art forms. These datasets act as *canonical pools* from which daily “On This Day” entries can be drawn and enriched.

This document outlines what each CSV represents, the criteria used to populate it, and how each data source will be expanded over time with richer metadata (IMDb, sales, awards, certifications, etc).

---

# albums_us_1m.csv  
### Purpose  
Albums that have sold **over 1 million copies in the United States**, forming a list of high-impact releases.

### Current Criteria  
- RIAA-certified 1x Platinum or higher.

### Planned Enhancements  
- Add global sales (IFPI)  
- Add chart performance (Billboard)  
- Add critical score (Metacritic)  
- Add award flags (Grammy wins, nominations)  
- Add MusicBrainz + Discogs IDs  
- Add canonical release date with ISO precision  
- Add anniversary flags (10y, 20y, 25y, 30y, 50y)

---

# births.csv / deaths.csv  
(Generated nightly; part of the OTD pipeline)

No changes required here.

---

# calendar_index.csv  
(Generated nightly; merged index of all daily events)

This will eventually integrate enriched metadata from all arts canon files.

---

# literature_canon.csv  
### Purpose  
Key works of literature used for OTD content.

### Current Criteria (seed)  
- Major award winners (Pulitzer, Booker, Nobel Literature)  
- Culturally significant novels and non-fiction  
- Public domain classics

### Planned Enhancements  
- Goodreads rating + rating count  
- Awards field (Booker/Pulitzer/etc)  
- Language, genre, page count  
- Author nationality  
- Adaptations (film, TV)  
- Anniversary flags  

---

# movies_rt80.csv  
### Purpose  
Films with **Rotten Tomatoes score >= 80%**, used as a high-quality film canon.

### Current Criteria  
- RT >= 80%  
- IMDb score optionally included

### Planned Enhancements  
- Add IMDb rating + vote count  
- Add OMDb metadata (runtime, genre, cast, plot)  
- Add box office data (BoxOfficeMojo)  
- Add Oscars/BAFTA wins  
- Add production year and exact release ISO  
- Add TMDb ID for richer artwork/posters  
- Separate films vs documentaries vs animation  

---

# paintings_canon.csv  
### Purpose  
Major artworks spanning Renaissance → modern era.

### Current Criteria  
- Museum significance (MoMA, Tate, Louvre, Rijksmuseum)  
- Known auction record holders  
- Works with established historical importance

### Planned Enhancements  
- Artist biography links  
- Art movement/style  
- Museum of origin  
- Auction high sale price  
- Creation year ISO normalization  
- Exhibition/retrospective anniversaries  

---

# sculpture_canon.csv  
### Purpose  
Sculptures of high historical or cultural importance.

### Current Criteria  
- Major works recognized by authoritative art history sources

### Planned Enhancements  
- Museum metadata  
- Material (bronze, marble, forged steel, etc)  
- Dimensions  
- Restoration/rediscovery events  
- Anniversary flags  

---

# songs_top10_us.csv / songs_top10_us_with_dates.csv  
### Purpose  
Top 10 US chart entries matched with release date metadata.

### Current Criteria  
- Billboard Hot 100 Top 10 entries  
- Release date extracted from Wikipedia/Wikidata

### Planned Enhancements  
- Add audio features (Spotify API)  
- Add certifications (RIAA)  
- Add global chart peak positions  
- Add genre and producer metadata  
- Add anniversary flags  

---

# state_songs.json  
### Purpose  
Official state songs of U.S. states.

### Planned Enhancements  
- Release dates  
- Songwriters/composers  
- Replace JSON with CSV for consistency  
- Add cross-links to songs_top10_us_with_dates.csv (if overlapping)

---

# theatre_canon.csv  
### Purpose  
Canonical list of major theatre works and musicals.

### Current Criteria  
- Broadway/West End classics  
- Tony Award winners  
- Olivier Award winners  
- Shakespeare canonical works

### Planned Enhancements  
- Opening dates (ISO)  
- Number of performances / run length  
- Revival data  
- Major cast members  
- Award metadata  
- Adaptations (film/musical versions)

---

# tv_canon.csv  
### Purpose  
Significant TV shows and episodes.

### Current Criteria  
- Highly rated shows  
- Culturally important episodes  
- Critically acclaimed series

### Planned Enhancements  
- IMDb rating and vote count  
- Rotten Tomatoes season scores  
- Awards (Emmys, Golden Globes)  
- Premiere/season finale dates  
- Episode-level metadata for major milestones  
- Viewer numbers / Nielsen ratings  

---

# Planned General Enhancements (All Canon Files)

All arts canon datasets will eventually support the following metadata fields:

- `title`  
- `creator/artist/author`  
- `release_date_iso`  
- `genre / style / medium`  
- `country`  
- `awards`  
- `rating` (IMDb, Goodreads, Metacritic, RT etc)  
- `popularity_metric` (sales, views, box office, certifications)  
- `external_ids` (Wikidata QID, MusicBrainz, Discogs, IMDb, TMDb, Goodreads)  
- `anniversary_flag` (is_today_10th / 20th / 25th / 30th etc)  
- `otd_relevance_score` (calculated signal strength for daily posting)

---

# End Goal

The goal is to turn the `otd/` directory into a set of **high-quality, curated cultural datasets**, enabling:

- daily automated Strum arts posts  
- trivia questions  
- timeline visualisations  
- anniversary highlights  
- cross-referenced cultural insights  

All fed by a single unified pipeline with nightly updates.


