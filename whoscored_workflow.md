# WhoScored Minute‑by‑Minute Scraping – **Updated Python Workflow**

> **Audience**  Junior data engineer familiar with Python but new to web‑scraping & basic databases.
> **Goal**  Build a reproducible pipeline that (1) discovers fixtures for any league, (2) scrapes minute‑level match data, and (3) persists both metadata & granular stats to an SQLite data‑mart ready for analysis or export to BI tools.

---

## 0  Project Setup

1. **Create a fresh virtual environment** (e.g. `python -m venv ws_env`) and install core packages:

   * Web requests & parsing — `requests`, `beautifulsoup4`, `lxml`, `cloudscraper`
   * Browser automation — `selenium`, `undetected‑chromedriver`
   * Data wrangling — `pandas`, `pyarrow`, `tenacity`
   * Database layer — `SQLAlchemy`, `sqlite-utils` (optional CLI helpers)
   * Config & secrets — `python‑dotenv`
2. **Repo skeleton**

   ```text
   whoscored_scraper/
   ├── data/                # Raw JSON, interim parquet
   ├── ws/                  # Package code
   │   ├── db.py            # SQLite models & helpers
   │   ├── fixtures.py      # League‑level fixture scraper (uses Selenium)
   │   ├── match.py         # Match‑level scrapers (JSON or HTML route)
   │   ├── parse.py         # JSON → tidy DataFrames
   │   └── cli.py           # Entry‑point & orchestration
   └── README.md
   ```

---

## 1  Discover Fixtures (**new module**)

### 1.1  Purpose

Automate collection of **match / fixture IDs** for a selected league & date window (default ± 3 months either side of today).

### 1.2  Implementation highlights (`ws/fixtures.py`)

1. Accept a **league overview URL** (e.g. Premier League 2024‑25 fixtures page).
2. Spin up **undetected‑Chrome** ➝ wait for calendar component.
3. Iterate **Prev / Next** month buttons up to *N months*; collect all `<a href="…/matches/{id}/…">` links.
4. De‑duplicate IDs, return `List[int]`.
5. Optionally write to `data/raw/{league}_{yyyymmdd}.fixtures.json`.

> A working prototype lives in `fetch_epl_fixtures.py`; refactor its logic into a reusable `get_fixture_ids(url:str, months:int)->List[int]`.

---

## 2  Database Layer (**new module**)

### 2.1  Why SQLite?

* Zero‑config, cross‑platform, perfect for < GB‑scale analytics.
* Schema versioned in‑repo; easily promoted to Postgres later.

### 2.2  Schema (ER‑style overview)

```
competitions(id PK, name, country, season, stage, scraped_at)
fixtures(id PK, competition_id FK, date_utc, home_team, away_team,
         round, referee, venue, scraped_at)
minutes(match_id FK, minute PK, added_time, possession_home, possession_away,
        rating_home, rating_away, total_shots_home, total_shots_away, pass_success_home, pass_success_away, dribbles_home, dribbles_away, aerial_won_home, aerial_won_away, tackles_home, tackles_away, corners_home, corners_away, scraped_at)
```

*All tables share `scraped_at` for lineage.*

### 2.3  Helper API (`ws/db.py`)

* `get_engine(db_path="data/ws.db")` → returns SQLAlchemy engine.
* `upsert(table:str, df:pd.DataFrame, pk:list[str])` → generic UPSERT (handles duplicates).
* `fixture_exists(match_id:int)` ➝ bool (skip re‑scrape).

---

## 3  Identify Target Matches

1. **Mode A – From fixture scraper**

   ```python
   ids = fixtures.get_fixture_ids(league_url, months=6)
   ```
2. **Mode B – Ad‑hoc list** passed at CLI (`--match 1825717 1825720`).
3. Filter out IDs already present in `fixtures` table unless `--force` flag is set.

---

## 4  Handle Anti‑Bot & Session Re‑use

*Unchanged from earlier doc – Cloudscraper first, Selenium fallback, cookie reuse.*

---

## 5  Match‑Level Data Acquisition

### 5.1  Public JSON endpoints

Same `/MatchEvents`, `/teamstatistics`, `/playerRatingGraph` flow.  Raw JSON persisted to `data/raw/`.

### 5.2  Fallback – `matchCentreData` script scraping

Use `ws/match.py:fetch_match_centre_data(match_id)` (ported from `proto.py`).

---

## 6  Parsing & Normalisation (`ws/parse.py`)

Functions convert raw payloads to tidy DataFrames ready for DB insert.  *Reuse logic from the original workflow.*

---

## 7  Persistence to SQLite (**updated section**)

```python
engine = db.get_engine()

# ⬇ write granular minute‑graph
minutes_df.to_sql("minutes", engine, if_exists="append", index=False)

# ⬇ write one‑row aggregate per match/team
agg_df.to_sql("agg_stats", engine, if_exists="append", index=False)
```

* Use `upsert()` to prevent duplicates when re‑running.
* Wrap all inserts in a single transaction for speed.

---

## 8  Post‑Processing & Quality Control

1. **DB‑centric checks** – e.g. `SELECT COUNT(*)«` minutes covering 0‑90.
2. Flag possession sums ≠ 100.
3. Create **materialised views** for common queries (e.g. per‑minute xG once added).

---

## 9  CLI & Automation

```bash
python -m ws.cli scrape \
  --league "https://www.whoscored.com/regions/252/.../fixtures" \
  --months 6 \
  --db data/ws.db \
  --output parquet
```

**Command groups**

| Command      | Action                                            |
| ------------ | ------------------------------------------------- |
| `scrape`     | Discover fixtures → scrape → load DB              |
| `refresh`    | Rescrape last *N* days (to catch postponed games) |
| `export csv` | Dump tables/queries for BI tools                  |

Set up a **cron / Task Scheduler** job nightly; pipe logs to a file.

---

## 10  Error Handling & Edge Cases

| Scenario                       | Strategy                      |
| ------------------------------ | ----------------------------- |
| Fixture calendar fails to load | Retry with full‑browser UA ✔  |
| DB lock (another job running)  | Back‑off & retry after 60 s   |
| Upsert violates schema change  | Migrate DB via Alembic script |

---

## 11  Ethical & Legal Notes

Same as before **plus**: storing data locally does not grant redistribution rights; check WhoScored T\&C before sharing DB dumps.

---

## 12  Next Steps

1. **Incremental scrape** – use LAST MODIFIED header / match kickoff time to only hit fresh games.
2. **Opta event enrichment** – calculate in‑play KPIs (xThreat, PPDA) directly in DB.
3. **Switch DB** – lift‑and‑shift from SQLite to Postgres once data >2 GB.
4. Containerise with Docker Compose: Scraper ‑> DB ‑> Metabase dashboard.

---

> **Milestones**
> *M1* – Fixture scraper returns clean ID list ✓
> *M2* – Match pipeline fills SQLite for one season ✓
> *M3* – Automated nightly job + quality alerts via Slack 🏁
