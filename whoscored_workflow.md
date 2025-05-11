# WhoScored Minute‑by‑Minute Scraping – Python Workflow

> **Audience**  Junior data engineer familiar with Python but new to web‑scraping. **Goal**  Build a reusable pipeline that extracts per‑minute possession & player‑rating data (plus full Opta event streams) for any WhoScored match and saves into a SQL ready for analysis.

---

## 0  Project Setup

1. **Create a clean virtual environment** (e.g. `python -m venv ws_env`) and install core packages:
   - `requests`, `beautifulsoup4`, `lxml`, `pandas`, `tenacity`  →  Standard HTTP route.
   - `cloudscraper`  →  Lightweight Cloudflare/Incapsula bypass; try this before launching a real browser.
   - `selenium`, `undetected‑chromedriver`  →  Only if Incapsula blocks direct requests.
   - `soccerdata`  →  Alternative Opta event extractor.
   - `python‑dotenv`  →  Keep configurable items (user‑agents, wait times) out of code.
2. **Repo skeleton**
   ```text
   whoscored_scraper/
   ├── data/               # Raw & processed outputs
   ├── ws/                 # Package code
   │   ├── fetch.py        # HTTP + Selenium helpers
   │   ├── parse.py        # JSON → tidy frames
   │   ├── postprocess.py  # Aggregation, validation
   │   └── cli.py          # Entry‑point script
   └── README.md
   ```

## 1  Identify the Match

- **Input parameters:** season, competition, and either
  - a list of known `matchId` values, or
  - a URL on the form `https://www.whoscored.com/matches/{matchId}/...`.
- Quick sanity‑check: a valid `matchId` is a 6‑ or 7‑digit integer; use regex to extract if only the URL is given.

## 2  Handle Anti‑Bot (Cloudflare / Incapsula) & Re‑Use Session

1. **First attempt with Cloudscraper**
   - Build a `cloudscraper.create_scraper()` session which automatically solves most Cloudflare or Incapsula challenges in pure Python.
   - Send a test `GET https://www.whoscored.com` – if status 200 and HTML contains `match-centre`, keep using this session throughout.
2. **Fallback → undetected‑Chrome + Selenium** when Cloudscraper still returns 403/JS:
   - Launch undetected‑Chrome.
   - Navigate to any WhoScored page; wait until network idle.
   - Export solved cookies (`incap_ses_*`, `visid_incap_*`, `cf_clearance` if present) into a `requests.Session`.
   - Persist cookies to disk (`cookies.pkl`) so subsequent runs can skip Selenium unless expired.

## 3  Data Acquisition Phase

### 3.1  Hitting the Public JSON Endpoints

For each `matchId` build these URLs:

| Endpoint             | Payload                             | Purpose                                  |
| -------------------- | ----------------------------------- | ---------------------------------------- |
| `/MatchEvents`       | Full Opta‑style event list          | minute, second, team, player, eventType… |
| `/teamstatistics`    | Possession/performance graph arrays | per‑minute possession%                   |
| `/playerRatingGraph` | Match rating trajectory             | aggregate rating every ~1–2 min          |

- **Steps**
  1. Loop over endpoint list.
  2. `GET` with the shared session.
  3. On HTTP 429/5xx, back‑off & retry (use **tenacity**).
  4. Save raw JSON to `data/raw/{matchId}_{endpoint}.json` for audit.

### 3.2  Fallback – Parsing `matchCentreData` Embedded Script

Certain older matches don’t expose graph endpoints. In that case:

1. GET the match HTML.
2. Regex search the `<script>` that defines `var matchCentreData = {...};`.
3. Extract the JSON object & write to `data/raw/{matchId}_matchCentreData.json`.

### 3.3  Alternative Package Route (`soccerdata`)

If you only need Opta event streams and don’t want to maintain scraping logic:

1. Initialise `ws = soccerdata.WhoScored(leagues="ENG‑Premier League", seasons=2024)`.
2. Call `events_df = ws.read_events(match_id=matchId, output_fmt="events")`.
3. Proceed directly to section 5 for post‑processing.

## 4  Parsing & Normalising the JSON

Implement parser functions in **parse.py**:

1. **Possession Graph**
   - From `/teamstatistics` or `matchCentreData['teamPerformance']['possessionGraph']`.
   - Output tidy DataFrame with columns: `minute`, `pct_home`, `pct_away`.
2. **Player Rating Graph**
   - From `/playerRatingGraph` or `matchCentreData['playerRatingGraph']`.
   - Output DataFrame: `minute`, `rating` (home team aggregate), | duplicate for away if available.
3. **Event Stream**
   - From `/MatchEvents` JSON or soccerdata.
   - Keep `eventId`, `minute`, `second`, `teamId`, `playerId`, `type_name`.

## 5  Post‑Processing & Quality Control

1. **Minute coverage check**
   - Verify possession graph covers 0→90 (or 100 incl. added‑time).
   - If gaps: forward‑fill the previous minute’s values.
2. **Merge datasets**
   - Left‑join possession & rating on `minute`.
   - Optionally aggregate events per minute (e.g. total shots, fouls) before joining.
3. **Validate totals**
   - For each minute, `pct_home + pct_away` ≈ 100.  Flag anomalies.

## 6  Persist Clean Outputs

- Save each table to `data/processed/{matchId}_possession.parquet` etc.
- Provide a helper `to_dashboard()` that concatenates multiple matches and pushes to a visualisation layer (e.g. Power BI, Looker).

## 7  Automation & CLI

- ``** outline**
  ```bash
  python -m ws.cli --match 1640755 1640756 --output parquet
  ```
  1. Parse CLI args → list of matchIds.
  2. Initialise session (with or without Selenium).
  3. Loop → fetch → parse → postprocess → save.
  4. Log progress to console + rotating file.

## 8  Error Handling & Edge Cases

| Scenario                      | Strategy                                                        |
| ----------------------------- | --------------------------------------------------------------- |
| Endpoint returns empty list   | Retry once, then log & skip match                               |
| Cookie expiry / 403 mid‑run   | Relaunch Selenium to refresh cookies                            |
| Unexpected JSON schema change | Capture raw payload, raise `SchemaError`, push to alert channel |

## 9  Ethical & Legal Notes

- Respect WhoScored’s robots.txt and Terms; scrape **sparingly** (≤ 1 req/sec, random sleeps).
- Data is for internal research; redistribute only per site policy.

## 10  Next Steps & Enhancements

1. Add Redis cache so repeated match requests read from disk/network cache.
2. Integrate with a match‑schedule scraper so the pipeline auto‑discovers new matches daily.
3. Containerise the workflow with Docker for reproducible deployments.
4. Build unit tests (pytest) that use stored fixture JSON to guard against schema drift.

---

> **Milestone definition**> *Milestone 1* – Able to fetch & store raw JSON for one match.> *Milestone 2* – Clean possession + rating CSV for a batch of matches.> *Milestone 3* – Fully automated nightly pipeline with logging & alerts.
