# Site Unseen — Intelligence Layer
## Instructions for Claude Code

**Repo:** https://github.com/itogeo/site-unseen
**Context:** The base scoring pipeline (steps 00-05) is already built and running.
This session adds the proactive ownership watchdog layer.

---

## What's Already Built (don't touch)
- `pipeline/00_download_data.py` through `pipeline/05_export_geojson.py`
- `data/processed/tribal_lands.gpkg` — tribal boundaries, scored
- `output/tribal_datacenter_risk.geojson` — scored risk map
- `output/stats.json`
- `webapp/` — Mapbox GL JS frontend
- `run_pipeline.sh`

## What We're Building Now
Three intelligence files that run as a separate layer ON TOP of the existing pipeline.
Focus: **proactive ownership tracking** — catch hyperscalers before they announce.

**Rules:**
- All free APIs only (FERC, SEC EDGAR, OpenCorporates free tier, ArcGIS Hub)
- Under 1000 lines per file
- SQLite for state persistence between runs (so we never re-fetch resolved LLCs)
- No live alerts — write to JSON/GeoJSON output files only
- No satellite change detection
- Load tribal boundaries ONCE at module startup, keep in memory
- Python only

---

## File 1: `intelligence/subsidiaries.py`

Static lookup table — no API calls, no external imports beyond json + pathlib.

Define `HYPERSCALER_MAP` dict. Keys: Microsoft, Amazon, Alphabet, Meta, Apple,
OpenAI_Stargate, Oracle. Each entry:
```python
{
    "subsidiaries": [...],   # official SEC-registered subsidiaries
    "land_llcs": [...],      # known purpose-built acquisition vehicles
    "naming_patterns": [...], # strings to flag in buyer names
    "registered_agent_patterns": [...],
    "known_dc_states": [...]
}
```

Critical entries to include:
- Amazon → land_llcs: ["Vadata Inc", "Vadata LLC", "Cumulus Data LLC",
  "Pearl Street Capital LLC", "Innovation Park Holdings LLC"]
- OpenAI_Stargate → land_llcs: ["Stargate LLC", "SGP Operator LLC",
  "Stargate Abilene LLC", "Stargate Milam County LLC"]
- Meta → land_llcs: ["Graceland Acquisitions LLC", "Papyrus Acquisitions LLC",
  "Cold Spring Land LLC"]
- Alphabet → land_llcs: ["Quantum Valley LLC", "Charleston East LLC",
  "RREEF America LLC"]

Functions:
- `build_flat_lookup() -> dict` — flattens all names to {NAME_UPPER: parent_company}
- `save(output_dir="output")` — writes known_subsidiaries.json and
  known_subsidiaries_flat.json
- `load_flat(output_dir="output") -> dict` — loads flat lookup, returns {} if missing

`if __name__ == "__main__"`: call save(), print summary counts per company.

Target: ~250 lines.

---

## File 2: `intelligence/ferc_monitor.py`

Scrapes public FERC/RTO interconnection queue data for large-load requests
near tribal lands. These are filed 2-3 years before construction — earliest
possible signal.

**Imports:** requests, pandas, geopandas, json, time, pathlib, datetime

**Key function: `load_tribal_buffer(buffer_km=100) -> gpd.GeoDataFrame`**
- Loads `data/processed/tribal_lands.gpkg`
- Reprojects to EPSG:5070, buffers by buffer_km * 1000
- Returns buffered GDF in EPSG:4326
- Call ONCE at module level: `TRIBAL_BUFFER = load_tribal_buffer()`

**`fetch_pjm_queue() -> pd.DataFrame`**
- GET https://www.pjm.com/-/media/planning/new-services-queue/new-services-queue.ashx
- Read Excel with pd.read_excel(BytesIO(resp.content))
- Find column containing "type" or "fuel", filter for LOAD|DEMAND|DR
- Find MW column, filter >= 50 MW
- Return filtered df

**`fetch_ferc_elibrary(days_back=365) -> list[dict]`**
- GET https://elibrary.ferc.gov/eLibrary/search
- Params: searchType=fullText, format=json
- Search terms (loop): "large load interconnection tribal", "data center tribal Indian"
- sleep(1) between requests
- Return list of {docket, title, date, url, search_term}

**`flag_near_tribal(df, lat_col, lon_col, rto) -> gpd.GeoDataFrame`**
- Build GeoDataFrame from lat/lon cols
- sjoin against TRIBAL_BUFFER
- Add columns: rto, flag_type="ferc_queue", flagged_date=now().isoformat()
- Return matched rows

**`main()`**
- Run PJM fetch, attempt to geocode, flag near tribal
- Run FERC eLibrary search
- Save results:
  - `data/intel/ferc_queue_flags.json`
  - `data/intel/ferc_elibrary_filings.json`
- Print summary counts

`if __name__ == "__main__"`: call main()

Target: ~350 lines.

---

## File 3: `intelligence/ownership_watch.py`

Core watchdog. Resolves LLC names from public land records to hyperscaler
parent companies. Uses SQLite so re-runs never re-fetch already-resolved names.

**Imports:** requests, pandas, geopandas, sqlite3, json, re, time, pathlib,
datetime, rapidfuzz.fuzz, rapidfuzz.process, os, dotenv

**DB schema (auto-create on startup):**
```sql
CREATE TABLE IF NOT EXISTS resolutions (
    name TEXT PRIMARY KEY,
    resolved_parent TEXT,
    confidence REAL,
    method TEXT,
    incorporation_date TEXT,
    registered_agent TEXT,
    flags TEXT,  -- JSON array
    checked_at TEXT
);

CREATE TABLE IF NOT EXISTS acquisitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    county_fips TEXT,
    county_name TEXT,
    buyer TEXT,
    resolved_parent TEXT,
    confidence REAL,
    acreage REAL,
    sale_date TEXT,
    lat REAL,
    lon REAL,
    flags TEXT,
    detected_at TEXT
);
```

**`init_db(db_path) -> sqlite3.Connection`**
Creates tables if not exist, returns connection.

**`resolve_llc(name, conn, api_key=None) -> dict`**
Check DB cache first — if name already resolved and checked within 30 days, return cached.
Resolution order:
1. Load flat lookup from `output/known_subsidiaries_flat.json`, check direct match → confidence 98
2. rapidfuzz process.extractOne against all flat keys, threshold 85 → confidence = score
3. If api_key: GET https://api.opencorporates.com/v0.4/companies/search?q={name}&jurisdiction_code=us
   - Check incorporation_date — if < 730 days old, add flag "LLC formed N days ago"
   - Check registered_address for: CT CORPORATION, CORPORATION SERVICE COMPANY,
     NATIONAL REGISTERED AGENTS → add flag, boost confidence to 65 min
4. Direct substring: check if any HYPERSCALER_MAP key is in name.upper() → confidence 96

Save result to DB. Return {resolved_parent, confidence, method, flags, incorporation_date,
registered_agent, is_llc}.

**`get_target_counties() -> list[dict]`**
- Load tribal_lands.gpkg, buffer 80km
- If `data/raw/counties/` missing: download Census TIGER county file and extract
- Spatial intersect → return [{fips, name, state}] list
- Cache result — don't re-download if counties dir exists

**`scan_arcgis_hub(county_fips, county_name, conn, min_acres=50, days_back=180) -> list[dict]`**
- GET https://hub.arcgis.com/api/v3/datasets?q=parcel+sales&filter[region]={county_fips}
- For first matching dataset, try CSV download
- Filter: acreage > min_acres, sale_date within days_back
- Find buyer column (owner/buyer/grantee)
- For each buyer row: call resolve_llc()
- Flag if: is_llc AND (generic_name OR known_hyperscaler OR suspicion_score >= 4)
- Suspicion scoring:
  - is_llc: +2
  - generic patterns (HOLDINGS|VENTURES|DIGITAL|CLOUD|COMPUTE|INFRASTRUCTURE): +2
  - known subsidiary match: +5
  - acreage > 500: +1
- Insert flagged rows into acquisitions table
- Return list of flagged dicts

**`export_flags(conn, output_path)`**
SELECT all from acquisitions where confidence > 0 or suspicion_score >= 4,
convert to GeoDataFrame if lat/lon present, save as GeoJSON.
Also save full acquisitions table as JSON.

**`main()`**
- init_db
- load_tribal boundaries (once)
- get_target_counties()
- For each county (with tqdm progress): scan_arcgis_hub, sleep(0.3)
- export_flags to output/land_acquisitions.geojson
- Print summary: total counties scanned, total flagged, resolved to known parent

`if __name__ == "__main__"`: call main()

Target: ~550 lines.

---

## File 4: `intelligence/impact_metrics.py`

Attaches concrete town-hall-ready numbers to each tribal land in the risk GeoJSON.
No API calls. Pure calculation from constants.

**Constants (all sourced):**
```python
WATER_GAL_PER_DAY_PER_MW = 3_000        # EPA 2025
ELEC_RATE_INCREASE_HIGH_PCT = 267        # Bloomberg analysis
ELEC_RATE_INCREASE_LOW_PCT = 50
JOBS_PROMISED = 1_500
JOBS_ACTUAL_PERMANENT = 3               # Rapid City SD public record
GRID_UPGRADE_COST_PER_MW = 500_000
HEAT_ISLAND_MAX_F = 16.0               # peer-reviewed
NOISE_DB_CONTINUOUS = 97               # Honor the Earth
OSHA_8HR_LIMIT_DB = 90
```

**`estimate_dc_mw(area_km2) -> float`**
< 10 km² → 100 MW, < 100 → 300, < 500 → 500, else → 1000

**`compute_impacts(area_km2, tribal_pop=1000) -> dict`**
Returns dict with water_annual_millions, water_equivalent_households,
elec_rate_high_pct, elec_rate_low_pct, monthly_bill_increase_usd,
grid_upgrade_cost_usd, grid_upgrade_per_household, jobs_promised,
jobs_actual, heat_island_f, noise_db, noise_vs_osha_db, estimated_dc_mw.

**`main()`**
- Load output/tribal_datacenter_risk.geojson
- For each feature: compute_impacts(area_km2)
- Merge impacts back into GDF
- Save output/tribal_datacenter_risk_with_impacts.geojson
- Also save output/impact_metrics.json (just the metrics, no geometry)
- Print example for highest combined_score tribal land

Target: ~150 lines.

---

## GitHub Actions: `.github/workflows/weekly_watchdog.yml`

```yaml
name: Weekly Ownership Watchdog

on:
  schedule:
    - cron: '0 6 * * 1'
  workflow_dispatch:

jobs:
  watchdog:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - name: Run intelligence layer
        run: |
          python intelligence/subsidiaries.py
          python intelligence/ferc_monitor.py
          python intelligence/ownership_watch.py
          python intelligence/impact_metrics.py
        env:
          CENSUS_API_KEY: ${{ secrets.CENSUS_API_KEY }}
          OPENCORPORATES_API_KEY: ${{ secrets.OPENCORPORATES_API_KEY }}
      - name: Commit outputs
        run: |
          git config user.email "sentinel@honorearth.org"
          git config user.name "Watchdog Bot"
          git add output/
          git diff --staged --quiet || git commit -m "Watchdog scan $(date +%Y-%m-%d)" && git push
```

---

## Additional requirements to add to requirements.txt

```
rapidfuzz>=3.5.0
feedparser>=6.0.0
httpx>=0.25.0
```

---

## Run Order (after building)

```bash
# 1. Seed the subsidiary database first — everything else reads from it
python intelligence/subsidiaries.py

# 2. FERC queue scan
python intelligence/ferc_monitor.py

# 3. Land acquisition watchdog (slow — iterates counties)
python intelligence/ownership_watch.py

# 4. Impact metrics (fast — no API calls)
python intelligence/impact_metrics.py
```

---

## Output Files Produced

| File | What it is |
|---|---|
| `output/known_subsidiaries.json` | Full hyperscaler subsidiary database |
| `output/known_subsidiaries_flat.json` | Flat name→parent lookup for fast resolution |
| `output/land_acquisitions.geojson` | Flagged land deals near tribal lands |
| `output/tribal_datacenter_risk_with_impacts.geojson` | Risk map + town hall numbers |
| `output/impact_metrics.json` | Impact projections per tribal land |
| `data/intel/ferc_queue_flags.json` | FERC large-load requests near tribal lands |
| `data/intel/ferc_elibrary_filings.json` | FERC text search results |
| `data/intel/ownership.db` | SQLite cache of all resolved LLCs |

---

## Priority Watch Entities

Tell Claude Code to hard-code these as the HIGHEST PRIORITY flags:

| Entity | Parent | Signal |
|---|---|---|
| VADATA INC | Amazon | Primary AWS real estate vehicle |
| STARGATE LLC | OpenAI/Oracle | $500B joint buildout |
| SGP OPERATOR LLC | OpenAI/Oracle | Stargate operating entity |
| GRACELAND ACQUISITIONS LLC | Meta | Oklahoma tribal targeting |
| PAPYRUS ACQUISITIONS LLC | Meta | Known Meta project name |
| QUANTUM VALLEY LLC | Alphabet | Google project code name |
| CHARLESTON EAST LLC | Alphabet | Google project code name |
| RREEF AMERICA LLC | Alphabet | Google real estate entity |

Any match = CRITICAL flag regardless of confidence score.
