# Data Center Sentinel — PLAN.md
**Ito Geospatial LLC** | Client: Honor the Earth / No Data Center Coalition

## What This Is
A predictive vulnerability map across all US tribal lands. Scores every tribal land unit against hyperscale data center siting criteria — flagging highest-risk communities **before** targeting begins.

Three scores per tribal area:
- `corp_score` — how attractive to a hyperscale developer (0-1)
- `vuln_score` — how vulnerable the community is to exploitation (0-1)
- `combined_score` — early-warning priority flag (0.45 × corp + 0.55 × vuln)

Risk tiers: `CRITICAL` / `HIGH` / `MODERATE` / `LOW`

Output: GeoJSON + PMTiles → Mapbox GL JS → Cloudflare Pages

---

## Pipeline Steps

| Step | Script | Input | Output |
|------|--------|-------|--------|
| 0 | `00_download_data.py` | (URLs) | `data/raw/*` |
| 1 | `01_process_tribal_lands.py` | TIGER AIANNH | `tribal_lands.gpkg` |
| 2 | `02_score_infrastructure.py` | tribal_lands + raw infra | `tribal_lands_corp_scored.gpkg` |
| 3 | `03_score_vulnerability.py` | corp_scored + EJScreen/Census | `tribal_lands_vuln_scored.gpkg` |
| 4 | `04_combine_scores.py` | vuln_scored + HTE tracker CSV | `tribal_datacenter_risk_full.gpkg` |
| 5 | `05_export_geojson.py` | risk_full | `output/*.geojson`, `output/stats.json` |

Run all: `./run_pipeline.sh`

---

## Data Sources

| Layer | Source | Notes |
|-------|--------|-------|
| Tribal Boundaries | Census TIGER 2023 AIANNH | Auto-downloaded |
| Transmission Lines | HIFLD | Auto-downloaded (HV >= 115kV filtered) |
| Substations | HIFLD | Auto-downloaded |
| Aquifers | USGS | Auto-downloaded |
| Opportunity Zones | IRS/CDFI Fund | Auto-downloaded |
| EJScreen | EPA 2023 | Auto-downloaded |
| NHD Water | USGS | Auto-downloaded (large ~10GB) |
| Urban Areas | Census TIGER | Auto-downloaded in step 3 |
| Superfund Sites | EPA GIS API | Fetched live in step 3 |
| FCC Fiber | FCC NBM | **Manual** — broadbandmap.fcc.gov → data/raw/fcc_fiber/ |
| FEMA Flood | FEMA NFHL | **Manual** — msc.fema.gov → data/raw/fema_flood/ |
| HTE Known Sites | Honor the Earth | **Manual** — export Google Form → data/raw/honor_earth_tracker.csv |
| Census ACS Poverty | Census API | Free key needed → CENSUS_API_KEY in .env |

---

## Setup

```bash
cp .env.example .env
# Add CENSUS_API_KEY (free at api.census.gov/data/key_signup.html)

conda install -n geodata geopandas pandas shapely fiona requests numpy pyproj rasterio rasterstats tqdm python-dotenv -c conda-forge
conda install -n geodata census -c conda-forge   # or: pip install census

./run_pipeline.sh
```

---

## Known TODOs / Gaps

- `score_terrain_flatness`: bbox aspect ratio proxy — replace with rasterstats mean slope over USGS 3DEP tiles
- `score_poverty_rate`: currently falls back to EJScreen — implement direct Census ACS tribal area (AIAN subject tables) matching by GEOID
- `score_population_size`: using area as proxy — add actual Census population (P001001) per tribal area
- FCC fiber scoring not yet integrated (data is manual download, large)
- FEMA flood scoring only fires if flood shapefiles present (manual download required)

---

## Next: Mapbox Frontend

1. Upload `output/tribal_datacenter_risk.geojson` to Mapbox Tileset (or serve flat file)
2. Mapbox GL JS map:
   - Choropleth fill by `risk_tier` (CRITICAL=red, HIGH=orange, MODERATE=yellow, LOW=gray)
   - Circle layer for `known_sites.geojson`
   - Popup: all score components on click
   - Filter controls: Risk tier / State / Known vs Predicted
   - Stats panel from `stats.json`
3. GitHub Actions: monthly pipeline re-run
4. Deploy: Cloudflare Pages

---

## Contributing Known Sites

1. Open GitHub issue with location, company, status
2. Or submit via Honor the Earth: honorearth.org/datacentertracker
3. Export their Google Sheet → `data/raw/honor_earth_tracker.csv` → re-run step 4+5
