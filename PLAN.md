# Tribal DC Scout — PLAN.md
**Ito Geospatial LLC** | Product: Tribal Sovereign Land Data Center Opportunity Map

## What This Is
A data center siting scorecard across all US tribal lands. Scores every tribal land unit on criteria that hyperscale and colocation developers actually use — flagging where **sovereign development potential is highest**.

The opportunity angle: tribal sovereign land offers regulatory advantages (reduced state/county permitting), unique tax incentives (Opportunity Zones), and the ability to negotiate custom power rates. This tool helps tribes identify and quantify that potential.

**One score per tribal area:**
- `siting_score` (= `corp_score`) — infrastructure attractiveness for data center development (0–1)

Opportunity tiers (stored as legacy names for webapp compat):
- `CRITICAL` → **Prime**     — siting_score ≥ 0.65
- `HIGH`     → **Strong**    — siting_score ≥ 0.40
- `MODERATE` → **Moderate**  — siting_score ≥ 0.20
- `LOW`      → **Low**       — siting_score < 0.20

Output: GeoJSON → MapLibre GL JS → Cloudflare Pages

---

## Business Model
- Sell per-tribe siting reports (PDF + GeoJSON) to tribal economic development offices
- White-label dashboard subscription for tribal nations or BIA regional offices
- Consulting: connect high-scoring tribes with data center developers

---

## Pipeline Steps

| Step | Script | Input | Output |
|------|--------|-------|--------|
| 0 | `00_download_data.py` | (URLs) | `data/raw/*` |
| 1 | `01_process_tribal_lands.py` | TIGER AIANNH | `tribal_lands.gpkg` |
| 2 | `02_score_infrastructure.py` | tribal_lands + raw infra | `tribal_lands_corp_scored.gpkg` |
| 3 | `03_score_vulnerability.py` | *(can skip — vuln not used)* | *(optional)* |
| 4 | `04_combine_scores.py` | corp_scored | `tribal_datacenter_risk_full.gpkg` |
| 5 | `05_export_geojson.py` | risk_full | `output/*.geojson`, `output/stats.json` |

Run all: `./run_pipeline.sh`

---

## Siting Score Components (max 100 pts → normalized 0–1)

| Factor | Points | Signal |
|--------|--------|--------|
| Transmission proximity (≥115kV) | 0–20 | Power access |
| Substation proximity | 0–15 | Grid connection capacity |
| Water availability (NHD) | 0–20 | Cooling water |
| Aquifer overlap | 0–10 | Long-term water supply |
| Land area | 0–15 | Room for large campus |
| Terrain flatness | 0–10 | Lower construction cost |
| Opportunity Zone overlap | 0–5 | Tax incentive |
| Flood risk penalty | 0 to −10 | Risk discount |

---

## Data Sources

| Layer | Source | Notes |
|-------|--------|-------|
| Tribal Boundaries | Census TIGER 2023 AIANNH | Auto-downloaded |
| Transmission Lines | HIFLD | Auto-downloaded (HV ≥ 115kV) |
| Substations | HIFLD | Auto-downloaded |
| Aquifers | USGS | Auto-downloaded |
| Opportunity Zones | IRS/CDFI Fund | Auto-downloaded |
| NHD Water | USGS | Auto-downloaded (large ~10GB) |
| FCC Fiber | FCC NBM | **Manual** — broadbandmap.fcc.gov → data/raw/fcc_fiber/ |
| FEMA Flood | FEMA NFHL | **Manual** — msc.fema.gov → data/raw/fema_flood/ |

---

## Setup

```bash
cp .env.example .env
# CENSUS_API_KEY optional (only needed for vulnerability scoring, not used here)

conda install -n geodata geopandas pandas shapely fiona requests numpy pyproj rasterio rasterstats tqdm python-dotenv -c conda-forge

./run_pipeline.sh
```

---

## Known TODOs / Enhancements

- **Market proximity score**: distance to nearest top-20 metro (latency signal for colocation DCs)
- **Climate score**: NOAA cooling degree days — cold climate = free air cooling potential
- **Renewable energy score**: wind/solar capacity factor on tribal land (power cost + green credibility)
- **Terrain flatness**: replace bbox proxy with rasterstats mean slope over USGS 3DEP tiles
- **Sovereign tier**: layer distinguishing trust land vs fee land status (affects permitting complexity)
- FCC fiber scoring not yet integrated (manual download)
- FEMA flood scoring only fires if flood shapefiles present (manual download)

---

## Webapp (Cloudflare Pages)

Stack: MapLibre GL JS + plain JS + Cloudflare Pages (no build step)

Features:
- Choropleth fill by opportunity tier (Prime=dark green → Low=gray)
- Toggle: Tier view vs continuous Siting Score gradient
- Overlay layers: transmission, substations, power plants, wind, gas pipelines
- Market signal overlays: existing DCs, developer activity, grid investment
- Click detail panel: siting score breakdown + sovereign advantages callout
- Filter by opportunity tier

Deploy:
```bash
# Push webapp/ to GitHub → Cloudflare Pages auto-deploys
git push origin main
```
