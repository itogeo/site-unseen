# Data Center Sentinel — Predictive Siting Pipeline
### Open Source Tool for Honor the Earth / No Data Center Coalition
**Built by Ito Geospatial LLC**

---

## Overview

This pipeline reverse-engineers corporate data center site selection criteria to generate a
**predictive vulnerability map** across all US tribal lands. Rather than waiting for NDAs to
leak or press releases to drop, this tool scores every tribal land unit against the exact
criteria corporations use — flagging the highest-risk communities **before** targeting begins.

**Two composite scores per tribal land unit:**
- `corp_score` — How attractive this land looks to a hyperscale developer
- `vuln_score` — How vulnerable the community is to being targeted/exploited
- `combined_score` — The early warning priority flag (high on both = urgent)

**Output:** GeoJSON + PMTiles ready for Mapbox GL JS deployment on Cloudflare Pages.

---

## Project Structure

```
datacenter-sentinel/
├── README.md
├── requirements.txt
├── .env.example
├── pipeline/
│   ├── 00_download_data.py        # Fetch all public federal datasets
│   ├── 01_process_tribal_lands.py # Clean + standardize tribal boundaries
│   ├── 02_score_infrastructure.py # Corporate attractiveness scoring
│   ├── 03_score_vulnerability.py  # Community vulnerability scoring
│   ├── 04_combine_scores.py       # Merge + normalize final scores
│   ├── 05_export_geojson.py       # Export for Mapbox / Cloudflare
│   └── utils.py                   # Shared helpers
├── data/
│   ├── raw/                       # Downloaded source data (gitignored)
│   └── processed/                 # Intermediate outputs
├── output/
│   └── tribal_datacenter_risk.geojson
└── tests/
    └── test_scoring.py
```

---

## Requirements

```txt
# requirements.txt
geopandas>=0.14.0
pandas>=2.0.0
shapely>=2.0.0
fiona>=1.9.0
requests>=2.31.0
numpy>=1.24.0
pyproj>=3.6.0
rasterio>=1.3.0
rasterstats>=0.19.0
census>=0.8.0
tqdm>=4.65.0
python-dotenv>=1.0.0
tippecanoe>=2.0.0   # For PMTiles export — install separately via brew/apt
```

```bash
pip install -r requirements.txt
```

---

## Environment Setup

```bash
# .env.example — copy to .env and fill in
CENSUS_API_KEY=your_census_api_key_here   # free at api.census.gov/data/key_signup.html
```

---

## 00 — Download All Source Data

```python
# pipeline/00_download_data.py
"""
Downloads all public federal datasets needed for scoring.
All sources are free, no API keys required except Census (free registration).
Run this once — outputs land in data/raw/
"""

import os
import requests
import zipfile
import io
from pathlib import Path
from tqdm import tqdm

RAW = Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)


def download_file(url: str, dest: Path, label: str):
    """Stream download with progress bar."""
    if dest.exists():
        print(f"  [skip] {label} already downloaded")
        return
    print(f"  [fetch] {label}")
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=label[:40]) as bar:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))


def download_zip(url: str, dest_dir: Path, label: str):
    """Download and extract a zip."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    sentinel = dest_dir / ".downloaded"
    if sentinel.exists():
        print(f"  [skip] {label} already extracted")
        return
    print(f"  [fetch+unzip] {label}")
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        z.extractall(dest_dir)
    sentinel.touch()


# ── 1. BIA Tribal Lands (Census TIGER 2023) ────────────────────────────────
# American Indian/Alaska Native/Native Hawaiian Areas
download_zip(
    url="https://www2.census.gov/geo/tiger/TIGER2023/AIANNH/tl_2023_us_aiannh.zip",
    dest_dir=RAW / "tribal_boundaries",
    label="BIA Tribal Boundaries (TIGER 2023)"
)

# ── 2. EIA Electric Transmission Lines ─────────────────────────────────────
# High-voltage transmission lines (Homeland Infrastructure Foundation)
download_zip(
    url="https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::electric-power-transmission-lines.zip",
    dest_dir=RAW / "transmission_lines",
    label="EIA Transmission Lines (HIFLD)"
)

# Fallback: EIA bulk download
download_file(
    url="https://atlas.eia.gov/datasets/eia::electric-power-transmission-lines/about",
    dest=RAW / "transmission_lines" / "eia_transmission_readme.txt",
    label="EIA Transmission Lines (readme)"
)

# ── 3. EIA Electric Substations ────────────────────────────────────────────
download_zip(
    url="https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::electric-substations.zip",
    dest_dir=RAW / "substations",
    label="Electric Substations (HIFLD)"
)

# ── 4. EIA Electricity Rates by State/Utility ──────────────────────────────
# Form EIA-861 — utility-level electricity rates and service territories
download_file(
    url="https://www.eia.gov/electricity/data/eia861/zip/f8612022.zip",
    dest=RAW / "eia861_rates.zip",
    label="EIA Form 861 Electricity Rates"
)

# ── 5. USGS National Hydrography Dataset (NHD) — Major Water Bodies ────────
# Using NHD Medium Resolution national file (perennial streams + water bodies)
download_zip(
    url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/National/HighResolution/GDB/NHD_H_National_GDB.zip",
    dest_dir=RAW / "nhd_water",
    label="USGS NHD Water Bodies (national)"
)

# ── 6. USGS Principal Aquifers ──────────────────────────────────────────────
download_zip(
    url="https://water.usgs.gov/GIS/dsdl/aquifers_us.zip",
    dest_dir=RAW / "aquifers",
    label="USGS Principal Aquifers"
)

# ── 7. FCC National Broadband Map — Fiber Infrastructure ───────────────────
# FCC fabric availability data (publicly downloadable)
print("  [manual] FCC Broadband: download from https://broadbandmap.fcc.gov/data-download")
print("           Select: Fixed Broadband Availability > Fiber > Download")
print("           Save to: data/raw/fcc_fiber/")

# ── 8. IRS Opportunity Zones ────────────────────────────────────────────────
download_file(
    url="https://www.cdfifund.gov/sites/cdfi/files/2018-06/OZ_Shapefile.zip",
    dest=RAW / "opportunity_zones.zip",
    label="IRS Opportunity Zones (CDFI Fund)"
)

# ── 9. USGS 3DEP Slope (1/3 arc-second) ────────────────────────────────────
# National elevation — we derive slope from this
# Use USGS TNM API — download by bounding box in scoring script
print("  [api] USGS 3DEP elevation pulled per-tile in scoring script")

# ── 10. FEMA National Flood Hazard Layer ───────────────────────────────────
download_zip(
    url="https://msc.fema.gov/portal/downloadProduct?productTypeID=NFHL&productSubTypeID=NFHL",
    dest_dir=RAW / "fema_flood",
    label="FEMA Flood Hazard (NFHL) — may require manual download"
)

# ── 11. EPA EJScreen — Environmental Justice + Cumulative Burden ───────────
download_zip(
    url="https://gaftp.epa.gov/EJSCREEN/2023/EJSCREEN_2023_BG_with_AS_CNMI_GU_VI.csv.zip",
    dest_dir=RAW / "ejscreen",
    label="EPA EJScreen 2023 (block group)"
)

# ── 12. Census ACS 5-Year — Poverty + Income ───────────────────────────────
print("  [api] Census ACS data pulled via census API in vulnerability script")
print("        Get free key at: https://api.census.gov/data/key_signup.html")

# ── 13. Known Existing Data Centers (DC Map + CBRE) ────────────────────────
# Open Infrastructure Map aggregates known DCs from OpenStreetMap
download_file(
    url="https://openinframap.org/stats",
    dest=RAW / "openinframap_readme.txt",
    label="OpenInfraMap (see URL for data download)"
)

print("\n[done] Raw data download complete.")
print("Manual steps needed: FCC broadband, FEMA flood (see messages above)")
```

---

## 01 — Process Tribal Boundaries

```python
# pipeline/01_process_tribal_lands.py
"""
Loads BIA tribal boundaries, standardizes CRS, adds basic attributes.
Output: data/processed/tribal_lands.gpkg
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path

RAW = Path("data/raw")
PROC = Path("data/processed")
PROC.mkdir(parents=True, exist_ok=True)

WORKING_CRS = "EPSG:5070"   # Albers Equal Area — good for area + distance calcs nationally
OUTPUT_CRS  = "EPSG:4326"   # WGS84 for final GeoJSON


def load_tribal_boundaries() -> gpd.GeoDataFrame:
    shp = next((RAW / "tribal_boundaries").glob("*.shp"))
    print(f"Loading tribal boundaries from {shp}")
    gdf = gpd.read_file(shp)
    print(f"  {len(gdf)} features loaded")
    print(f"  Columns: {list(gdf.columns)}")
    return gdf


def clean_tribal_lands(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Reproject to working CRS
    gdf = gdf.to_crs(WORKING_CRS)

    # Drop null geometries
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()

    # Standardize key columns (TIGER AIANNH schema)
    rename = {
        "GEOID":    "geoid",
        "NAME":     "tribe_name",
        "NAMELSAD": "tribe_name_full",
        "ALAND":    "land_area_sqm",
        "AWATER":   "water_area_sqm",
        "STATESFP": "state_fips",
    }
    gdf = gdf.rename(columns={k: v for k, v in rename.items() if k in gdf.columns})

    # Compute area in km²
    gdf["area_km2"] = gdf.geometry.area / 1_000_000

    # Compute centroid (for distance calculations)
    gdf["centroid_x"] = gdf.geometry.centroid.x
    gdf["centroid_y"] = gdf.geometry.centroid.y

    # Filter: keep federally recognized tribal lands only
    # TIGER LSAD codes for tribal areas
    tribal_lsad = ["00", "25", "27", "28", "29", "30", "31", "32", "33", "34",
                   "35", "36", "37", "38", "39", "40", "41", "42", "43", "44",
                   "45", "46", "47", "48", "49", "50", "51", "52", "53", "54",
                   "55", "56", "75", "76", "77", "78", "79", "80", "81", "82",
                   "83", "84", "85", "86", "87", "88", "89"]
    if "LSAD" in gdf.columns:
        gdf = gdf[gdf["LSAD"].isin(tribal_lsad)].copy()

    print(f"  After cleaning: {len(gdf)} tribal land units")
    print(f"  Total area: {gdf['area_km2'].sum():,.0f} km²")

    return gdf


def main():
    gdf = load_tribal_boundaries()
    gdf = clean_tribal_lands(gdf)
    out = PROC / "tribal_lands.gpkg"
    gdf.to_file(out, driver="GPKG")
    print(f"Saved to {out}")


if __name__ == "__main__":
    main()
```

---

## 02 — Score Corporate Attractiveness (Infrastructure)

```python
# pipeline/02_score_infrastructure.py
"""
Scores each tribal land unit on criteria hyperscale data center developers use.
Higher score = more attractive to a developer.

Scoring dimensions:
  - Transmission line proximity       (0-20 pts)
  - Substation capacity proximity     (0-15 pts)
  - Water availability                (0-20 pts)
  - Aquifer access                    (0-10 pts)
  - Land area (large contiguous)      (0-15 pts)
  - Terrain flatness                  (0-10 pts)
  - Fiber / broadband proximity       (0-10 pts)
  - Flood risk penalty                (0 to -10 pts)
  ─────────────────────────────────────────────────
  Max raw score: 100 pts → normalized 0-1
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from shapely.ops import nearest_points
import warnings
warnings.filterwarnings("ignore")

RAW  = Path("data/raw")
PROC = Path("data/processed")
WORKING_CRS = "EPSG:5070"


def load_tribal_lands() -> gpd.GeoDataFrame:
    return gpd.read_file(PROC / "tribal_lands.gpkg")


def score_transmission_proximity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Distance to nearest high-voltage (>= 115kV) transmission line.
    Score: 20 pts at 0km, 0 pts at 50km+
    Hyperscale requires direct HV interconnect or short extension.
    """
    shp = next((RAW / "transmission_lines").glob("*.shp"), None)
    if shp is None:
        print("  [warn] Transmission lines shapefile not found — scoring 0")
        return pd.Series(0, index=tribal.index)

    lines = gpd.read_file(shp).to_crs(WORKING_CRS)

    # Filter to high-voltage lines if voltage field exists
    volt_cols = [c for c in lines.columns if "volt" in c.lower() or "kv" in c.lower()]
    if volt_cols:
        col = volt_cols[0]
        lines[col] = pd.to_numeric(lines[col], errors="coerce")
        hv_lines = lines[lines[col] >= 115].copy()
        if len(hv_lines) < 100:  # fallback if filter too aggressive
            hv_lines = lines
    else:
        hv_lines = lines

    # Create union of all lines for efficient distance calc
    hv_union = hv_lines.geometry.unary_union

    distances = tribal.geometry.centroid.distance(hv_union) / 1000  # km

    # Score: 20 at 0km, linear decay to 0 at 50km
    scores = np.clip(20 * (1 - distances / 50), 0, 20)
    return scores.rename("score_transmission")


def score_substation_proximity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Distance to nearest substation >= 115kV.
    Score: 15 pts at 0km, 0 pts at 30km+
    """
    shp = next((RAW / "substations").glob("*.shp"), None)
    if shp is None:
        print("  [warn] Substations shapefile not found — scoring 0")
        return pd.Series(0, index=tribal.index)

    subs = gpd.read_file(shp).to_crs(WORKING_CRS)

    # Filter to high-voltage substations
    volt_cols = [c for c in subs.columns if "volt" in c.lower() or "kv" in c.lower() or "max_volt" in c.lower()]
    if volt_cols:
        col = volt_cols[0]
        subs[col] = pd.to_numeric(subs[col], errors="coerce")
        hv_subs = subs[subs[col] >= 115].copy()
        if len(hv_subs) < 50:
            hv_subs = subs
    else:
        hv_subs = subs

    sub_union = hv_subs.geometry.unary_union
    distances = tribal.geometry.centroid.distance(sub_union) / 1000  # km

    scores = np.clip(15 * (1 - distances / 30), 0, 15)
    return scores.rename("score_substation")


def score_water_availability(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Proximity to perennial water bodies / rivers (NHD).
    Score: 20 pts intersecting or adjacent, decay to 0 at 25km.
    Hyperscale DCs need 300k-5M gal/yr — water body proximity is critical.
    """
    # Try to load NHD flowlines or water bodies
    nhd_dir = RAW / "nhd_water"
    water_files = list(nhd_dir.glob("**/*.shp"))

    if not water_files:
        print("  [warn] NHD water data not found — using tribal water_area as proxy")
        # Fallback: use internal water area (Census AWATER field)
        if "water_area_sqm" in tribal.columns:
            water_km2 = tribal["water_area_sqm"] / 1_000_000
            scores = np.clip(20 * water_km2 / 50, 0, 20)
        else:
            scores = pd.Series(10, index=tribal.index)  # neutral
        return scores.rename("score_water")

    # Load flowlines (NHDFlowline) or water bodies (NHDWaterbody)
    water_gdf = None
    for f in water_files:
        name = f.stem.lower()
        if "flowline" in name or "waterbody" in name or "nhd" in name:
            try:
                water_gdf = gpd.read_file(f).to_crs(WORKING_CRS)
                # Filter to perennial (FCode 46006) or large water bodies
                if "FCode" in water_gdf.columns:
                    water_gdf = water_gdf[water_gdf["FCode"].isin([46006, 39004, 39009])]
                break
            except Exception:
                continue

    if water_gdf is None or len(water_gdf) == 0:
        print("  [warn] Could not parse NHD — defaulting water score to 10")
        return pd.Series(10, index=tribal.index).rename("score_water")

    water_union = water_gdf.geometry.unary_union
    distances = tribal.geometry.centroid.distance(water_union) / 1000  # km

    scores = np.clip(20 * (1 - distances / 25), 0, 20)
    return scores.rename("score_water")


def score_aquifer_access(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Whether tribal land overlaps a principal aquifer.
    Score: 10 pts if overlapping, 0 if not.
    """
    shp = next((RAW / "aquifers").glob("*.shp"), None)
    if shp is None:
        print("  [warn] Aquifer shapefile not found — scoring 5 (neutral)")
        return pd.Series(5, index=tribal.index).rename("score_aquifer")

    aquifers = gpd.read_file(shp).to_crs(WORKING_CRS)
    aquifer_union = aquifers.geometry.unary_union

    overlaps = tribal.geometry.intersects(aquifer_union).astype(int) * 10
    return overlaps.rename("score_aquifer")


def score_land_area(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Large contiguous land area is a primary driver.
    Score: 15 pts at >= 500 km², scaled down for smaller parcels.
    Hyperscale facilities need 200-2000+ acres (0.8-8+ km²) but prefer large
    land banks for future expansion.
    """
    area = tribal["area_km2"] if "area_km2" in tribal.columns else \
           tribal.geometry.area / 1_000_000

    # Log scale — rewards large land bases but not infinitely
    scores = np.clip(15 * np.log1p(area) / np.log1p(500), 0, 15)
    return scores.rename("score_land_area")


def score_terrain_flatness(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Flat terrain reduces grading costs significantly.
    Approximation: use bbox aspect ratio + known geography as proxy.
    Full implementation: USGS 3DEP slope raster via rasterstats.
    Score: 0-10 pts
    
    TODO: implement rasterstats mean slope once 3DEP tiles downloaded
    """
    # Proxy using bounding box compactness
    bounds = tribal.geometry.bounds
    width  = bounds["maxx"] - bounds["minx"]
    height = bounds["maxy"] - bounds["miny"]
    aspect = np.minimum(width, height) / (np.maximum(width, height) + 1e-9)

    # More square bbox = more likely flat valley/plain
    scores = np.clip(10 * aspect, 0, 10)
    return scores.rename("score_terrain")


def score_flood_risk_penalty(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Flood zone overlap is a disqualifier for most developers.
    Score: 0 (no penalty) to -10 (high overlap)
    """
    fema_dir = RAW / "fema_flood"
    fema_files = list(fema_dir.glob("**/*.shp"))

    if not fema_files:
        print("  [warn] FEMA flood data not found — no penalty applied")
        return pd.Series(0, index=tribal.index).rename("score_flood_penalty")

    # Load 100-year flood zones (Zone A, AE, AH, AO, etc.)
    flood_zones = []
    for f in fema_files[:5]:  # limit to avoid memory issues with national NFHL
        try:
            gdf = gpd.read_file(f).to_crs(WORKING_CRS)
            if "ZONE" in gdf.columns or "FLD_ZONE" in gdf.columns:
                zcol = "FLD_ZONE" if "FLD_ZONE" in gdf.columns else "ZONE"
                high_risk = gdf[gdf[zcol].str.startswith("A", na=False)].copy()
                flood_zones.append(high_risk)
        except Exception:
            continue

    if not flood_zones:
        return pd.Series(0, index=tribal.index).rename("score_flood_penalty")

    flood_gdf = gpd.GeoDataFrame(pd.concat(flood_zones), crs=WORKING_CRS)
    flood_union = flood_gdf.geometry.unary_union

    # Fraction of tribal land in flood zone
    def flood_fraction(geom):
        try:
            intersection = geom.intersection(flood_union)
            return intersection.area / geom.area
        except Exception:
            return 0

    fractions = tribal.geometry.apply(flood_fraction)
    penalties = -10 * fractions  # 0 to -10
    return penalties.rename("score_flood_penalty")


def score_opportunity_zone(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Opportunity Zone overlap = major tax incentive for developers.
    Score: +5 pts if overlapping (a corporate attractiveness boost)
    """
    oz_file = RAW / "opportunity_zones.zip"
    if not oz_file.exists():
        print("  [warn] Opportunity Zones not found — scoring 0")
        return pd.Series(0, index=tribal.index).rename("score_opp_zone")

    import zipfile, io
    with zipfile.ZipFile(oz_file) as z:
        shp_name = [n for n in z.namelist() if n.endswith(".shp")]
        if not shp_name:
            return pd.Series(0, index=tribal.index).rename("score_opp_zone")
        oz = gpd.read_file(z.open(shp_name[0])).to_crs(WORKING_CRS)

    oz_union = oz.geometry.unary_union
    overlaps = tribal.geometry.intersects(oz_union).astype(int) * 5
    return overlaps.rename("score_opp_zone")


def combine_infrastructure_scores(tribal: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Run all infrastructure scoring functions and attach results."""
    print("Scoring transmission proximity...")
    tribal["score_transmission"]  = score_transmission_proximity(tribal).values

    print("Scoring substation proximity...")
    tribal["score_substation"]    = score_substation_proximity(tribal).values

    print("Scoring water availability...")
    tribal["score_water"]         = score_water_availability(tribal).values

    print("Scoring aquifer access...")
    tribal["score_aquifer"]       = score_aquifer_access(tribal).values

    print("Scoring land area...")
    tribal["score_land_area"]     = score_land_area(tribal).values

    print("Scoring terrain flatness...")
    tribal["score_terrain"]       = score_terrain_flatness(tribal).values

    print("Scoring flood risk penalty...")
    tribal["score_flood_penalty"] = score_flood_risk_penalty(tribal).values

    print("Scoring opportunity zone overlap...")
    tribal["score_opp_zone"]      = score_opportunity_zone(tribal).values

    # Raw corporate score (max 100)
    score_cols = [
        "score_transmission", "score_substation", "score_water",
        "score_aquifer", "score_land_area", "score_terrain",
        "score_flood_penalty", "score_opp_zone"
    ]
    tribal["corp_score_raw"] = tribal[score_cols].sum(axis=1)

    # Normalize 0-1
    raw_min = tribal["corp_score_raw"].min()
    raw_max = tribal["corp_score_raw"].max()
    tribal["corp_score"] = (tribal["corp_score_raw"] - raw_min) / (raw_max - raw_min + 1e-9)

    return tribal


def main():
    print("Loading tribal lands...")
    tribal = load_tribal_lands()
    print(f"  {len(tribal)} tribal land units")

    tribal = combine_infrastructure_scores(tribal)

    out = PROC / "tribal_lands_corp_scored.gpkg"
    tribal.to_file(out, driver="GPKG")
    print(f"\nSaved to {out}")
    print(f"Corp score range: {tribal['corp_score'].min():.3f} – {tribal['corp_score'].max():.3f}")
    print(f"Top 10 most attractive tribal lands:")
    top = tribal.nlargest(10, "corp_score")[["tribe_name", "corp_score", "area_km2"]]
    print(top.to_string())


if __name__ == "__main__":
    main()
```

---

## 03 — Score Community Vulnerability

```python
# pipeline/03_score_vulnerability.py
"""
Scores each tribal land unit on vulnerability to predatory targeting.
Higher score = more vulnerable community.

Scoring dimensions:
  - Poverty rate                      (0-25 pts)
  - Low population (less pushback)    (0-10 pts)
  - Opportunity Zone overlap          (0-10 pts)  [already signals poverty]
  - EJScreen cumulative burden        (0-20 pts)
  - Prior industry targeting          (0-15 pts)  [sacrifice zone history]
  - Remoteness / media invisibility   (0-10 pts)
  - Jurisdictional complexity flag    (0-10 pts)  [always true for tribal]
  ─────────────────────────────────────────────────
  Max raw score: 100 pts → normalized 0-1
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from census import Census
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings("ignore")

load_dotenv()

RAW  = Path("data/raw")
PROC = Path("data/processed")
WORKING_CRS = "EPSG:5070"


def load_tribal_lands() -> gpd.GeoDataFrame:
    return gpd.read_file(PROC / "tribal_lands_corp_scored.gpkg")


def score_poverty_rate(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Pull Census ACS 5-year poverty data by tribal census tract.
    Score: 25 pts at 100% poverty, scaled linearly.
    
    Uses Census API — requires free key in .env (CENSUS_API_KEY)
    Fallback: uses EJScreen LOWINCPCT if available
    """
    api_key = os.getenv("CENSUS_API_KEY")
    if not api_key:
        print("  [warn] No CENSUS_API_KEY — attempting EJScreen fallback for poverty")
        return score_poverty_from_ejscreen(tribal)

    try:
        c = Census(api_key)
        # B17001_001E = total pop for poverty status
        # B17001_002E = below poverty level
        print("  Pulling ACS 5-year poverty data by state...")

        state_fips = tribal["state_fips"].dropna().unique() if "state_fips" in tribal.columns else []

        all_poverty = []
        for state in state_fips:
            try:
                data = c.acs5.state_county(
                    fields=("NAME", "B17001_001E", "B17001_002E"),
                    state_fips=str(state).zfill(2),
                    county_fips="*"
                )
                df = pd.DataFrame(data)
                df["poverty_pct"] = pd.to_numeric(df["B17001_002E"], errors="coerce") / \
                                    pd.to_numeric(df["B17001_001E"], errors="coerce").replace(0, np.nan)
                all_poverty.append(df)
            except Exception as e:
                print(f"    [warn] Census API error for state {state}: {e}")

        if not all_poverty:
            return score_poverty_from_ejscreen(tribal)

        # NOTE: County-level poverty is a coarse proxy for tribal land poverty.
        # Tribal-specific poverty from Census is available via the ACS AIAN subject tables
        # (e.g., S0201) but requires geoid matching. This is a working approximation.
        # TODO: match tribal GEOIDs to ACS tribal area tables directly
        poverty_df = pd.concat(all_poverty, ignore_index=True)

        # For now return neutral score — proper tribal poverty matching is a TODO
        # that requires Census AIAN subject table alignment
        print("  [note] Using EJScreen for poverty (tribal-specific ACS matching is TODO)")
        return score_poverty_from_ejscreen(tribal)

    except Exception as e:
        print(f"  [warn] Census API failed: {e} — using EJScreen")
        return score_poverty_from_ejscreen(tribal)


def score_poverty_from_ejscreen(tribal: gpd.GeoDataFrame) -> pd.Series:
    """Fallback: use EPA EJScreen LOWINCPCT (low income %) field."""
    ej_dir = RAW / "ejscreen"
    ej_files = list(ej_dir.glob("*.csv"))

    if not ej_files:
        print("  [warn] EJScreen not found — using flat poverty score of 15")
        return pd.Series(15, index=tribal.index).rename("score_poverty")

    print("  Loading EJScreen data...")
    ej = pd.read_csv(ej_files[0], usecols=["ID", "LOWINCPCT", "SHAPE_CENTROID_X", "SHAPE_CENTROID_Y"],
                     low_memory=False)
    ej = ej.dropna(subset=["SHAPE_CENTROID_X", "SHAPE_CENTROID_Y"])

    ej_gdf = gpd.GeoDataFrame(
        ej,
        geometry=gpd.points_from_xy(ej["SHAPE_CENTROID_X"], ej["SHAPE_CENTROID_Y"]),
        crs="EPSG:4326"
    ).to_crs(WORKING_CRS)

    # Spatial join: mean LOWINCPCT within each tribal boundary
    joined = gpd.sjoin(ej_gdf, tribal[["geoid", "geometry"]], how="left", predicate="within")
    mean_poverty = joined.groupby("index_right")["LOWINCPCT"].mean()

    scores = mean_poverty.reindex(tribal.index, fill_value=0.3) * 25  # 0-25 pts
    return scores.rename("score_poverty")


def score_population_size(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Smaller population = less political resistance capacity.
    Score: 10 pts for pop < 500, scaling down to 0 at 50,000+
    """
    # Use land area as proxy if population not available
    # TODO: join Census P001001 (total population) per tribal area
    area = tribal["area_km2"] if "area_km2" in tribal.columns else \
           tribal.geometry.area / 1_000_000

    # Smaller, more isolated parcels score higher on vulnerability
    # Invert land area — larger land bases often have more political capacity
    scores = np.clip(10 * np.exp(-area / 100), 0, 10)
    return scores.rename("score_population")


def score_ejscreen_burden(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    EPA EJScreen Supplemental Index — cumulative environmental burden.
    Communities already carrying high burden are more vulnerable to more.
    Score: 0-20 pts based on percentile rank.
    """
    ej_dir = RAW / "ejscreen"
    ej_files = list(ej_dir.glob("*.csv"))

    if not ej_files:
        print("  [warn] EJScreen not found — scoring 10 neutral")
        return pd.Series(10, index=tribal.index).rename("score_ejscreen")

    try:
        ej = pd.read_csv(ej_files[0],
                         usecols=["ID", "SUPPLEMENTAL_INDEX_USN",
                                  "SHAPE_CENTROID_X", "SHAPE_CENTROID_Y"],
                         low_memory=False)
    except ValueError:
        # Column name may vary by EJScreen version
        ej = pd.read_csv(ej_files[0], low_memory=False)
        supp_col = [c for c in ej.columns if "SUPPL" in c.upper() or "INDEX" in c.upper()]
        if not supp_col:
            return pd.Series(10, index=tribal.index).rename("score_ejscreen")
        ej = ej[["SHAPE_CENTROID_X", "SHAPE_CENTROID_Y", supp_col[0]]].copy()
        ej.columns = ["SHAPE_CENTROID_X", "SHAPE_CENTROID_Y", "SUPPLEMENTAL_INDEX_USN"]

    ej = ej.dropna(subset=["SHAPE_CENTROID_X", "SHAPE_CENTROID_Y"])
    ej_gdf = gpd.GeoDataFrame(
        ej,
        geometry=gpd.points_from_xy(ej["SHAPE_CENTROID_X"], ej["SHAPE_CENTROID_Y"]),
        crs="EPSG:4326"
    ).to_crs(WORKING_CRS)

    joined = gpd.sjoin(ej_gdf, tribal[["geoid", "geometry"]], how="left", predicate="within")
    mean_burden = joined.groupby("index_right")["SUPPLEMENTAL_INDEX_USN"].mean()

    # Normalize to 0-20
    scores = mean_burden.reindex(tribal.index, fill_value=50)
    scores = np.clip(20 * scores / 100, 0, 20)
    return scores.rename("score_ejscreen")


def score_sacrifice_zone_history(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Has this land been previously targeted by extractive industry?
    Uses proximity to known mining sites, superfund sites, pipeline ROWs.
    Score: 0-15 pts

    Data sources:
    - EPA Superfund National Priorities List
    - USGS Mineral Resources (mine locations)
    - PHMSA hazardous liquid pipelines
    """
    # EPA Superfund sites (NPL) — publicly available
    superfund_url = "https://gis.epa.gov/arcgis/rest/services/OEI/FRS_INTERESTS/MapServer/21/query?where=1%3D1&outFields=*&f=geojson"
    superfund_path = RAW / "superfund_sites.geojson"

    if not superfund_path.exists():
        try:
            import requests
            print("  Fetching EPA Superfund sites...")
            # Note: full NPL is large — fetch via pagination in production
            resp = requests.get(superfund_url, timeout=60)
            if resp.status_code == 200:
                superfund_path.write_text(resp.text)
        except Exception as e:
            print(f"  [warn] Superfund fetch failed: {e}")

    if superfund_path.exists():
        try:
            sf = gpd.read_file(superfund_path).to_crs(WORKING_CRS)
            sf_union = sf.geometry.unary_union
            distances = tribal.geometry.centroid.distance(sf_union) / 1000  # km
            scores = np.clip(15 * (1 - distances / 100), 0, 15)
        except Exception:
            scores = pd.Series(7, index=tribal.index)
    else:
        print("  [warn] Superfund data unavailable — scoring 7 neutral")
        scores = pd.Series(7, index=tribal.index)

    return scores.rename("score_sacrifice_history")


def score_remoteness(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Remote communities have lower media visibility and slower legal response.
    Proxy: distance from nearest urban area (50k+ pop city).
    Score: 10 pts most remote, 0 pts within 30km of major city.
    """
    # Use Census Urban Areas shapefile
    urban_path = RAW / "urban_areas"
    urban_files = list(urban_path.glob("*.shp")) if urban_path.exists() else []

    if not urban_files:
        # Download Census urban areas if not present
        try:
            import requests, zipfile, io
            url = "https://www2.census.gov/geo/tiger/TIGER2023/UAC/tl_2023_us_uac20.zip"
            print("  Fetching Census urban areas...")
            resp = requests.get(url, timeout=120)
            urban_path.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                z.extractall(urban_path)
            urban_files = list(urban_path.glob("*.shp"))
        except Exception as e:
            print(f"  [warn] Urban areas fetch failed: {e} — scoring 5 neutral")
            return pd.Series(5, index=tribal.index).rename("score_remoteness")

    urban = gpd.read_file(urban_files[0]).to_crs(WORKING_CRS)
    # Filter to urbanized areas (50k+ pop)
    if "UATYPE20" in urban.columns:
        urban = urban[urban["UATYPE20"] == "U"].copy()

    urban_union = urban.geometry.unary_union
    distances = tribal.geometry.centroid.distance(urban_union) / 1000  # km

    # 10 pts at 200km+, 0 pts within 30km
    scores = np.clip(10 * (distances - 30) / 170, 0, 10)
    return scores.rename("score_remoteness")


def score_jurisdictional_complexity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    All tribal lands have inherent jurisdictional complexity.
    Smaller nations / more isolated = less regulatory capacity.
    Base score: 5 pts for all tribal land (inherent).
    Additional points for very small land area (proxy for limited governance capacity).
    Score: 5-10 pts
    """
    area = tribal["area_km2"] if "area_km2" in tribal.columns else \
           tribal.geometry.area / 1_000_000

    # Extra vulnerability for very small land bases (< 10 km²)
    extra = np.clip(5 * np.exp(-area / 10), 0, 5)
    scores = 5 + extra  # 5-10 pts
    return scores.rename("score_jurisdiction")


def combine_vulnerability_scores(tribal: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Run all vulnerability scoring functions and attach results."""
    print("Scoring poverty rate...")
    tribal["score_poverty"]           = score_poverty_rate(tribal).values

    print("Scoring population size...")
    tribal["score_population"]        = score_population_size(tribal).values

    print("Scoring EJScreen cumulative burden...")
    tribal["score_ejscreen"]          = score_ejscreen_burden(tribal).values

    print("Scoring sacrifice zone history...")
    tribal["score_sacrifice_history"] = score_sacrifice_zone_history(tribal).values

    print("Scoring remoteness...")
    tribal["score_remoteness"]        = score_remoteness(tribal).values

    print("Scoring jurisdictional complexity...")
    tribal["score_jurisdiction"]      = score_jurisdictional_complexity(tribal).values

    # Raw vulnerability score (max 100)
    vuln_cols = [
        "score_poverty", "score_population", "score_ejscreen",
        "score_sacrifice_history", "score_remoteness", "score_jurisdiction"
    ]
    tribal["vuln_score_raw"] = tribal[vuln_cols].sum(axis=1)

    # Normalize 0-1
    raw_min = tribal["vuln_score_raw"].min()
    raw_max = tribal["vuln_score_raw"].max()
    tribal["vuln_score"] = (tribal["vuln_score_raw"] - raw_min) / (raw_max - raw_min + 1e-9)

    return tribal


def main():
    print("Loading tribal lands (with corp scores)...")
    tribal = load_tribal_lands()
    print(f"  {len(tribal)} tribal land units")

    tribal = combine_vulnerability_scores(tribal)

    out = PROC / "tribal_lands_vuln_scored.gpkg"
    tribal.to_file(out, driver="GPKG")
    print(f"\nSaved to {out}")
    print(f"Vuln score range: {tribal['vuln_score'].min():.3f} – {tribal['vuln_score'].max():.3f}")
    print(f"Top 10 most vulnerable tribal lands:")
    top = tribal.nlargest(10, "vuln_score")[["tribe_name", "vuln_score", "area_km2"]]
    print(top.to_string())


if __name__ == "__main__":
    main()
```

---

## 04 — Combine Scores + Priority Classification

```python
# pipeline/04_combine_scores.py
"""
Merges corporate attractiveness + community vulnerability scores.
Computes combined priority score and assigns risk tiers.

Risk Tiers:
  CRITICAL  — High corp score + High vuln score (immediate alert)
  HIGH      — High on either dimension
  MODERATE  — Medium on both
  LOW       — Low attractiveness or low vulnerability

Also attaches known Honor the Earth tracker data if available.
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path

PROC = Path("data/processed")


def classify_risk_tier(corp: float, vuln: float) -> str:
    """Assign a risk tier based on combined scores."""
    if corp >= 0.65 and vuln >= 0.65:
        return "CRITICAL"
    elif corp >= 0.65 or vuln >= 0.65:
        return "HIGH"
    elif corp >= 0.35 and vuln >= 0.35:
        return "MODERATE"
    else:
        return "LOW"


def load_honor_earth_known_sites() -> gpd.GeoDataFrame | None:
    """
    Load known data center sites from Honor the Earth's crowdsource tracker.
    Currently their data comes from a Google Form — export that CSV here.
    
    Expected CSV columns (from their Google Form):
      - tribe_name / community_name
      - location_description
      - lat / lon (if available)
      - status (proposed / under_construction / blocked / moratorium)
      - company_name
      - notes
    """
    csv_path = Path("data/raw/honor_earth_tracker.csv")
    if not csv_path.exists():
        print("  [info] No Honor the Earth tracker CSV found")
        print("         Export from their Google Form and save to data/raw/honor_earth_tracker.csv")
        return None

    df = pd.read_csv(csv_path)
    print(f"  Loaded {len(df)} known sites from Honor the Earth tracker")

    # If lat/lon present, make spatial
    if "lat" in df.columns and "lon" in df.columns:
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326"
        )
        return gdf

    return None


def main():
    print("Loading scored tribal lands...")
    tribal = gpd.read_file(PROC / "tribal_lands_vuln_scored.gpkg")
    print(f"  {len(tribal)} tribal land units")

    # ── Combined Score ──────────────────────────────────────────────────────
    # Weighted average: slightly weight vulnerability higher
    # (the goal is protecting communities, not just finding attractive sites)
    w_corp = 0.45
    w_vuln = 0.55
    tribal["combined_score"] = (
        w_corp * tribal["corp_score"] +
        w_vuln * tribal["vuln_score"]
    )

    # ── Risk Tier ───────────────────────────────────────────────────────────
    tribal["risk_tier"] = tribal.apply(
        lambda row: classify_risk_tier(row["corp_score"], row["vuln_score"]),
        axis=1
    )

    # ── Alert Priority (1-5 for map symbology) ──────────────────────────────
    tier_order = {"CRITICAL": 5, "HIGH": 4, "MODERATE": 3, "LOW": 2}
    tribal["priority"] = tribal["risk_tier"].map(tier_order).fillna(1)

    # ── Attach Known HTE Sites ──────────────────────────────────────────────
    tribal["known_datacenter"] = False
    tribal["known_dc_status"]  = None
    tribal["known_dc_company"] = None

    hte_sites = load_honor_earth_known_sites()
    if hte_sites is not None:
        hte_sites = hte_sites.to_crs(tribal.crs)
        joined = gpd.sjoin(hte_sites, tribal[["geoid", "geometry"]], how="left", predicate="within")
        confirmed_geoids = joined["geoid"].dropna().unique()
        tribal.loc[tribal["geoid"].isin(confirmed_geoids), "known_datacenter"] = True

        # Attach status and company where known
        for _, site in hte_sites.iterrows():
            mask = tribal.geometry.contains(site.geometry)
            if mask.any():
                if "status" in site.index:
                    tribal.loc[mask, "known_dc_status"] = site.get("status")
                if "company_name" in site.index:
                    tribal.loc[mask, "known_dc_company"] = site.get("company_name")

    # ── Summary Stats ───────────────────────────────────────────────────────
    print("\n=== Risk Tier Summary ===")
    tier_counts = tribal["risk_tier"].value_counts()
    for tier in ["CRITICAL", "HIGH", "MODERATE", "LOW"]:
        count = tier_counts.get(tier, 0)
        print(f"  {tier:10s}: {count:4d} tribal land units")

    print(f"\nKnown active data center sites: {tribal['known_datacenter'].sum()}")

    print("\nTop 20 CRITICAL + HIGH priority tribal lands:")
    top = tribal[tribal["risk_tier"].isin(["CRITICAL", "HIGH"])].nlargest(20, "combined_score")[
        ["tribe_name", "risk_tier", "corp_score", "vuln_score", "combined_score", "known_datacenter"]
    ]
    print(top.to_string())

    # ── Save ─────────────────────────────────────────────────────────────────
    out = PROC / "tribal_datacenter_risk_full.gpkg"
    tribal.to_file(out, driver="GPKG")
    print(f"\nSaved full scored dataset to {out}")


if __name__ == "__main__":
    main()
```

---

## 05 — Export GeoJSON for Mapbox

```python
# pipeline/05_export_geojson.py
"""
Exports final scored data as optimized GeoJSON (and optionally PMTiles)
for Mapbox GL JS deployment on Cloudflare Pages.

Outputs:
  output/tribal_datacenter_risk.geojson   — full dataset for Mapbox
  output/tribal_datacenter_risk_pts.geojson — centroid points (fast load)
  output/known_sites.geojson              — confirmed Honor the Earth sites only
"""

import geopandas as gpd
import pandas as pd
import json
from pathlib import Path

PROC   = Path("data/processed")
OUTPUT = Path("output")
OUTPUT.mkdir(exist_ok=True)

OUTPUT_CRS = "EPSG:4326"

# Columns to keep in export (keep file size manageable)
EXPORT_COLS = [
    "geoid", "tribe_name", "tribe_name_full",
    "area_km2", "state_fips",
    # Scores
    "corp_score", "vuln_score", "combined_score",
    "risk_tier", "priority",
    # Score components
    "score_transmission", "score_substation", "score_water",
    "score_aquifer", "score_land_area", "score_terrain",
    "score_flood_penalty", "score_opp_zone",
    "score_poverty", "score_ejscreen",
    "score_sacrifice_history", "score_remoteness", "score_jurisdiction",
    # Known sites
    "known_datacenter", "known_dc_status", "known_dc_company",
    # Geometry
    "geometry"
]


def round_scores(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Round score columns to 3 decimal places to reduce file size."""
    score_cols = [c for c in gdf.columns if c.startswith("score_") or c.endswith("_score")]
    for col in score_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].round(3)
    return gdf


def simplify_geometries(gdf: gpd.GeoDataFrame, tolerance_m: float = 500) -> gpd.GeoDataFrame:
    """
    Simplify polygon geometries to reduce file size.
    500m tolerance is imperceptible at zoom 6-8 (national view).
    """
    working_crs = "EPSG:5070"
    gdf = gdf.to_crs(working_crs)
    gdf["geometry"] = gdf.geometry.simplify(tolerance_m, preserve_topology=True)
    gdf = gdf.to_crs("EPSG:4326")
    return gdf


def main():
    print("Loading full scored dataset...")
    gdf = gpd.read_file(PROC / "tribal_datacenter_risk_full.gpkg")
    print(f"  {len(gdf)} features")

    # Keep only export columns
    export_cols_present = [c for c in EXPORT_COLS if c in gdf.columns]
    gdf = gdf[export_cols_present].copy()

    # Round scores
    gdf = round_scores(gdf)

    # ── Full Polygon Export ──────────────────────────────────────────────────
    print("Simplifying geometries...")
    gdf_simplified = simplify_geometries(gdf.copy())
    gdf_simplified = gdf_simplified.to_crs(OUTPUT_CRS)

    poly_out = OUTPUT / "tribal_datacenter_risk.geojson"
    gdf_simplified.to_file(poly_out, driver="GeoJSON")
    size_mb = poly_out.stat().st_size / 1_000_000
    print(f"  Polygon GeoJSON: {poly_out} ({size_mb:.1f} MB)")

    # ── Centroid Point Export (fast initial load) ────────────────────────────
    gdf_pts = gdf.copy().to_crs(OUTPUT_CRS)
    gdf_pts["geometry"] = gdf_pts.geometry.centroid
    pts_out = OUTPUT / "tribal_datacenter_risk_pts.geojson"
    gdf_pts.to_file(pts_out, driver="GeoJSON")
    pts_mb = pts_out.stat().st_size / 1_000_000
    print(f"  Points GeoJSON:  {pts_out} ({pts_mb:.1f} MB)")

    # ── Known Sites Only ─────────────────────────────────────────────────────
    if "known_datacenter" in gdf.columns:
        known = gdf[gdf["known_datacenter"] == True].copy().to_crs(OUTPUT_CRS)
        known["geometry"] = known.geometry.centroid
        known_out = OUTPUT / "known_sites.geojson"
        known.to_file(known_out, driver="GeoJSON")
        print(f"  Known sites:     {known_out} ({len(known)} features)")

    # ── PMTiles (optional, requires tippecanoe) ──────────────────────────────
    print("\nOptional: Generate PMTiles for high-performance Mapbox rendering")
    print("  Run: tippecanoe -o output/tribal_risk.pmtiles \\")
    print("         --minimum-zoom=3 --maximum-zoom=12 \\")
    print("         --layer=tribal_risk \\")
    print("         output/tribal_datacenter_risk.geojson")

    # ── Stats JSON (for dashboard cards) ────────────────────────────────────
    gdf_stats = gpd.read_file(PROC / "tribal_datacenter_risk_full.gpkg")
    stats = {
        "total_tribal_lands": int(len(gdf_stats)),
        "critical_count":     int((gdf_stats["risk_tier"] == "CRITICAL").sum()),
        "high_count":         int((gdf_stats["risk_tier"] == "HIGH").sum()),
        "moderate_count":     int((gdf_stats["risk_tier"] == "MODERATE").sum()),
        "known_sites":        int(gdf_stats["known_datacenter"].sum()) if "known_datacenter" in gdf_stats.columns else 0,
        "total_area_km2":     float(gdf_stats["area_km2"].sum().round(0)) if "area_km2" in gdf_stats.columns else 0,
        "top_critical": gdf_stats[gdf_stats["risk_tier"] == "CRITICAL"].nlargest(5, "combined_score")[
            ["tribe_name", "combined_score", "corp_score", "vuln_score"]
        ].round(3).to_dict("records")
    }

    stats_out = OUTPUT / "stats.json"
    stats_out.write_text(json.dumps(stats, indent=2))
    print(f"\nStats JSON: {stats_out}")
    print(json.dumps(stats, indent=2))

    print("\n[done] Export complete.")


if __name__ == "__main__":
    main()
```

---

## utils.py — Shared Helpers

```python
# pipeline/utils.py

import geopandas as gpd
import numpy as np
from pathlib import Path


def normalize_series(s, out_min=0, out_max=1):
    """Min-max normalize a pandas Series."""
    s_min, s_max = s.min(), s.max()
    if s_max == s_min:
        return s * 0 + (out_min + out_max) / 2
    return out_min + (s - s_min) / (s_max - s_min) * (out_max - out_min)


def distance_score(distances_km, max_dist_km, max_pts):
    """Linear decay score from 0 to max_dist_km."""
    return np.clip(max_pts * (1 - distances_km / max_dist_km), 0, max_pts)


def nearest_feature_distance(origins: gpd.GeoDataFrame,
                              targets: gpd.GeoDataFrame,
                              unit: str = "km") -> "pd.Series":
    """
    Compute distance from each origin centroid to nearest target feature.
    Returns Series of distances in specified unit.
    """
    target_union = targets.geometry.unary_union
    distances = origins.geometry.centroid.distance(target_union)
    if unit == "km":
        return distances / 1000
    elif unit == "m":
        return distances
    elif unit == "mi":
        return distances / 1609.34
    return distances


def print_score_summary(gdf, score_col, label):
    """Print distribution summary for a score column."""
    s = gdf[score_col]
    print(f"\n{label} ({score_col}):")
    print(f"  min={s.min():.3f}  mean={s.mean():.3f}  max={s.max():.3f}  "
          f"p25={s.quantile(0.25):.3f}  p75={s.quantile(0.75):.3f}")
```

---

## run_pipeline.sh — Run Everything

```bash
#!/bin/bash
# run_pipeline.sh
# Run the full Data Center Sentinel pipeline end to end

set -e  # exit on any error

echo "======================================"
echo "  Data Center Sentinel Pipeline"
echo "  Ito Geospatial / Honor the Earth"
echo "======================================"
echo ""

echo "Step 0: Downloading source data..."
python pipeline/00_download_data.py

echo ""
echo "Step 1: Processing tribal boundaries..."
python pipeline/01_process_tribal_lands.py

echo ""
echo "Step 2: Scoring corporate attractiveness..."
python pipeline/02_score_infrastructure.py

echo ""
echo "Step 3: Scoring community vulnerability..."
python pipeline/03_score_vulnerability.py

echo ""
echo "Step 4: Combining scores + classifying risk..."
python pipeline/04_combine_scores.py

echo ""
echo "Step 5: Exporting GeoJSON for Mapbox..."
python pipeline/05_export_geojson.py

echo ""
echo "======================================"
echo "  Pipeline complete!"
echo "  Outputs in: output/"
echo "    - tribal_datacenter_risk.geojson"
echo "    - tribal_datacenter_risk_pts.geojson"
echo "    - known_sites.geojson"
echo "    - stats.json"
echo "======================================"
```

---

## tests/test_scoring.py

```python
# tests/test_scoring.py
"""Basic sanity checks on scoring output."""

import geopandas as gpd
import pytest
from pathlib import Path

OUTPUT = Path("output")
PROC   = Path("data/processed")


def test_output_exists():
    assert (OUTPUT / "tribal_datacenter_risk.geojson").exists()
    assert (OUTPUT / "stats.json").exists()


def test_scores_in_range():
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    assert gdf["corp_score"].between(0, 1).all(),   "corp_score out of 0-1 range"
    assert gdf["vuln_score"].between(0, 1).all(),   "vuln_score out of 0-1 range"
    assert gdf["combined_score"].between(0, 1).all(), "combined_score out of 0-1 range"


def test_risk_tiers_valid():
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    valid_tiers = {"CRITICAL", "HIGH", "MODERATE", "LOW"}
    assert set(gdf["risk_tier"].unique()).issubset(valid_tiers)


def test_no_null_geometries():
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    assert gdf.geometry.notna().all(), "Null geometries found"
    assert (~gdf.geometry.is_empty).all(), "Empty geometries found"


def test_critical_tier_exists():
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    critical = gdf[gdf["risk_tier"] == "CRITICAL"]
    assert len(critical) > 0, "No CRITICAL tier results — check scoring weights"
    print(f"  {len(critical)} CRITICAL tier tribal lands identified")


def test_score_variance():
    """Scores should have meaningful variance, not all clumped."""
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    assert gdf["corp_score"].std() > 0.05, "corp_score has very low variance"
    assert gdf["vuln_score"].std() > 0.05, "vuln_score has very low variance"
```

---

## Data Sources Reference

| Layer | Source | URL | License |
|---|---|---|---|
| Tribal Boundaries | Census TIGER AIANNH | census.gov/geo/maps-data | Public Domain |
| Transmission Lines | HIFLD / EIA | hifld-geoplatform.opendata.arcgis.com | Public Domain |
| Substations | HIFLD | hifld-geoplatform.opendata.arcgis.com | Public Domain |
| Water Bodies | USGS NHD | nhd.usgs.gov | Public Domain |
| Aquifers | USGS | water.usgs.gov | Public Domain |
| Flood Zones | FEMA NFHL | msc.fema.gov | Public Domain |
| Opportunity Zones | IRS / CDFI Fund | cdfifund.gov | Public Domain |
| Environmental Justice | EPA EJScreen | epa.gov/ejscreen | Public Domain |
| Poverty / Income | Census ACS | api.census.gov | Public Domain |
| Fiber Infrastructure | FCC NBM | broadbandmap.fcc.gov | Public Domain |
| Urban Areas | Census TIGER | census.gov | Public Domain |
| Superfund Sites | EPA | epa.gov | Public Domain |
| Known Data Centers | Honor the Earth | honorearth.org/datacentertracker | Crowdsource |

---

## Next Steps (Mapbox Frontend)

Once `output/tribal_datacenter_risk.geojson` is generated:

1. Upload to Mapbox Tileset or serve as flat file from Cloudflare Pages
2. Build Mapbox GL JS map with:
   - Choropleth fill by `risk_tier` (CRITICAL=red, HIGH=orange, MODERATE=yellow, LOW=gray)
   - Circle layer for `known_sites.geojson` (confirmed Honor the Earth sites)
   - Popup on click showing all score components
   - Filter controls: Risk tier / State / Known vs Predicted
   - Stats panel using `stats.json` (total critical, known sites count)
3. GitHub Actions workflow to re-run pipeline monthly (data sources update)
4. Deploy to Cloudflare Pages

---

## Contributing New Sites

If you know of a data center project near tribal land:
1. Open an issue with location, company, and status
2. Or submit via Honor the Earth's existing form: honorearth.org/datacentertracker
3. Export their Google Sheet → `data/raw/honor_earth_tracker.csv` → re-run pipeline

---

*Built with public federal data. All scoring weights are adjustable. This tool is designed
to support community organizing and sovereignty — not to replace on-the-ground knowledge.*
