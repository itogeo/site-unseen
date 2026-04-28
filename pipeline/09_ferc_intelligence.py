"""
pipeline/09_ferc_intelligence.py
Pulls planned / under-construction grid projects near tribal lands from the
EIA Form 860M Monthly Generator Inventory.

Large planned generators (>50 MW) within 200 km of tribal lands are the
clearest public-data signal of imminent grid expansion for data-center loads.

Sources:
  EIA Form 860M  https://www.eia.gov/electricity/data/eia860m/

Output:
  webapp/data/overlays/ferc_flags.geojson
  output/ferc_flags.geojson

Usage:
  python pipeline/09_ferc_intelligence.py
"""

import io
import json
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point
from shapely.ops import unary_union

OUTPUT       = Path("output")
WEBAPP       = Path("webapp/data/overlays")
PROCESSED    = Path("data/processed")
OUTPUT.mkdir(parents=True, exist_ok=True)
WEBAPP.mkdir(parents=True, exist_ok=True)

MIN_MW           = 50     # only projects this large matter for data centers
BUFFER_KM        = 200
HEADERS          = {"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"}

# EIA status codes → human-readable
STATUS_MAP = {
    "P":  "Proposed",
    "L":  "Regulatory approval pending",
    "T":  "Regulatory approval pending",
    "U":  "Under construction",
    "V":  "Under construction (planned retirement)",
    "TS": "Construction started, less than half complete",
    "OS": "Out of service",
    "OA": "Out of service (planned)",
}

# Energy sources that could serve data-center loads (all large thermal/solar)
DC_RELEVANT_SOURCES = {
    "NG",   # natural gas
    "SUN",  # solar
    "NUC",  # nuclear
    "WAT",  # hydro
    "WND",  # wind (large wind farms)
    "GEO",  # geothermal
    "BIT", "SUB", "LIG",  # coal (retiring, but still signals grid nodes)
    "OTH", "DFO", "RFO",  # other fuels
    "MWH",  # battery storage (co-located with data centers)
}


# ── EIA 860M download ─────────────────────────────────────────────────────────

def get_eia860m_url() -> str:
    """Scrape EIA landing page and return the first xlsx URL that is a real Excel file."""
    page = requests.get(
        "https://www.eia.gov/electricity/data/eia860m/",
        headers=HEADERS, timeout=30,
    )
    page.raise_for_status()
    links = re.findall(r'href="(/electricity/data/eia860m/xls/[^"]+\.xlsx)"', page.text)
    if not links:
        raise RuntimeError("Could not find EIA 860M Excel link on landing page")

    # EIA sometimes lists future months that return HTML 404 pages.
    # Try each link until we find one with valid xlsx magic bytes (PK header).
    for rel in links:
        url = "https://www.eia.gov" + rel
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200 and r.content[:4] == b"PK\x03\x04":
                print(f"[ferc] Valid EIA file: {rel.split('/')[-1]} ({len(r.content)/1_048_576:.1f} MB)")
                return url, r.content
        except Exception:
            pass

    raise RuntimeError("No valid EIA 860M xlsx found — check landing page links")


def download_planned_generators() -> pd.DataFrame:
    url, content = get_eia860m_url()
    print(f"[ferc] Parsing {url.split('/')[-1]} ...")

    xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    print(f"  Worksheets: {xl.sheet_names}")

    # Read "Planned" sheet (skip EIA header rows — usually 1 or 2 blank/title rows)
    for skip in (1, 2, 0):
        try:
            df = xl.parse("Planned", header=skip)
            # Check that we got real column headers
            if "Plant Name" in df.columns or "Plant ID" in df.columns:
                break
        except Exception:
            continue
    else:
        df = xl.parse("Planned", header=1)

    print(f"  Planned rows: {len(df)}, columns: {list(df.columns[:8])}")
    return df


# ── Tribal buffer ─────────────────────────────────────────────────────────────

def load_tribal_buffer() -> object:
    gpkg = PROCESSED / "tribal_lands.gpkg"
    if not gpkg.exists():
        raise FileNotFoundError(f"{gpkg} not found — run pipeline first")
    gdf = gpd.read_file(gpkg).to_crs("EPSG:5070")
    union_geom = unary_union(gdf.geometry)
    return union_geom.buffer(BUFFER_KM * 1000)


# ── Parse + filter ────────────────────────────────────────────────────────────

def _col(df: pd.DataFrame, *candidates: str) -> str:
    """Return the first column name that exists (case-insensitive)."""
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        hit = lower.get(cand.lower())
        if hit:
            return hit
    return ""


def build_features(df: pd.DataFrame, buffer_geom) -> list:
    # Normalize column names
    col_mw     = _col(df, "Nameplate Capacity (MW)", "Nameplate_Capacity_MW", "Nameplate Capacity")
    col_lat    = _col(df, "Latitude", "LAT")
    col_lon    = _col(df, "Longitude", "LON")
    col_name   = _col(df, "Plant Name", "Plant_Name")
    col_entity = _col(df, "Entity Name", "Entity_Name", "Utility Name")
    col_status = _col(df, "Status")
    col_state  = _col(df, "Plant State", "State")
    col_county = _col(df, "County")
    col_tech   = _col(df, "Technology")
    col_source = _col(df, "Energy Source Code", "Energy Source 1")

    if not col_mw:
        print("  [warn] Cannot find MW column — check worksheet structure")
        return []

    df[col_mw] = pd.to_numeric(df[col_mw], errors="coerce")
    df = df[df[col_mw] >= MIN_MW].copy()
    print(f"  After >{MIN_MW} MW filter: {len(df)} rows")

    if col_lat and col_lon:
        df[col_lat] = pd.to_numeric(df[col_lat], errors="coerce")
        df[col_lon] = pd.to_numeric(df[col_lon], errors="coerce")
        df = df.dropna(subset=[col_lat, col_lon])
        df = df[df[col_lat].between(17, 72) & df[col_lon].between(-180, -60)]
        print(f"  After valid coords filter: {len(df)} rows")
    else:
        print("  [warn] No Latitude/Longitude columns — cannot place on map")
        return []

    # Spatial filter: within tribal buffer
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(row[col_lon], row[col_lat]) for _, row in df.iterrows()],
        crs="EPSG:4326",
    ).to_crs("EPSG:5070")

    in_buffer = gdf[gdf.geometry.within(buffer_geom)].copy()
    print(f"  After tribal buffer filter: {len(in_buffer)} rows")

    features = []
    for _, row in in_buffer.to_crs("EPSG:4326").iterrows():
        status_code = str(row.get(col_status, "P")).strip()
        status_label = STATUS_MAP.get(status_code, status_code)
        mw_val = float(row[col_mw]) if pd.notna(row[col_mw]) else 0

        props = {
            "project_name": str(row.get(col_name, "") or "").strip() or None,
            "applicant":    str(row.get(col_entity, "") or "").strip() or None,
            "mw":           round(mw_val, 1),
            "status":       status_label,
            "state":        str(row.get(col_state, "") or "").strip(),
            "county":       str(row.get(col_county, "") or "").strip(),
            "technology":   str(row.get(col_tech, "") or "").strip(),
            "source":       "EIA Form 860M",
        }

        geom = row.geometry
        features.append({
            "type":       "Feature",
            "geometry":   {"type": "Point", "coordinates": [geom.x, geom.y]},
            "properties": props,
        })

    return features


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("[ferc] Loading tribal buffer...")
    buffer = load_tribal_buffer()

    df = download_planned_generators()
    features = build_features(df, buffer)

    fc = {"type": "FeatureCollection", "features": features}

    for path in (OUTPUT / "ferc_flags.geojson", WEBAPP / "ferc_flags.geojson"):
        path.write_text(json.dumps(fc))
        print(f"[ferc] Saved → {path} ({len(features)} flags, {path.stat().st_size/1024:.0f} KB)")

    # Summary by state
    from collections import Counter
    states = Counter(f["properties"]["state"] for f in features)
    print("\n[ferc] Flags by state:")
    for st, n in states.most_common(20):
        print(f"  {st:4s} {n}")

    print("\n[ferc] Done. Next: refresh localhost:8765 to see FERC flags layer.")


if __name__ == "__main__":
    main()
