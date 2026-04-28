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
    gdf = gdf.to_crs(WORKING_CRS)

    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()

    rename = {
        "GEOID":    "geoid",
        "NAME":     "tribe_name",
        "NAMELSAD": "tribe_name_full",
        "ALAND":    "land_area_sqm",
        "AWATER":   "water_area_sqm",
        "STATESFP": "state_fips",
    }
    gdf = gdf.rename(columns={k: v for k, v in rename.items() if k in gdf.columns})

    gdf["area_km2"] = gdf.geometry.area / 1_000_000
    gdf["centroid_x"] = gdf.geometry.centroid.x
    gdf["centroid_y"] = gdf.geometry.centroid.y

    # Filter to federally recognized tribal lands (TIGER LSAD codes)
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
