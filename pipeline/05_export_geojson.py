"""
Exports final scored data as optimized GeoJSON for Mapbox GL JS / Cloudflare Pages.

Outputs:
  output/tribal_datacenter_risk.geojson     — full polygons (webapp-ready)
  output/tribal_datacenter_risk_pts.geojson — centroid points (fast load)
  output/known_sites.geojson                — existing data center sites
  output/stats.json                         — summary counts for dashboard cards
"""

import geopandas as gpd
import pandas as pd
import json
from pathlib import Path

PROC   = Path("data/processed")
OUTPUT = Path("output")
OUTPUT.mkdir(exist_ok=True)

OUTPUT_CRS = "EPSG:4326"

EXPORT_COLS = [
    "geoid", "tribe_name", "tribe_name_full",
    "area_km2", "state_fips",
    "corp_score", "siting_score", "combined_score",
    "risk_tier", "priority",
    "score_transmission", "score_substation", "score_water",
    "score_aquifer", "score_land_area", "score_terrain",
    "score_fiber_proximity", "score_power_cost",
    "score_flood_penalty", "score_opp_zone",
    "known_datacenter", "known_dc_status", "known_dc_company",
    "geometry"
]


def round_scores(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    score_cols = [c for c in gdf.columns if c.startswith("score_") or c.endswith("_score")]
    for col in score_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].round(3)
    return gdf


def simplify_geometries(gdf: gpd.GeoDataFrame, tolerance_m: float = 500) -> gpd.GeoDataFrame:
    working_crs = "EPSG:5070"
    gdf = gdf.to_crs(working_crs)
    gdf["geometry"] = gdf.geometry.simplify(tolerance_m, preserve_topology=True)
    gdf = gdf.to_crs("EPSG:4326")
    return gdf


def main():
    print("Loading full scored dataset...")
    gdf = gpd.read_file(PROC / "tribal_datacenter_risk_full.gpkg")
    print(f"  {len(gdf)} features")

    export_cols_present = [c for c in EXPORT_COLS if c in gdf.columns]
    gdf = gdf[export_cols_present].copy()

    gdf = round_scores(gdf)

    print("Simplifying geometries...")
    gdf_simplified = simplify_geometries(gdf.copy())
    gdf_simplified = gdf_simplified.to_crs(OUTPUT_CRS)

    poly_out = OUTPUT / "tribal_datacenter_risk.geojson"
    gdf_simplified.to_file(poly_out, driver="GeoJSON")
    size_mb = poly_out.stat().st_size / 1_000_000
    print(f"  Polygon GeoJSON: {poly_out} ({size_mb:.1f} MB)")

    gdf_pts = gdf.copy().to_crs(OUTPUT_CRS)
    gdf_pts["geometry"] = gdf_pts.geometry.centroid
    pts_out = OUTPUT / "tribal_datacenter_risk_pts.geojson"
    gdf_pts.to_file(pts_out, driver="GeoJSON")
    pts_mb = pts_out.stat().st_size / 1_000_000
    print(f"  Points GeoJSON:  {pts_out} ({pts_mb:.1f} MB)")

    if "known_datacenter" in gdf.columns:
        known = gdf[gdf["known_datacenter"] == True].copy().to_crs(OUTPUT_CRS)
        known["geometry"] = known.geometry.centroid
        known_out = OUTPUT / "known_sites.geojson"
        known.to_file(known_out, driver="GeoJSON")
        print(f"  Known sites:     {known_out} ({len(known)} features)")

    print("\nOptional: Generate PMTiles for high-performance Mapbox rendering")
    print("  Run: tippecanoe -o output/tribal_risk.pmtiles \\")
    print("         --minimum-zoom=3 --maximum-zoom=12 \\")
    print("         --layer=tribal_risk \\")
    print("         output/tribal_datacenter_risk.geojson")

    gdf_stats = gpd.read_file(PROC / "tribal_datacenter_risk_full.gpkg")
    stats = {
        "total_tribal_lands": int(len(gdf_stats)),
        "critical_count":     int((gdf_stats["risk_tier"] == "CRITICAL").sum()),
        "high_count":         int((gdf_stats["risk_tier"] == "HIGH").sum()),
        "moderate_count":     int((gdf_stats["risk_tier"] == "MODERATE").sum()),
        "known_sites":        int(gdf_stats["known_datacenter"].sum()) if "known_datacenter" in gdf_stats.columns else 0,
        "total_area_km2":     float(gdf_stats["area_km2"].sum().round(0)) if "area_km2" in gdf_stats.columns else 0,
        "top_prime": gdf_stats[gdf_stats["risk_tier"] == "CRITICAL"].nlargest(5, "corp_score")[
            ["tribe_name", "corp_score", "area_km2"]
        ].round(3).to_dict("records")
    }

    stats_out = OUTPUT / "stats.json"
    stats_out.write_text(json.dumps(stats, indent=2))
    print(f"\nStats JSON: {stats_out}")
    print(json.dumps(stats, indent=2))

    print("\n[done] Export complete.")


if __name__ == "__main__":
    main()
