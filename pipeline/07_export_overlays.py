"""
pipeline/07_export_overlays.py
Clips national overlay datasets to a 200km buffer around all tribal lands,
simplifies geometry for web performance, and exports to webapp/data/overlays/.

Run after: pipeline/06_download_overlays.py
Output: webapp/data/overlays/{power_plants,gas_pipelines,transmission_lines,substations}.geojson

Each output file is ≤5MB and ready for direct browser loading.
"""

import json
from pathlib import Path

import geopandas as gpd
from shapely.ops import unary_union

PROCESSED = Path("data/processed")
RAW_OVERLAYS = Path("data/raw/overlays")
WEBAPP_OVERLAYS = Path("webapp/data/overlays")
WEBAPP_OVERLAYS.mkdir(parents=True, exist_ok=True)

BUFFER_KM = 200        # clip everything to 200km around tribal lands
SIMPLIFY_DEG = 0.005   # ~500m tolerance for lines/polygons; skip for points


def load_tribal_buffer() -> gpd.GeoDataFrame:
    gpkg = PROCESSED / "tribal_lands.gpkg"
    if not gpkg.exists():
        raise FileNotFoundError(f"{gpkg} not found — run pipeline first")
    gdf = gpd.read_file(gpkg).to_crs("EPSG:5070")
    union_geom = unary_union(gdf.geometry)
    buffered = union_geom.buffer(BUFFER_KM * 1000)
    return gpd.GeoDataFrame(geometry=[buffered], crs="EPSG:5070").to_crs("EPSG:4326")


def clip_and_export(name: str, buffer_gdf: gpd.GeoDataFrame, simplify: bool = True) -> None:
    src = RAW_OVERLAYS / f"{name}.geojson"
    dst = WEBAPP_OVERLAYS / f"{name}.geojson"

    if not src.exists():
        print(f"[overlays] {name}: source not found — run 06_download_overlays.py first")
        return

    print(f"\n[overlays] Processing {name}...")
    gdf = gpd.read_file(src)
    print(f"  Raw: {len(gdf)} features")

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    # Clip to tribal buffer
    buffer_union = buffer_gdf.geometry.iloc[0]
    gdf = gdf[gdf.geometry.intersects(buffer_union)].copy()
    print(f"  After clip: {len(gdf)} features")

    if gdf.empty:
        print(f"  [warn] No features in tribal buffer — skipping")
        return

    # Simplify line/polygon geometry for web perf
    if simplify and gdf.geometry.geom_type.isin(["LineString", "MultiLineString", "Polygon", "MultiPolygon"]).any():
        gdf = gdf.to_crs("EPSG:5070")
        gdf["geometry"] = gdf.geometry.simplify(tolerance=SIMPLIFY_DEG * 111_000)
        gdf = gdf.to_crs("EPSG:4326")

    # Drop null geometries
    gdf = gdf[~gdf.geometry.isna()].copy()

    # Keep only essential columns to minimize file size
    keep_cols = _get_keep_cols(name, gdf.columns.tolist())
    gdf = gdf[keep_cols + ["geometry"]]

    gdf.to_file(dst, driver="GeoJSON")
    size_kb = dst.stat().st_size / 1024
    print(f"  Saved → {dst} ({len(gdf)} features, {size_kb:.0f} KB)")


def _get_keep_cols(name: str, available: list) -> list:
    """Return a minimal set of columns relevant to each layer."""
    wanted = {
        "power_plants":       ["NAME", "TYPE", "TOTAL_MW", "STATE", "STATUS"],
        "substations":        ["NAME", "TYPE", "MAX_VOLT", "STATE", "STATUS"],
        "transmission_lines": ["VOLTAGE", "TYPE", "STATUS", "OWNER"],
        "gas_pipelines":      ["Operator", "Type"],
        "wind_turbines":      ["p_name", "t_state", "t_county", "t_cap", "p_year"],
    }
    desired = wanted.get(name, [])
    return [c for c in desired if c in available]


def export_known_sites() -> None:
    """Copy known_sites.geojson from output/ to webapp/data/overlays/ (already in webapp/data/)."""
    src = Path("output/known_sites.geojson")
    if not src.exists():
        src = Path("data/known_sites.geojson")
    if not src.exists():
        print("[overlays] known_sites.geojson not found — skipping")
        return
    dst = WEBAPP_OVERLAYS / "known_sites.geojson"
    dst.write_text(src.read_text())
    size_kb = dst.stat().st_size / 1024
    print(f"\n[overlays] known_sites → {dst} ({size_kb:.0f} KB)")


def main() -> None:
    print("[overlays] Clipping + exporting overlay layers for webapp...")

    print("[overlays] Loading tribal buffer...")
    buffer_gdf = load_tribal_buffer()
    print(f"  Buffer bounds: {buffer_gdf.total_bounds.round(2)}")

    clip_and_export("power_plants",        buffer_gdf, simplify=False)  # points
    clip_and_export("substations",         buffer_gdf, simplify=False)  # points
    clip_and_export("transmission_lines",  buffer_gdf, simplify=True)
    clip_and_export("gas_pipelines",       buffer_gdf, simplify=True)
    clip_and_export("wind_turbines",       buffer_gdf, simplify=False)  # points

    export_known_sites()

    print("\n[overlays] Done. Files in webapp/data/overlays/:")
    for f in sorted(WEBAPP_OVERLAYS.glob("*.geojson")):
        print(f"  {f.name}: {f.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
