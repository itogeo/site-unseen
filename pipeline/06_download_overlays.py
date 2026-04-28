"""
pipeline/06_download_overlays.py
Downloads national infrastructure overlay datasets for the Site Unseen webapp.

Sources:
  - HIFLD (confirmed in catalog):  transmission_lines, wind_turbines
  - OpenStreetMap Overpass API:    power_plants, substations, gas_pipelines

HIFLD services catalog confirms only Electric_Power_Transmission_Lines and
US_Wind_Turbines are available for US energy infrastructure. Substations and
Power_Plants are not in the HIFLD catalog — Overpass provides reliable fallback.

Run after: pipeline/00_download_data.py
Output: data/raw/overlays/{transmission_lines,substations,power_plants,gas_pipelines,wind_turbines}.geojson

Usage:
  python pipeline/06_download_overlays.py
"""

import json
import time
from pathlib import Path

import requests

RAW_OVERLAYS = Path("data/raw/overlays")
RAW_OVERLAYS.mkdir(parents=True, exist_ok=True)

TIMEOUT = 120
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

HIFLD_BASE = (
    "https://services1.arcgis.com/Hp6G80Pky0om7QvQ"
    "/arcgis/rest/services"
)

# Continental US + AK/HI bounding box (xmin,ymin,xmax,ymax in WGS84)
US_BBOX_GEOM = "-179.5,18.0,-66.0,72.0"

# Overpass uses south,west,north,east
US_BBOX_OVERPASS = "18,-179.5,72,-66"

# HIFLD services confirmed to exist via catalog query
# use_bbox=True: requires spatial filter (line features)
# use_bbox=False: fetch with where clause only (point features — bbox returns 0)
HIFLD_SOURCES = {
    "transmission_lines": {
        "service": "Electric_Power_Transmission_Lines",
        "where": "1=1",
        "fields": "VOLTAGE,TYPE,STATUS,OWNER",
        "description": "High-voltage transmission lines (HIFLD)",
        "use_bbox": True,
    },
    "wind_turbines": {
        "service": "US_Wind_Turbines",
        "where": "1=1",
        "fields": "p_name,t_state,t_county,p_tnum,t_cap,t_hh,t_rd,p_year",
        "description": "US wind turbines (HIFLD/USGS)",
        "use_bbox": False,
    },
}


# ── HIFLD ─────────────────────────────────────────────────────────────────────

def fetch_hifld(name: str, config: dict) -> dict:
    """
    Fetch a HIFLD FeatureServer using f=geojson.
    Line features require the bbox spatial filter; point features do not (bbox returns 0).
    """
    spatial_params = (
        f"&geometry={US_BBOX_GEOM}"
        f"&geometryType=esriGeometryEnvelope"
        f"&inSR=4326"
        f"&spatialRel=esriSpatialRelIntersects"
        if config.get("use_bbox", True) else ""
    )
    base_url = (
        f"{HIFLD_BASE}/{config['service']}/FeatureServer/0/query"
        f"?where={requests.utils.quote(config['where'])}"
        f"&outFields={config['fields']}"
        f"&f=geojson"
        + spatial_params
    )

    print(f"\n[hifld] {name}: {config['description']}")
    features = []
    offset = 0
    page_size = 1000

    while True:
        paged_url = base_url + f"&resultOffset={offset}&resultRecordCount={page_size}"
        try:
            resp = requests.get(
                paged_url, timeout=TIMEOUT,
                headers={"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"},
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [error] offset {offset}: {e}")
            break

        if "error" in data:
            print(f"  [error] HIFLD: {data['error'].get('message', data['error'])}")
            break

        page_feats = data.get("features", [])
        features.extend(page_feats)
        print(f"  offset {offset}: +{len(page_feats)} ({len(features)} total)")

        if len(page_feats) < page_size:
            break

        offset += page_size
        time.sleep(0.4)

    print(f"  Total: {len(features)} features")
    return {"type": "FeatureCollection", "features": features}


# ── Overpass API ──────────────────────────────────────────────────────────────

def _overpass_post(query: str, label: str) -> list:
    """POST an Overpass QL query, return elements list."""
    print(f"  Overpass: {label}...")
    try:
        resp = requests.post(
            OVERPASS_URL,
            data={"data": query},
            timeout=TIMEOUT,
            headers={"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"},
        )
        resp.raise_for_status()
        return resp.json().get("elements", [])
    except Exception as e:
        print(f"  [error] Overpass: {e}")
        return []


def _elements_to_geojson(elements: list) -> dict:
    """Convert Overpass nodes/ways to GeoJSON FeatureCollection."""
    features = []
    for el in elements:
        tags = el.get("tags", {})
        if el["type"] == "node" and "lat" in el:
            geom = {"type": "Point", "coordinates": [el["lon"], el["lat"]]}
        elif el["type"] == "way" and "geometry" in el:
            coords = [[pt["lon"], pt["lat"]] for pt in el["geometry"]]
            geom = {"type": "LineString", "coordinates": coords}
        elif el["type"] == "way" and "center" in el:
            geom = {"type": "Point", "coordinates": [el["center"]["lon"], el["center"]["lat"]]}
        else:
            continue
        features.append({"type": "Feature", "geometry": geom, "properties": dict(tags)})
    return {"type": "FeatureCollection", "features": features}


def fetch_power_plants_overpass() -> dict:
    """Power plants from OSM."""
    print(f"\n[overpass] power_plants")
    query = f"""
[out:json][timeout:180][bbox:{US_BBOX_OVERPASS}];
(
  node["power"="plant"];
  way["power"="plant"];
);
out center tags;
"""
    elements = _overpass_post(query, "US power plants")
    print(f"  Raw elements: {len(elements)}")
    fc = _elements_to_geojson(elements)
    for feat in fc["features"]:
        p = feat["properties"]
        p.setdefault("NAME", p.get("name", p.get("operator", "")))
        p.setdefault("TYPE", p.get("plant:source", p.get("generator:source", "")))
        p.setdefault("TOTAL_MW", p.get("plant:output:electricity", ""))
        p.setdefault("STATUS", p.get("operational_status", ""))
    print(f"  Features: {len(fc['features'])}")
    return fc


def fetch_substations_overpass() -> dict:
    """High-voltage substations from OSM (≥115kV)."""
    print(f"\n[overpass] substations")
    query = f"""
[out:json][timeout:180][bbox:{US_BBOX_OVERPASS}];
(
  node["power"="substation"]["voltage"];
  way["power"="substation"]["voltage"];
);
out center tags;
"""
    elements = _overpass_post(query, "US substations with voltage tag")
    print(f"  Raw elements: {len(elements)}")

    # Filter to ≥115kV
    filtered = []
    for el in elements:
        volt_str = el.get("tags", {}).get("voltage", "0")
        try:
            volts = [float(v) for v in volt_str.replace(" ", "").split(";") if v]
            max_volt = max(volts)
            if max_volt >= 1000:
                max_volt /= 1000  # V → kV
            if max_volt >= 115:
                el["tags"]["MAX_VOLT"] = max_volt
                filtered.append(el)
        except (ValueError, ZeroDivisionError):
            continue

    print(f"  After ≥115kV filter: {len(filtered)}")
    fc = _elements_to_geojson(filtered)
    for feat in fc["features"]:
        p = feat["properties"]
        p.setdefault("NAME", p.get("name", p.get("operator", "")))
        p.setdefault("STATUS", p.get("operational_status", ""))
    print(f"  Features: {len(fc['features'])}")
    return fc


def fetch_gas_pipelines_overpass() -> dict:
    """Natural gas pipelines from OSM."""
    print(f"\n[overpass] gas_pipelines")
    query = f"""
[out:json][timeout:240][bbox:{US_BBOX_OVERPASS}];
way["man_made"="pipeline"]["substance"~"gas|natural_gas|fuel",i];
out geom tags;
"""
    elements = _overpass_post(query, "US natural gas pipelines")
    print(f"  Raw elements: {len(elements)}")
    fc = _elements_to_geojson(elements)
    for feat in fc["features"]:
        p = feat["properties"]
        p.setdefault("Operator", p.get("operator", ""))
        p.setdefault("Type", p.get("substance", "gas"))
    print(f"  Features: {len(fc['features'])}")
    return fc


# ── Download dispatch ─────────────────────────────────────────────────────────

LAYER_FETCHERS = {
    "transmission_lines": lambda: fetch_hifld("transmission_lines", HIFLD_SOURCES["transmission_lines"]),
    "wind_turbines":      lambda: fetch_hifld("wind_turbines",      HIFLD_SOURCES["wind_turbines"]),
    "power_plants":       fetch_power_plants_overpass,
    "substations":        fetch_substations_overpass,
    "gas_pipelines":      fetch_gas_pipelines_overpass,
}

DEFAULT_LAYERS = ["transmission_lines", "wind_turbines", "power_plants", "substations", "gas_pipelines"]


def main() -> None:
    print("[overlays] Downloading national infrastructure overlay data...")
    print(f"  Output: {RAW_OVERLAYS.resolve()}\n")

    for name in DEFAULT_LAYERS:
        out_path = RAW_OVERLAYS / f"{name}.geojson"
        if out_path.exists():
            existing = json.loads(out_path.read_text())
            n = len(existing.get("features", []))
            print(f"[overlays] {name}: already exists ({n} features) — skipping. Delete to re-download.")
            continue

        fc = LAYER_FETCHERS[name]()
        out_path.write_text(json.dumps(fc))
        size_kb = out_path.stat().st_size / 1024
        print(f"  Saved → {out_path} ({len(fc['features'])} features, {size_kb:.0f} KB)\n")

    print("[overlays] Download complete.")
    print("Next step: python pipeline/07_export_overlays.py")


if __name__ == "__main__":
    main()
