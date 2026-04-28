"""
pipeline/06_download_overlays.py
Downloads national infrastructure overlay datasets for the Site Unseen webapp.

Sources:
  - HIFLD (confirmed in catalog):  transmission_lines, wind_turbines
  - OpenStreetMap Overpass API:    power_plants, substations, gas_pipelines,
                                   fiber_optic, railways, highways
  - Hardcoded:                     ixp_locations (major US internet exchanges)

Run after: pipeline/00_download_data.py
Output: data/raw/overlays/{layer}.geojson

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


def fetch_fiber_overpass() -> dict:
    """Long-haul fiber optic backbone cables from OSM."""
    print(f"\n[overpass] fiber_optic")
    query = f"""
[out:json][timeout:240][bbox:{US_BBOX_OVERPASS}];
way["telecom"="cable"];
out geom tags;
"""
    elements = _overpass_post(query, "US fiber backbone cables")
    print(f"  Raw elements: {len(elements)}")
    fc = _elements_to_geojson(elements)
    for feat in fc["features"]:
        p = feat["properties"]
        p.setdefault("OWNER", p.get("operator", p.get("owner", "")))
        p.setdefault("CABLE_TYPE", p.get("telecom", "fiber"))
    print(f"  Features: {len(fc['features'])}")
    return fc


def fetch_railways_overpass() -> dict:
    """Main line and light rail from OSM."""
    print(f"\n[overpass] railways")
    query = f"""
[out:json][timeout:240][bbox:{US_BBOX_OVERPASS}];
way["railway"~"^(rail|light_rail)$"];
out geom tags;
"""
    elements = _overpass_post(query, "US railways")
    print(f"  Raw elements: {len(elements)}")
    fc = _elements_to_geojson(elements)
    for feat in fc["features"]:
        p = feat["properties"]
        p.setdefault("OPERATOR", p.get("operator", ""))
        p.setdefault("TYPE", p.get("railway", "rail"))
    print(f"  Features: {len(fc['features'])}")
    return fc


def fetch_highways_overpass() -> dict:
    """Interstate highways (motorways) from OSM."""
    print(f"\n[overpass] highways")
    query = f"""
[out:json][timeout:240][bbox:{US_BBOX_OVERPASS}];
way["highway"="motorway"];
out geom tags;
"""
    elements = _overpass_post(query, "US motorways")
    print(f"  Raw elements: {len(elements)}")
    fc = _elements_to_geojson(elements)
    for feat in fc["features"]:
        p = feat["properties"]
        p.setdefault("REF", p.get("ref", ""))
        p.setdefault("NAME", p.get("name", ""))
    print(f"  Features: {len(fc['features'])}")
    return fc


def create_ixp_geojson() -> dict:
    """Major US internet exchange point colocation campuses (hardcoded)."""
    print(f"\n[hardcoded] ixp_locations")
    IXP_LOCATIONS = [
        {"name": "Equinix Ashburn (IAD)",   "city": "Ashburn",     "state": "VA", "lon": -77.487,  "lat": 39.043},
        {"name": "DE-CIX New York",          "city": "New York",    "state": "NY", "lon": -74.006,  "lat": 40.713},
        {"name": "Equinix Chicago (CH)",     "city": "Chicago",     "state": "IL", "lon": -87.629,  "lat": 41.878},
        {"name": "Equinix Dallas (DA)",      "city": "Dallas",      "state": "TX", "lon": -96.797,  "lat": 32.776},
        {"name": "Equinix San Jose (SV)",    "city": "San Jose",    "state": "CA", "lon": -121.886, "lat": 37.338},
        {"name": "Equinix Los Angeles (LA)", "city": "Los Angeles", "state": "CA", "lon": -118.244, "lat": 34.052},
        {"name": "Equinix Atlanta (AT)",     "city": "Atlanta",     "state": "GA", "lon": -84.388,  "lat": 33.749},
        {"name": "Equinix Seattle (SE)",     "city": "Seattle",     "state": "WA", "lon": -122.335, "lat": 47.608},
        {"name": "CoreSite Denver (DE1)",    "city": "Denver",      "state": "CO", "lon": -104.990, "lat": 39.739},
        {"name": "Equinix Miami (MI)",       "city": "Miami",       "state": "FL", "lon": -80.197,  "lat": 25.775},
        {"name": "Equinix Boston (BO)",      "city": "Boston",      "state": "MA", "lon": -71.059,  "lat": 42.360},
        {"name": "PhoenixNAP",               "city": "Phoenix",     "state": "AZ", "lon": -112.074, "lat": 33.449},
        {"name": "Corelink Minneapolis",     "city": "Minneapolis", "state": "MN", "lon": -93.265,  "lat": 44.977},
        {"name": "Equinix Portland (PD)",    "city": "Portland",    "state": "OR", "lon": -122.676, "lat": 45.523},
    ]
    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [ixp["lon"], ixp["lat"]]},
            "properties": {k: v for k, v in ixp.items() if k not in ("lon", "lat")},
        }
        for ixp in IXP_LOCATIONS
    ]
    print(f"  Features: {len(features)}")
    return {"type": "FeatureCollection", "features": features}


# ── Download dispatch ─────────────────────────────────────────────────────────

LAYER_FETCHERS = {
    "transmission_lines": lambda: fetch_hifld("transmission_lines", HIFLD_SOURCES["transmission_lines"]),
    "wind_turbines":      lambda: fetch_hifld("wind_turbines",      HIFLD_SOURCES["wind_turbines"]),
    "power_plants":       fetch_power_plants_overpass,
    "substations":        fetch_substations_overpass,
    "gas_pipelines":      fetch_gas_pipelines_overpass,
    "fiber_optic":        fetch_fiber_overpass,
    "railways":           fetch_railways_overpass,
    "highways":           fetch_highways_overpass,
    "ixp_locations":      create_ixp_geojson,
}

DEFAULT_LAYERS = [
    "transmission_lines", "wind_turbines", "power_plants", "substations", "gas_pipelines",
    "fiber_optic", "railways", "highways", "ixp_locations",
]


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
