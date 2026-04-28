"""
pipeline/06_download_overlays.py
Downloads national infrastructure overlay datasets from HIFLD (all free, public).
These power the "Data Layers" panel in the webapp — the physical infrastructure
that drives data center siting decisions.

Run after: pipeline/00_download_data.py (tribal boundaries must be present)
Output: data/raw/overlays/{power_plants,gas_pipelines,transmission_lines,substations}.geojson

Usage:
  export PROJ_DATA=/Users/<user>/anaconda3/envs/geodata/share/proj
  export GDAL_DATA=/Users/<user>/anaconda3/envs/geodata/share/gdal
  python pipeline/06_download_overlays.py
"""

import json
import time
from pathlib import Path

import requests

RAW_OVERLAYS = Path("data/raw/overlays")
RAW_OVERLAYS.mkdir(parents=True, exist_ok=True)

TIMEOUT = 60
PAGE_SIZE = 2000  # HIFLD max per request

# ── HIFLD endpoints ───────────────────────────────────────────────────────────
HIFLD_BASE = "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services"

SOURCES = {
    "power_plants": {
        "url": f"{HIFLD_BASE}/Power_Plants/FeatureServer/0/query",
        "where": "TOTAL_MW >= 50",  # only plants >= 50 MW
        "fields": "NAME,TYPE,TOTAL_MW,STATE,STATUS,LATITUDE,LONGITUDE",
        "description": "Power plants >= 50 MW (EIA/HIFLD)",
    },
    "substations": {
        "url": f"{HIFLD_BASE}/Electric_Substations/FeatureServer/0/query",
        "where": "MAX_VOLT >= 115",  # 115kV+ substations
        "fields": "NAME,TYPE,MAX_VOLT,STATE,STATUS,LATITUDE,LONGITUDE",
        "description": "High-voltage substations ≥115kV (HIFLD)",
    },
    "transmission_lines": {
        "url": f"{HIFLD_BASE}/Electric_Power_Transmission_Lines/FeatureServer/0/query",
        "where": "VOLTAGE >= 115",  # high-voltage backbone only
        "fields": "VOLTAGE,TYPE,STATUS,OWNER,SHAPE_Length",
        "description": "High-voltage transmission lines ≥115kV (HIFLD)",
    },
    "gas_pipelines": {
        "url": f"{HIFLD_BASE}/Natural_Gas_Interstate_and_Intrastate_Pipelines/FeatureServer/0/query",
        "where": "1=1",
        "fields": "Operator,Type,SHAPE_Length",
        "description": "Natural gas pipelines — interstate + intrastate (HIFLD)",
    },
}


def fetch_hifld_paginated(name: str, config: dict) -> dict:
    """
    Fetches a HIFLD FeatureService layer with offset pagination.
    Returns a GeoJSON FeatureCollection.
    """
    print(f"\n[overlays] {name}: {config['description']}")
    features = []
    offset = 0

    while True:
        params = {
            "where": config["where"],
            "outFields": config["fields"],
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "returnGeometry": "true",
        }
        try:
            resp = requests.get(
                config["url"],
                params=params,
                timeout=TIMEOUT,
                headers={"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"},
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [warn] page {offset}: {e}")
            break

        page_features = data.get("features", [])
        features.extend(page_features)
        print(f"  offset {offset}: +{len(page_features)} ({len(features)} total)")

        # HIFLD signals "no more" with exceededTransferLimit=false + empty page
        if len(page_features) < PAGE_SIZE:
            break
        if not data.get("exceededTransferLimit", False) and len(page_features) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.5)

    print(f"  Total: {len(features)} features")
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "source": "HIFLD",
            "description": config["description"],
            "feature_count": len(features),
        },
    }


def main() -> None:
    print("[overlays] Downloading national infrastructure overlay data from HIFLD...")
    print(f"  Output: {RAW_OVERLAYS.resolve()}\n")

    for name, config in SOURCES.items():
        out_path = RAW_OVERLAYS / f"{name}.geojson"
        if out_path.exists():
            existing = json.loads(out_path.read_text())
            n = len(existing.get("features", []))
            print(f"[overlays] {name}: already exists ({n} features) — skipping. Delete to re-download.")
            continue

        geojson = fetch_hifld_paginated(name, config)
        out_path.write_text(json.dumps(geojson))
        print(f"  Saved → {out_path} ({len(geojson['features'])} features)")

    print("\n[overlays] Download complete.")
    print("Next step: python pipeline/07_export_overlays.py")


if __name__ == "__main__":
    main()
