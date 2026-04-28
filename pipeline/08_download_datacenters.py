"""
pipeline/08_download_datacenters.py
Fetches US data center locations from OpenStreetMap (Overpass API).

Queries multiple OSM tag combinations:
  - building=data_center (OSM wiki standard)
  - telecom=data_center
  - building=server_farm
  - name~"data cent" (named facilities with power infrastructure)

Also queries major hyperscaler operators by name to catch facilities not
explicitly tagged as data centers.

Output: output/known_sites.geojson + webapp/data/overlays/known_sites.geojson

Usage:
  python pipeline/08_download_datacenters.py
"""

import json
import time
from pathlib import Path

import requests

OUTPUT = Path("output")
WEBAPP_OVERLAYS = Path("webapp/data/overlays")
OUTPUT.mkdir(parents=True, exist_ok=True)
WEBAPP_OVERLAYS.mkdir(parents=True, exist_ok=True)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
TIMEOUT = 240

# Overpass: south,west,north,east
US_BBOX = "18,-179.5,72,-66"

# Canonical company name normalization
COMPANY_NORM = {
    "amazon": "Amazon AWS",
    "amazon web services": "Amazon AWS",
    "aws": "Amazon AWS",
    "google": "Google",
    "google llc": "Google",
    "alphabet": "Google",
    "microsoft": "Microsoft Azure",
    "microsoft corporation": "Microsoft Azure",
    "azure": "Microsoft Azure",
    "meta": "Meta",
    "meta platforms": "Meta",
    "facebook": "Meta",
    "apple": "Apple",
    "apple inc": "Apple",
    "oracle": "Oracle",
    "oracle corporation": "Oracle",
    "ibm": "IBM",
    "equinix": "Equinix",
    "digital realty": "Digital Realty",
    "cyrusone": "CyrusOne",
    "iron mountain": "Iron Mountain",
    "qts": "QTS Realty",
    "qts realty": "QTS Realty",
    "coresite": "CoreSite",
    "switch": "Switch",
    "vantage data centers": "Vantage",
    "vantage": "Vantage",
    "edgeconnex": "EdgeConneX",
    "cologix": "Cologix",
    "databank": "DataBank",
    "ntt": "NTT",
    "ntt communications": "NTT",
    "lumen": "Lumen",
    "zayo": "Zayo",
    "flexential": "Flexential",
    "aligned data centers": "Aligned Energy",
    "aligned energy": "Aligned Energy",
    "compass datacenters": "Compass Datacenters",
    "compass data centers": "Compass Datacenters",
    "cyxtera": "Cyxtera",
    "stream data centers": "Stream Data Centers",
    "t5 data centers": "T5 Data Centers",
    "tract": "Tract",
    "involta": "Involta",
    "tierpoint": "TierPoint",
    "datasite": "DataSite",
}

# Major hyperscaler operators — query these specifically by operator tag
# with building+power to reduce false positives
HYPERSCALER_OPERATORS = [
    "Amazon", "Amazon Web Services", "AWS",
    "Google", "Microsoft", "Meta", "Apple",
    "Oracle", "Equinix", "Digital Realty",
    "CyrusOne", "Iron Mountain", "QTS",
    "CoreSite", "Switch", "Vantage",
    "EdgeConneX", "Cologix", "DataBank",
]


def _post_overpass(query: str, label: str) -> list:
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
        print(f"  [error] {label}: {e}")
        return []


def fetch_explicit_datacenter_tags() -> list:
    """Query OSM for all explicit data center building/telecom tags."""
    query = f"""
[out:json][timeout:240][bbox:{US_BBOX}];
(
  node["building"="data_center"];
  way["building"="data_center"];
  node["telecom"="data_center"];
  way["telecom"="data_center"];
  node["building"="server_farm"];
  way["building"="server_farm"];
  node["man_made"="data_center"];
  way["man_made"="data_center"];
);
out center tags;
"""
    elements = _post_overpass(query, "explicit data_center tags")
    print(f"  Raw elements: {len(elements)}")
    return elements


def fetch_named_datacenters() -> list:
    """Query OSM for facilities with 'data center' in name that also have power infrastructure."""
    query = f"""
[out:json][timeout:240][bbox:{US_BBOX}];
(
  way["name"~"[Dd]ata [Cc]ent(er|re)"]["building"];
  node["name"~"[Dd]ata [Cc]ent(er|re)"]["building"];
  way["name"~"[Dd]ata [Cc]ent(er|re)"]["amenity"];
  way["name"~"[Ss]erver [Ff]arm"]["building"];
  way["name"~"[Cc]olocation"]["building"];
  way["name"~"[Cc]olo(?: [Ff]acility)?"]["building"];
);
out center tags;
"""
    elements = _post_overpass(query, "named data center facilities")
    print(f"  Raw elements: {len(elements)}")
    return elements


def fetch_hyperscaler_campuses() -> list:
    """Query OSM for known hyperscaler operators with data center infrastructure."""
    ops = "|".join(HYPERSCALER_OPERATORS)
    query = f"""
[out:json][timeout:240][bbox:{US_BBOX}];
(
  way["operator"~"{ops}",i]["building"]["power"];
  way["operator"~"{ops}",i]["building"="data_center"];
  way["operator"~"{ops}",i]["telecom"="data_center"];
  node["operator"~"{ops}",i]["building"="data_center"];
  node["operator"~"{ops}",i]["telecom"="data_center"];
);
out center tags;
"""
    elements = _post_overpass(query, "hyperscaler operator campuses")
    print(f"  Raw elements: {len(elements)}")
    return elements


def _normalize_company(raw: str) -> str:
    if not raw:
        return ""
    key = raw.strip().lower().rstrip(".")
    return COMPANY_NORM.get(key, raw.strip())


def _parse_status(tags: dict) -> str:
    os = tags.get("operational_status", tags.get("status", "")).lower()
    if "construct" in os or "building" in os:
        return "under_construction"
    if "planned" in os or "propos" in os:
        return "planned"
    if "closed" in os or "decommiss" in os or "abandon" in os:
        return "closed"
    # Check opening_date for future facilities
    od = tags.get("opening_date", tags.get("start_date", ""))
    if od and od > "2025":
        return "under_construction"
    return "operational"


def elements_to_features(elements: list) -> list:
    seen_ids = set()
    features = []
    for el in elements:
        uid = f"{el['type']}/{el['id']}"
        if uid in seen_ids:
            continue
        seen_ids.add(uid)

        tags = el.get("tags", {})

        # Get coordinates
        if el["type"] == "node" and "lat" in el:
            lon, lat = el["lon"], el["lat"]
        elif "center" in el:
            lon, lat = el["center"]["lon"], el["center"]["lat"]
        else:
            continue

        company_raw = tags.get("operator", tags.get("brand", tags.get("owner", "")))
        name = tags.get("name", "")

        # Infer company from name if operator missing
        if not company_raw and name:
            for key, norm in COMPANY_NORM.items():
                if key in name.lower():
                    company_raw = norm
                    break

        company = _normalize_company(company_raw)

        # Power/capacity
        mw_raw = tags.get("power", tags.get("plant:output:electricity", ""))
        mw = None
        if mw_raw:
            try:
                mw_val = float(str(mw_raw).replace(",", "").replace(" MW", "").replace("MW", "").split()[0])
                mw = round(mw_val)
            except (ValueError, IndexError):
                pass

        props = {
            "name": name,
            "company": company,
            "status": _parse_status(tags),
            "source": "OpenStreetMap",
            "osm_id": el["id"],
            "osm_type": el["type"],
        }
        if mw:
            props["mw"] = mw

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })

    return features


def deduplicate(features: list, min_dist_deg: float = 0.002) -> list:
    """Remove near-duplicate points (within ~200m)."""
    kept = []
    coords_kept = []
    for feat in features:
        lon, lat = feat["geometry"]["coordinates"]
        duplicate = False
        for klon, klat in coords_kept:
            if abs(lon - klon) < min_dist_deg and abs(lat - klat) < min_dist_deg:
                duplicate = True
                break
        if not duplicate:
            kept.append(feat)
            coords_kept.append((lon, lat))
    return kept


def main() -> None:
    print("[datacenters] Fetching US data center locations from OSM...")

    all_elements = []

    elements1 = fetch_explicit_datacenter_tags()
    all_elements.extend(elements1)
    time.sleep(2)

    elements2 = fetch_named_datacenters()
    all_elements.extend(elements2)
    time.sleep(2)

    elements3 = fetch_hyperscaler_campuses()
    all_elements.extend(elements3)

    print(f"\n[datacenters] Total raw elements: {len(all_elements)}")

    features = elements_to_features(all_elements)
    print(f"[datacenters] Valid features: {len(features)}")

    features = deduplicate(features)
    print(f"[datacenters] After dedup: {len(features)}")

    # Sort by company for readability
    features.sort(key=lambda f: (f["properties"].get("company") or "~", f["properties"].get("name") or ""))

    fc = {"type": "FeatureCollection", "features": features}

    out_path = OUTPUT / "known_sites.geojson"
    out_path.write_text(json.dumps(fc, indent=None))
    size_kb = out_path.stat().st_size / 1024
    print(f"\n[datacenters] Saved → {out_path} ({len(features)} sites, {size_kb:.0f} KB)")

    # Also write directly to webapp overlays
    webapp_path = WEBAPP_OVERLAYS / "known_sites.geojson"
    webapp_path.write_text(json.dumps(fc, indent=None))
    print(f"[datacenters] Saved → {webapp_path}")

    # Print company breakdown
    from collections import Counter
    companies = Counter(
        f["properties"].get("company") or "unattributed"
        for f in features
    )
    print("\n[datacenters] Top companies:")
    for co, cnt in companies.most_common(20):
        print(f"  {co:35s} {cnt}")


if __name__ == "__main__":
    main()
