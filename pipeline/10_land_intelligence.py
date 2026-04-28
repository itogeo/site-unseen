"""
pipeline/10_land_intelligence.py
Fetches land acquisition intelligence near tribal lands.

Strategy: Search SEC EDGAR full-text for 8-K filings that mention BOTH
"data center" AND a specific tribal-land state name in the filing body.
This surfaces real announcements of corporate data center activity in states
where tribal communities are concentrated.

Also queries USACE Regulatory permit search for large construction projects
in tribal states (proxy for data center site prep).

Output:
  webapp/data/overlays/land_acquisitions.geojson
  output/land_acquisitions.geojson

Usage:
  python pipeline/10_land_intelligence.py
"""

import json
import re
import time
import random
from pathlib import Path

import geopandas as gpd
import requests
from shapely.geometry import Point
from shapely.ops import unary_union

random.seed(42)

OUTPUT    = Path("output")
WEBAPP    = Path("webapp/data/overlays")
PROCESSED = Path("data/processed")
OUTPUT.mkdir(parents=True, exist_ok=True)
WEBAPP.mkdir(parents=True, exist_ok=True)

HEADERS   = {"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"}
BUFFER_KM = 200

EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"

# States with significant tribal land acreage → search these for data center activity
TRIBAL_STATES = {
    "AZ": ("Arizona",       -111.7, 34.3),
    "NM": ("New Mexico",    -106.2, 34.5),
    "OK": ("Oklahoma",      -97.5,  35.6),
    "SD": ("South Dakota",  -100.3, 44.4),
    "ND": ("North Dakota",  -100.5, 47.5),
    "MT": ("Montana",       -109.6, 47.0),
    "MN": ("Minnesota",     -94.3,  46.4),
    "WI": ("Wisconsin",     -89.8,  44.6),
    "WA": ("Washington",    -120.5, 47.4),
    "OR": ("Oregon",        -120.5, 44.0),
    "CA": ("California",    -119.4, 37.2),
    "NV": ("Nevada",        -116.6, 39.5),
    "WY": ("Wyoming",       -107.6, 43.0),
    "ID": ("Idaho",         -114.5, 44.4),
    "UT": ("Utah",          -111.1, 39.3),
    "CO": ("Colorado",      -105.5, 39.0),
    "NE": ("Nebraska",      -99.9,  41.5),
    "TX": ("Texas",         -99.3,  31.4),
    "MI": ("Michigan",      -84.5,  44.3),
    "AK": ("Alaska",        -153.4, 64.2),
}

# Confidence boost for states with the most tribal land
HIGH_TRIBAL = {"AZ", "NM", "OK", "SD", "ND", "MT", "MN", "WA", "AK"}

# Known data center company name fragments for normalization
COMPANY_PATTERNS = [
    (r"equinix",             "Equinix"),
    (r"digital realty",      "Digital Realty"),
    (r"iron mountain",       "Iron Mountain"),
    (r"amazon|aws",          "Amazon AWS"),
    (r"microsoft",           "Microsoft Azure"),
    (r"alphabet|google",     "Google"),
    (r"meta platforms|facebook|meta\b", "Meta"),
    (r"apple inc",           "Apple"),
    (r"oracle",              "Oracle"),
    (r"qts realty",          "QTS Realty"),
    (r"coresite",            "CoreSite"),
    (r"cyrusone",            "CyrusOne"),
    (r"switch",              "Switch"),
    (r"vantage",             "Vantage"),
    (r"edgeconnex",          "EdgeConneX"),
    (r"databank",            "DataBank"),
    (r"riot platforms",      "Riot Platforms"),
    (r"american tower",      "American Tower"),
    (r"blackstone",          "Blackstone"),
    (r"digitalbridge",       "DigitalBridge"),
    (r"compass data",        "Compass Datacenters"),
    (r"aligned",             "Aligned Energy"),
    (r"tierpoint",           "TierPoint"),
    (r"flexential",          "Flexential"),
    (r"terawulf",            "TeraWulf"),
    (r"riot",                "Riot Platforms"),
    (r"core scientific",     "Core Scientific"),
    (r"mara|marathon digital","Marathon Digital"),
]


def normalize_company(raw: str) -> str:
    low = raw.lower()
    for pattern, name in COMPANY_PATTERNS:
        if re.search(pattern, low):
            return name
    # Clean up: remove trailing CIK / ticker junk
    clean = re.sub(r"\(CIK.*\)", "", raw).strip()
    clean = re.sub(r"\s*\([A-Z-]+\)\s*", " ", clean).strip()
    return clean or raw.strip()


def extract_display_name(display_names: list) -> str:
    if not display_names:
        return "Unknown"
    first = display_names[0]
    # Format: "EQUINIX INC  (EQIX)  (CIK 0001101239)"
    name = re.sub(r"\s*\(CIK.*\)$", "", first).strip()
    name = re.sub(r"\s*\([A-Z,\s-]+\)\s*$", "", name).strip()
    return normalize_company(name)


# ── Tribal buffer ─────────────────────────────────────────────────────────────

def load_tribal_union():
    gpkg = PROCESSED / "tribal_lands.gpkg"
    if not gpkg.exists():
        raise FileNotFoundError(f"{gpkg} not found — run pipeline first")
    gdf = gpd.read_file(gpkg).to_crs("EPSG:5070")
    return unary_union(gdf.geometry).buffer(BUFFER_KM * 1000)


def in_buffer(lon: float, lat: float, buffer_5070, transformer) -> bool:
    x, y = transformer.transform(lon, lat)
    return buffer_5070.contains(Point(x, y))


# ── EDGAR state-targeted search ───────────────────────────────────────────────

def search_edgar_by_state(state_abbr: str, state_name: str, base_lon: float, base_lat: float) -> list:
    """Search EDGAR 8-K filings mentioning 'data center' AND this state name."""
    results = []
    try:
        params = {
            "q": f'"data center" "{state_name}"',
            "forms": "8-K",
            "dateRange": "custom",
            "startdt": "2022-01-01",
            "size": 15,
        }
        resp = requests.get(EDGAR_SEARCH, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        hits = resp.json().get("hits", {}).get("hits", [])

        # Deduplicate by company per state
        seen_companies = set()
        for hit in hits:
            src = hit.get("_source", {})
            company = extract_display_name(src.get("display_names", []))
            if company in seen_companies:
                continue
            seen_companies.add(company)

            # Jitter so multiple companies in the same state don't stack
            lon = base_lon + random.uniform(-2.0, 2.0)
            lat = base_lat + random.uniform(-1.0, 1.0)
            confidence = 70 if state_abbr in HIGH_TRIBAL else 50

            results.append({
                "buyer":           company,
                "resolved_parent": company,
                "confidence":      confidence,
                "state":           state_abbr,
                "file_date":       src.get("file_date", ""),
                "form":            "8-K",
                "source":          "SEC EDGAR 8-K",
                "lon":             lon,
                "lat":             lat,
            })

    except Exception as e:
        print(f"    [error] EDGAR {state_name}: {e}")

    return results


def fetch_all_edgar_intelligence() -> list:
    all_records = []
    for state_abbr, (state_name, base_lon, base_lat) in TRIBAL_STATES.items():
        print(f"  {state_abbr} ({state_name})...", end=" ", flush=True)
        records = search_edgar_by_state(state_abbr, state_name, base_lon, base_lat)
        print(f"{len(records)} companies")
        all_records.extend(records)
        time.sleep(0.4)
    return all_records


# ── USACE permit search ───────────────────────────────────────────────────────

USACE_URL = "https://permits.ops.usace.army.mil/orm-public/instrument"


def fetch_usace_permits(buffer_5070, transformer) -> list:
    """
    Query USACE ORM public API for large permit applications in tribal states.
    Falls back gracefully if the API is unavailable.
    """
    results = []
    # USACE ORM public search — works with GET params
    for state_abbr, (state_name, base_lon, base_lat) in list(TRIBAL_STATES.items())[:10]:
        try:
            params = {
                "state": state_abbr,
                "projectType": "COMMERCIAL",
                "pageSize": 10,
                "pageNumber": 1,
            }
            resp = requests.get(USACE_URL, params=params, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = data if isinstance(data, list) else data.get("results", data.get("items", []))
            for item in items[:5]:
                lat = item.get("latitude") or item.get("lat")
                lon = item.get("longitude") or item.get("lon")
                if not lat or not lon:
                    continue
                try:
                    lat, lon = float(lat), float(lon)
                except (TypeError, ValueError):
                    continue
                if not in_buffer(lon, lat, buffer_5070, transformer):
                    continue
                results.append({
                    "buyer":           item.get("applicantName", item.get("name", "Unknown")),
                    "resolved_parent": "USACE Permit Applicant",
                    "confidence":      45,
                    "state":           state_abbr,
                    "source":          "USACE Permit",
                    "lon":             lon,
                    "lat":             lat,
                })
        except Exception:
            continue
        time.sleep(0.2)

    print(f"  USACE: {len(results)} permits in tribal buffer")
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    from pyproj import Transformer

    print("[land] Loading tribal buffer...")
    buffer_5070 = load_tribal_union()
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)

    print("\n[land] Searching EDGAR 8-K filings by tribal state...")
    edgar_records = fetch_all_edgar_intelligence()
    print(f"  Total EDGAR signals: {len(edgar_records)}")

    # Spatial filter
    edgar_in_buffer = [r for r in edgar_records if in_buffer(r["lon"], r["lat"], buffer_5070, transformer)]
    print(f"  In tribal buffer: {len(edgar_in_buffer)}")

    print("\n[land] Querying USACE permit database...")
    usace_records = fetch_usace_permits(buffer_5070, transformer)

    all_records = edgar_in_buffer + usace_records
    print(f"\n[land] Total acquisition signals: {len(all_records)}")

    features = []
    for rec in all_records:
        props = {
            "buyer":           rec["buyer"],
            "resolved_parent": rec["resolved_parent"],
            "confidence":      rec["confidence"],
            "state":           rec.get("state", ""),
            "source":          rec.get("source", ""),
        }
        if rec.get("file_date"):
            props["file_date"] = rec["file_date"]

        features.append({
            "type":       "Feature",
            "geometry":   {"type": "Point", "coordinates": [rec["lon"], rec["lat"]]},
            "properties": props,
        })

    fc = {"type": "FeatureCollection", "features": features}

    for path in (OUTPUT / "land_acquisitions.geojson", WEBAPP / "land_acquisitions.geojson"):
        path.write_text(json.dumps(fc))
        size_kb = path.stat().st_size / 1024
        print(f"[land] Saved → {path} ({len(features)} records, {size_kb:.0f} KB)")

    from collections import Counter
    companies = Counter(f["properties"]["resolved_parent"] for f in features)
    print("\n[land] Top companies near tribal lands:")
    for co, n in companies.most_common(15):
        print(f"  {co:35s} {n}")

    states = Counter(f["properties"]["state"] for f in features)
    print("\n[land] States with data center activity:")
    for st, n in states.most_common():
        tribal_flag = " ★" if st in HIGH_TRIBAL else ""
        print(f"  {st:4s} {n}{tribal_flag}")

    print("\n[land] Done. Refresh localhost:8765 to see Land Acquisitions layer.")


if __name__ == "__main__":
    main()
