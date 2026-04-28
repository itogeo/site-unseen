"""
intelligence/ownership_watch.py
Core watchdog — resolves LLC names from public land records to hyperscaler
parent companies. SQLite cache preserves state between runs so no LLC is
re-fetched unnecessarily.

Data sources (all free):
  - OpenCorporates API (free tier, 50 req/day without key)
  - SEC EDGAR submissions API (public, no key)
  - ArcGIS Hub open data (county parcel datasets)
  - Census TIGER county boundaries (auto-downloaded)

Run order:
  1. python intelligence/subsidiaries.py  (seed the flat lookup first)
  2. python intelligence/ownership_watch.py
"""

import json
import re
import sqlite3
import time
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz, process
import os

load_dotenv()

INTEL = Path("data/intel")
INTEL.mkdir(parents=True, exist_ok=True)
PROC = Path("data/processed")
OUTPUT = Path("output")
OUTPUT.mkdir(exist_ok=True)

DB_PATH = INTEL / "ownership.db"
MIN_ACRES = 50
MONITOR_BUFFER_KM = 80
LOOKBACK_DAYS = 180
CACHE_TTL_DAYS = 30   # re-check resolved LLCs after this many days

# Registered agents heavily used by hyperscalers for shell companies
HYPERSCALER_AGENTS = [
    "CT CORPORATION SYSTEM",
    "CORPORATION SERVICE COMPANY",
    "NATIONAL REGISTERED AGENTS",
    "THE PRENTICE-HALL CORPORATION SYSTEM",
    "UNITED AGENT GROUP",
    "COGENCY GLOBAL",
]

# Generic naming patterns that flag suspicious LLCs
GENERIC_PATTERNS = re.compile(
    r"(HOLDINGS|VENTURES|PROPERTIES|LAND|REALTY|ACQUISITION|CAPITAL|ASSETS|"
    r"DEVELOPMENT|INFRASTRUCTURE|DIGITAL|CLOUD|DATA|TECH|AI|COMPUTE|"
    r"HORIZON|SUMMIT|PINNACLE|APEX|VERTEX|NEXUS|PRIME|CORE|HUB|"
    r"PROJECT|CAMPUS|SITE|FACILITY|PARCEL|TRACT|INNOVATION|"
    r"QUANTUM|NIMBUS|CUMULUS|STRATUS|STARGATE|VADATA|GRACELAND|PAPYRUS)",
    re.IGNORECASE,
)

SEC_HEADERS = {
    "User-Agent": "SiteUnseen/1.0 HonorEarth contact@itogeo.com"
}


# ── Database ──────────────────────────────────────────────────────────────────
def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS resolutions (
            name TEXT PRIMARY KEY,
            resolved_parent TEXT,
            confidence REAL,
            method TEXT,
            incorporation_date TEXT,
            registered_agent TEXT,
            flags TEXT,
            is_critical INTEGER DEFAULT 0,
            checked_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS acquisitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            county_fips TEXT,
            county_name TEXT,
            buyer TEXT,
            resolved_parent TEXT,
            confidence REAL,
            suspicion_score REAL,
            acreage REAL,
            sale_date TEXT,
            lat REAL,
            lon REAL,
            flags TEXT,
            tribe_name TEXT,
            detected_at TEXT
        )
    """)
    conn.commit()
    return conn


def cache_get(name: str, conn: sqlite3.Connection) -> dict | None:
    """Return cached resolution if checked within CACHE_TTL_DAYS, else None."""
    row = conn.execute(
        "SELECT * FROM resolutions WHERE name = ?", (name.upper(),)
    ).fetchone()
    if not row:
        return None
    checked_at = datetime.fromisoformat(row[8]) if row[8] else datetime.min
    if (datetime.now() - checked_at).days > CACHE_TTL_DAYS:
        return None  # stale — re-check
    cols = ["name", "resolved_parent", "confidence", "method",
            "incorporation_date", "registered_agent", "flags", "is_critical", "checked_at"]
    result = dict(zip(cols, row))
    result["flags"] = json.loads(result["flags"] or "[]")
    return result


def cache_set(result: dict, conn: sqlite3.Connection) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO resolutions
        (name, resolved_parent, confidence, method, incorporation_date,
         registered_agent, flags, is_critical, checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        result["name"].upper(),
        result.get("resolved_parent"),
        result.get("confidence", 0),
        result.get("method"),
        result.get("incorporation_date"),
        result.get("registered_agent"),
        json.dumps(result.get("flags", [])),
        1 if result.get("is_critical") else 0,
        datetime.now().isoformat(),
    ))
    conn.commit()


# ── Resolution Pipeline ───────────────────────────────────────────────────────
def load_flat_lookup() -> dict:
    path = OUTPUT / "known_subsidiaries_flat.json"
    if path.exists():
        return json.loads(path.read_text())
    print("[ownership] WARNING: known_subsidiaries_flat.json missing — run subsidiaries.py first")
    return {}


def resolve_llc(name: str, conn: sqlite3.Connection,
                api_key: str | None = None) -> dict:
    """
    Resolve a company name to its ultimate hyperscaler parent.
    Checks cache first, then works through resolution methods in order.
    """
    if not name or name.lower() in ("nan", "none", ""):
        return {"name": name, "resolved_parent": None, "confidence": 0,
                "method": None, "flags": [], "is_critical": False}

    # Check cache
    cached = cache_get(name, conn)
    if cached:
        return cached

    result = {
        "name": name,
        "resolved_parent": None,
        "confidence": 0,
        "method": None,
        "incorporation_date": None,
        "registered_agent": None,
        "flags": [],
        "is_critical": False,
    }

    name_upper = name.upper().strip()
    flat = load_flat_lookup()

    # Step 1: Critical entity exact match (top priority)
    critical_entities = {
        "VADATA INC": "Amazon", "VADATA LLC": "Amazon",
        "STARGATE LLC": "OpenAI_Stargate", "SGP OPERATOR LLC": "OpenAI_Stargate",
        "STARGATE OPERATOR LLC": "OpenAI_Stargate",
        "GRACELAND ACQUISITIONS LLC": "Meta", "PAPYRUS ACQUISITIONS LLC": "Meta",
        "QUANTUM VALLEY LLC": "Alphabet", "CHARLESTON EAST LLC": "Alphabet",
        "RREEF AMERICA LLC": "Alphabet",
    }
    if name_upper in critical_entities:
        result.update({
            "resolved_parent": critical_entities[name_upper],
            "confidence": 99,
            "method": "critical_entity_exact",
            "is_critical": True,
        })
        result["flags"].append(f"CRITICAL: exact match to known acquisition entity")
        cache_set(result, conn)
        return result

    # Step 2: Flat lookup direct match
    if name_upper in flat:
        result.update({
            "resolved_parent": flat[name_upper],
            "confidence": 97,
            "method": "flat_lookup_exact",
        })
        result["flags"].append(f"Exact match in subsidiary database")
        cache_set(result, conn)
        return result

    # Step 3: Fuzzy match against all known names
    if flat:
        match = process.extractOne(
            name_upper, list(flat.keys()), scorer=fuzz.partial_ratio
        )
        if match and match[1] >= 85:
            result.update({
                "resolved_parent": flat.get(match[0], "LIKELY_HYPERSCALER"),
                "confidence": float(match[1]),
                "method": "fuzzy_match",
            })
            result["flags"].append(f"Fuzzy match ({match[1]}%): {match[0]}")

    # Step 4: Direct parent name substring check
    parent_names = ["MICROSOFT", "AMAZON", "GOOGLE", "ALPHABET", "META",
                    "FACEBOOK", "APPLE", "OPENAI", "ORACLE", "STARGATE",
                    "EQUINIX", "VADATA"]
    for parent in parent_names:
        if parent in name_upper:
            result.update({
                "resolved_parent": parent.capitalize(),
                "confidence": max(result["confidence"], 95),
                "method": "name_contains_parent",
            })
            result["flags"].append(f"Name contains parent: {parent}")
            break

    # Step 5: OpenCorporates (free tier — 50 req/day without key)
    if result["confidence"] < 80:
        oc = _query_opencorporates(name, api_key)
        if oc:
            result["incorporation_date"] = oc.get("incorporation_date")

            # Check registered agent
            addr = oc.get("registered_address", "").upper()
            for agent in HYPERSCALER_AGENTS:
                if agent in addr:
                    result["registered_agent"] = agent
                    result["flags"].append(f"Hyperscaler agent: {agent}")
                    result["confidence"] = max(result["confidence"], 65)
                    break

            # Check LLC age
            if oc.get("incorporation_date"):
                try:
                    inc_date = datetime.fromisoformat(oc["incorporation_date"])
                    age_days = (datetime.now() - inc_date).days
                    if age_days < 730:
                        result["flags"].append(
                            f"LLC formed only {age_days} days ago — likely purpose-built"
                        )
                        result["confidence"] = max(result["confidence"], 55)
                except Exception:
                    pass

    # Step 6: Generic LLC name pattern (even if unresolved, flag as suspicious)
    is_llc = bool(re.search(r"\bLLC\b|\bL\.L\.C\b|\bLTD\b", name, re.I))
    has_generic = bool(GENERIC_PATTERNS.search(name))
    if is_llc and has_generic:
        result["flags"].append("Generic LLC name pattern matches data center siting vocabulary")
        result["confidence"] = max(result["confidence"], 40)

    cache_set(result, conn)
    time.sleep(0.3)
    return result


def _query_opencorporates(name: str, api_key: str | None = None) -> dict:
    """Query OpenCorporates. Free tier: 50 req/day. Returns first result or {}."""
    params = {
        "q": name,
        "jurisdiction_code": "us",
        "normalise_company_name": "true",
        "per_page": 3,
    }
    if api_key:
        params["api_token"] = api_key
    try:
        resp = requests.get(
            "https://api.opencorporates.com/v0.4/companies/search",
            params=params, timeout=15,
            headers={"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"},
        )
        if resp.status_code == 200:
            companies = resp.json().get("results", {}).get("companies", [])
            if companies:
                c = companies[0].get("company", {})
                return {
                    "name": c.get("name", ""),
                    "incorporation_date": c.get("incorporation_date", ""),
                    "registered_address": c.get("registered_address_in_full", ""),
                    "company_number": c.get("company_number", ""),
                }
        elif resp.status_code == 429:
            print("  [warn] OpenCorporates rate limit — sleeping 60s")
            time.sleep(60)
    except Exception as e:
        print(f"  [warn] OpenCorporates: {e}")
    return {}


# ── County Discovery ──────────────────────────────────────────────────────────
def get_target_counties() -> list[dict]:
    """
    Get all US counties within MONITOR_BUFFER_KM of any tribal land.
    Auto-downloads Census TIGER county file if not cached.
    """
    county_dir = Path("data/raw/counties")
    sentinel = county_dir / ".downloaded"

    if not sentinel.exists():
        print("[ownership] Downloading Census county boundaries...")
        url = ("https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/"
               "tl_2023_us_county.zip")
        import zipfile
        resp = requests.get(url, timeout=180,
                            headers={"User-Agent": "SiteUnseen/1.0"})
        resp.raise_for_status()
        county_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(BytesIO(resp.content)) as z:
            z.extractall(county_dir)
        sentinel.touch()

    shp = next(county_dir.glob("*.shp"))
    counties = gpd.read_file(shp).to_crs("EPSG:5070")

    tribal = gpd.read_file(PROC / "tribal_lands.gpkg").to_crs("EPSG:5070")
    tribal_buf = tribal.geometry.buffer(MONITOR_BUFFER_KM * 1000).unary_union

    near = counties[counties.geometry.intersects(tribal_buf)].copy()
    print(f"[ownership] {len(near)} counties within {MONITOR_BUFFER_KM}km of tribal land")

    return [
        {"fips": row["GEOID"], "name": row["NAME"], "state": row["STATEFP"]}
        for _, row in near.iterrows()
    ]


# ── County Parcel Scanning ────────────────────────────────────────────────────
def scan_arcgis_hub(county_fips: str, county_name: str,
                    conn: sqlite3.Connection) -> list[dict]:
    """
    Query ArcGIS Hub for public parcel/deed datasets in this county.
    Downloads CSV if available, flags large recent acquisitions by LLCs.
    """
    flagged = []
    api_key = os.getenv("OPENCORPORATES_API_KEY")

    # ArcGIS Hub discovery API
    search_urls = [
        f"https://hub.arcgis.com/api/v3/datasets?q=parcel+sales&filter[region]={county_fips}",
        f"https://hub.arcgis.com/api/v3/datasets?q=deed+transfer&filter[region]={county_fips}",
        f"https://opendata.arcgis.com/api/v3/datasets?filter[tags]=parcels&q=sales&filter[region]={county_fips}",
    ]

    df = pd.DataFrame()
    for url in search_urls:
        try:
            resp = requests.get(url, timeout=20,
                                headers={"User-Agent": "SiteUnseen/1.0"})
            if resp.status_code == 200:
                data = resp.json()
                datasets = data.get("data", [])
                for ds in datasets[:2]:
                    ds_id = ds.get("id", "")
                    if not ds_id:
                        continue
                    csv_url = (f"https://opendata.arcgis.com/datasets/"
                               f"{ds_id}/downloads/data?format=csv")
                    try:
                        csv_resp = requests.get(csv_url, timeout=60)
                        if csv_resp.status_code == 200 and len(csv_resp.content) > 1000:
                            df = pd.read_csv(StringIO(csv_resp.text), low_memory=False)
                            print(f"    [{county_name}] Loaded {len(df)} parcels from ArcGIS Hub")
                            break
                    except Exception:
                        continue
                if not df.empty:
                    break
        except Exception:
            continue
        time.sleep(0.2)

    if df.empty:
        return []

    # Find buyer column
    buyer_col = next(
        (c for c in df.columns
         if any(w in c.lower() for w in ["owner", "buyer", "grantee", "seller"])),
        None
    )
    if not buyer_col:
        return []

    # Filter by acreage
    acre_col = next((c for c in df.columns if "acre" in c.lower()), None)
    if acre_col:
        df[acre_col] = pd.to_numeric(df[acre_col], errors="coerce")
        df = df[df[acre_col] >= MIN_ACRES].copy()

    # Filter by sale date
    date_col = next(
        (c for c in df.columns
         if any(w in c.lower() for w in ["sale", "deed", "record", "transfer", "date"])),
        None
    )
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)
        df = df[df[date_col] >= cutoff].copy()

    if df.empty:
        return []

    print(f"    [{county_name}] {len(df)} large recent transactions to check")

    # Score and resolve each buyer
    lat_col = next((c for c in df.columns if "lat" in c.lower()), None)
    lon_col = next((c for c in df.columns
                    if "lon" in c.lower() or "lng" in c.lower()), None)

    for _, row in df.iterrows():
        buyer = str(row.get(buyer_col, ""))
        if not buyer or buyer.lower() in ("nan", "none", ""):
            continue

        # Suspicion pre-filter — only resolve LLCs + generic names
        buyer_upper = buyer.upper()
        is_llc = bool(re.search(r"\bLLC\b|\bL\.L\.C\b|\bLTD\b", buyer, re.I))
        has_generic = bool(GENERIC_PATTERNS.search(buyer))
        if not (is_llc or has_generic):
            continue

        resolution = resolve_llc(buyer, conn, api_key)
        suspicion = _compute_suspicion(buyer, resolution,
                                       float(row.get(acre_col, 0)) if acre_col else 0)

        if suspicion < 3 and not resolution.get("is_critical"):
            continue

        record = {
            "county_fips": county_fips,
            "county_name": county_name,
            "buyer": buyer,
            "resolved_parent": resolution.get("resolved_parent"),
            "confidence": resolution.get("confidence", 0),
            "suspicion_score": suspicion,
            "acreage": float(row.get(acre_col, 0)) if acre_col else None,
            "sale_date": str(row.get(date_col, "")) if date_col else None,
            "lat": float(row[lat_col]) if lat_col and pd.notna(row.get(lat_col)) else None,
            "lon": float(row[lon_col]) if lon_col and pd.notna(row.get(lon_col)) else None,
            "flags": json.dumps(resolution.get("flags", [])),
            "tribe_name": None,  # populated in spatial join below
            "detected_at": datetime.now().isoformat(),
        }
        flagged.append(record)

        # Insert to DB
        conn.execute("""
            INSERT INTO acquisitions
            (county_fips, county_name, buyer, resolved_parent, confidence,
             suspicion_score, acreage, sale_date, lat, lon, flags, tribe_name, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, tuple(record.values()))
        conn.commit()

    return flagged


def _compute_suspicion(buyer: str, resolution: dict, acreage: float) -> float:
    """Score 0-10 on likelihood of being a hyperscaler acquisition."""
    score = 0.0
    name_upper = buyer.upper()
    if re.search(r"\bLLC\b|\bL\.L\.C\b|\bLTD\b", buyer, re.I):
        score += 2
    if GENERIC_PATTERNS.search(buyer):
        score += 2
    if resolution.get("is_critical"):
        score += 5
    elif resolution.get("resolved_parent"):
        score += 3
    if acreage >= 500:
        score += 2
    elif acreage >= 200:
        score += 1
    if resolution.get("registered_agent"):
        score += 1
    return round(score, 1)


# ── Spatial Join + Export ─────────────────────────────────────────────────────
def attach_tribal_names(flagged: list[dict]) -> list[dict]:
    """Spatial join to attach nearest tribal land name to each flagged acquisition."""
    if not flagged:
        return flagged
    geo_rows = [r for r in flagged if r.get("lat") and r.get("lon")]
    if not geo_rows:
        return flagged

    try:
        tribal = gpd.read_file(PROC / "tribal_lands.gpkg").to_crs("EPSG:4326")
        gdf = gpd.GeoDataFrame(
            geo_rows,
            geometry=gpd.points_from_xy(
                [r["lon"] for r in geo_rows],
                [r["lat"] for r in geo_rows],
            ),
            crs="EPSG:4326",
        )
        tribal_buf = tribal.copy().to_crs("EPSG:5070")
        tribal_buf["geometry"] = tribal_buf.geometry.buffer(80_000)
        tribal_buf = tribal_buf.to_crs("EPSG:4326")

        joined = gpd.sjoin(
            gdf, tribal_buf[["tribe_name", "geometry"]], how="left", predicate="within"
        )
        for i, row in joined.iterrows():
            if pd.notna(row.get("tribe_name_right")):
                geo_rows[i]["tribe_name"] = row["tribe_name_right"]
    except Exception as e:
        print(f"  [warn] Tribal spatial join failed: {e}")
    return flagged


def export_flags(conn: sqlite3.Connection) -> None:
    """Export all flagged acquisitions from DB to GeoJSON and JSON."""
    rows = conn.execute(
        "SELECT * FROM acquisitions WHERE suspicion_score >= 3 OR confidence >= 70"
    ).fetchall()
    cols = ["id", "county_fips", "county_name", "buyer", "resolved_parent",
            "confidence", "suspicion_score", "acreage", "sale_date",
            "lat", "lon", "flags", "tribe_name", "detected_at"]

    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        print("[ownership] No flagged acquisitions to export")
        return

    df["flags"] = df["flags"].apply(lambda x: json.loads(x) if x else [])

    # JSON export
    json_out = OUTPUT / "land_acquisitions.json"
    df.to_json(json_out, orient="records", indent=2)
    print(f"[ownership] Land acquisitions → {json_out} ({len(df)} records)")

    # GeoJSON if lat/lon present
    geo_df = df.dropna(subset=["lat", "lon"])
    if not geo_df.empty:
        gdf = gpd.GeoDataFrame(
            geo_df,
            geometry=gpd.points_from_xy(geo_df["lon"], geo_df["lat"]),
            crs="EPSG:4326",
        )
        gdf["flags"] = gdf["flags"].apply(json.dumps)
        geo_out = OUTPUT / "land_acquisitions.geojson"
        gdf.to_file(geo_out, driver="GeoJSON")
        print(f"[ownership] GeoJSON → {geo_out} ({len(gdf)} points)")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print("[ownership] Initializing database...")
    conn = init_db()

    print("[ownership] Getting target counties...")
    counties = get_target_counties()

    all_flagged = []
    for i, county in enumerate(counties):
        fips, name = county["fips"], county["name"]
        print(f"  [{i+1}/{len(counties)}] {name} County ({fips})")
        flagged = scan_arcgis_hub(fips, name, conn)
        all_flagged.extend(flagged)
        time.sleep(0.3)

    # Attach tribal names via spatial join
    all_flagged = attach_tribal_names(all_flagged)

    # Export
    export_flags(conn)

    # Summary
    resolved = [f for f in all_flagged if f.get("resolved_parent")]
    critical = [f for f in all_flagged if f.get("confidence", 0) >= 90]
    print(f"\n[ownership] Done.")
    print(f"  Counties scanned:     {len(counties)}")
    print(f"  Total flagged:        {len(all_flagged)}")
    print(f"  Resolved to parent:   {len(resolved)}")
    print(f"  Critical confidence:  {len(critical)}")
    if critical:
        print("\n  ⚠️  HIGH CONFIDENCE FLAGS:")
        for f in critical:
            print(f"    {f['buyer'][:50]:50s} → {f['resolved_parent']}"
                  f"  ({f['confidence']:.0f}%)")

    conn.close()


if __name__ == "__main__":
    main()
