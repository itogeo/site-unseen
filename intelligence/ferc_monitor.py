"""
intelligence/ferc_monitor.py
Scrapes public FERC/RTO interconnection queue data for large-load requests
near tribal lands. Filed 2-3 years before construction — earliest free signal.

Sources (all free, all public):
  - PJM interconnection queue (Excel download)
  - FERC eLibrary full-text search (REST API)
  - Future: MISO, SPP, WECC, ERCOT queues (same pattern)

Outputs:
  data/intel/ferc_queue_flags.json    — geocoded requests near tribal lands
  data/intel/ferc_elibrary_filings.json — text search hits
"""

import json
import time
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

INTEL = Path("data/intel")
INTEL.mkdir(parents=True, exist_ok=True)
PROC = Path("data/processed")

MIN_MW = 50           # flag requests >= this threshold
BUFFER_KM = 100       # km buffer around tribal lands to check


# ── Load tribal boundaries ONCE at module startup ─────────────────────────────
def _load_tribal_buffer(buffer_km: int = BUFFER_KM) -> gpd.GeoDataFrame:
    gpkg = PROC / "tribal_lands.gpkg"
    if not gpkg.exists():
        print(f"[ferc] WARNING: {gpkg} not found — run base pipeline first")
        return gpd.GeoDataFrame()
    gdf = gpd.read_file(gpkg).to_crs("EPSG:5070")
    gdf["geometry"] = gdf.geometry.buffer(buffer_km * 1000)
    return gdf.to_crs("EPSG:4326")[["geoid", "tribe_name", "geometry"]]


TRIBAL_BUFFER: gpd.GeoDataFrame = _load_tribal_buffer()


# ── PJM Queue ─────────────────────────────────────────────────────────────────
def fetch_pjm_queue() -> pd.DataFrame:
    """
    PJM publishes queue as a downloadable Excel file, updated roughly weekly.
    Filter for Large Load type entries >= MIN_MW.
    """
    url = ("https://www.pjm.com/-/media/planning/new-services-queue/"
           "new-services-queue.ashx")
    print("[ferc] Fetching PJM interconnection queue...")
    try:
        resp = requests.get(url, timeout=60,
                            headers={"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"})
        resp.raise_for_status()
        df = pd.read_excel(BytesIO(resp.content), sheet_name=0, header=0)
        print(f"  PJM: {len(df)} total entries")

        # Find type column and filter for load
        type_col = next(
            (c for c in df.columns
             if any(w in str(c).lower() for w in ["type", "fuel", "resource"])),
            None
        )
        if type_col:
            df = df[df[type_col].astype(str).str.upper()
                    .str.contains(r"LOAD|DEMAND|DR|LARGE", na=False, regex=True)]
            print(f"  After type filter: {len(df)} load entries")

        # Find MW column and filter
        mw_col = next(
            (c for c in df.columns
             if "mw" in str(c).lower() and "reactive" not in str(c).lower()),
            None
        )
        if mw_col:
            df[mw_col] = pd.to_numeric(df[mw_col], errors="coerce")
            df = df[df[mw_col] >= MIN_MW].copy()
            print(f"  After MW filter (>= {MIN_MW}): {len(df)} entries")

        return df

    except Exception as e:
        print(f"  [warn] PJM queue failed: {e}")
        return pd.DataFrame()


# ── FERC eLibrary Search ──────────────────────────────────────────────────────
def fetch_ferc_elibrary(days_back: int = 365) -> list[dict]:
    """
    Full-text search of FERC eLibrary for large-load filings mentioning
    tribal, Indian, or data center near Native lands.
    Free public API — rate limit with sleep(1).
    """
    print("[ferc] Searching FERC eLibrary...")
    results = []
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    end_date = datetime.now().strftime("%m/%d/%Y")

    search_terms = [
        "large load interconnection tribal",
        "large load data center tribal",
        "large load interconnection Indian Nation",
        "data center indigenous land",
        "hyperscale load interconnection rural",
    ]

    for term in search_terms:
        try:
            resp = requests.get(
                "https://elibrary.ferc.gov/eLibrary/search",
                params={
                    "searchType": "fullText",
                    "query": term,
                    "dateRange": "custom",
                    "startDate": start_date,
                    "endDate": end_date,
                    "format": "json",
                },
                timeout=30,
                headers={"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"},
            )
            if resp.status_code == 200:
                data = resp.json()
                hits = data.get("results", [])
                for item in hits:
                    results.append({
                        "source": "FERC_eLibrary",
                        "docket": item.get("docketNumber", ""),
                        "title": item.get("title", ""),
                        "date": item.get("filedDate", ""),
                        "description": item.get("description", "")[:300],
                        "url": item.get("url", ""),
                        "search_term": term,
                        "fetched_at": datetime.now().isoformat(),
                    })
            elif resp.status_code == 429:
                print("  Rate limited — sleeping 30s")
                time.sleep(30)
        except Exception as e:
            print(f"  [warn] eLibrary '{term}': {e}")
        time.sleep(1)

    # Deduplicate by docket+title
    seen = set()
    unique = []
    for r in results:
        key = f"{r['docket']}|{r['title']}"
        if key not in seen:
            seen.add(key)
            unique.append(r)

    print(f"  Found {len(unique)} unique FERC filings")
    return unique


# ── Docket RM26-4-000 Monitor ─────────────────────────────────────────────────
def fetch_rm26_docket() -> list[dict]:
    """
    Monitor FERC Docket RM26-4-000 — DOE's large-load interconnection rulemaking.
    Once finalized this will be the standardized national large-load queue.
    All filings are public.
    """
    print("[ferc] Fetching RM26-4-000 docket filings...")
    try:
        resp = requests.get(
            "https://elibrary.ferc.gov/eLibrary/search",
            params={
                "searchType": "docketNumber",
                "docketNumber": "RM26-4",
                "format": "json",
                "pageSize": 50,
            },
            timeout=30,
            headers={"User-Agent": "SiteUnseen/1.0 contact@itogeo.com"},
        )
        if resp.status_code == 200:
            data = resp.json()
            filings = data.get("results", [])
            print(f"  RM26-4-000: {len(filings)} filings")
            return [{
                "docket": "RM26-4-000",
                "title": f.get("title", ""),
                "date": f.get("filedDate", ""),
                "filer": f.get("name", ""),
                "url": f.get("url", ""),
            } for f in filings]
    except Exception as e:
        print(f"  [warn] RM26-4 docket: {e}")
    return []


# ── Spatial Flagging ──────────────────────────────────────────────────────────
def flag_near_tribal(df: pd.DataFrame, rto: str) -> gpd.GeoDataFrame:
    """
    Spatial join: flag queue entries within BUFFER_KM of any tribal land.
    Returns matched rows with tribe_name attached.
    """
    if TRIBAL_BUFFER.empty or df.empty:
        return gpd.GeoDataFrame()

    # Find lat/lon columns
    lat_col = next((c for c in df.columns if "lat" in str(c).lower()), None)
    lon_col = next((c for c in df.columns
                    if "lon" in str(c).lower() or "lng" in str(c).lower()), None)

    if not lat_col or not lon_col:
        print(f"  [note] {rto}: no lat/lon columns — cannot spatially join")
        return gpd.GeoDataFrame()

    df = df.copy()
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    df = df.dropna(subset=[lat_col, lon_col])

    if df.empty:
        return gpd.GeoDataFrame()

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326",
    )

    joined = gpd.sjoin(
        gdf, TRIBAL_BUFFER, how="inner", predicate="within"
    )
    joined["rto"] = rto
    joined["flag_type"] = "ferc_queue"
    joined["flagged_at"] = datetime.now().isoformat()

    print(f"  {rto}: {len(joined)} entries within {BUFFER_KM}km of tribal land")
    return joined


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    all_flags = []

    # PJM queue
    pjm_df = fetch_pjm_queue()
    if not pjm_df.empty:
        pjm_flagged = flag_near_tribal(pjm_df, "PJM")
        if not pjm_flagged.empty:
            all_flags.append(pjm_flagged)

    # FERC eLibrary text search
    elibrary = fetch_ferc_elibrary()
    elib_path = INTEL / "ferc_elibrary_filings.json"
    elib_path.write_text(json.dumps(elibrary, indent=2))
    print(f"[ferc] eLibrary filings → {elib_path}")

    # RM26-4-000 rulemaking docket
    rm26 = fetch_rm26_docket()
    rm26_path = INTEL / "ferc_rm26_filings.json"
    rm26_path.write_text(json.dumps(rm26, indent=2))
    print(f"[ferc] RM26-4 filings → {rm26_path}")

    # Save queue flags
    if all_flags:
        combined = pd.concat(all_flags, ignore_index=True)
        # Drop geometry for JSON output
        out_df = combined.drop(columns=["geometry"], errors="ignore")
        flags_path = INTEL / "ferc_queue_flags.json"
        out_df.to_json(flags_path, orient="records", indent=2)
        print(f"[ferc] Queue flags → {flags_path} ({len(combined)} entries)")
    else:
        print("[ferc] No geocoded queue entries near tribal lands")
        print(f"       Monitor manually: https://elibrary.ferc.gov/eLibrary/search"
              f"?docketNumber=RM26-4")

    print("\n[ferc] Done.")
    print(f"  eLibrary hits:    {len(elibrary)}")
    print(f"  RM26-4 filings:   {len(rm26)}")
    print(f"  Queue flags:      {sum(len(f) for f in all_flags)}")


if __name__ == "__main__":
    main()
