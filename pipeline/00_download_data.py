"""
Downloads all public federal datasets needed for scoring.
All sources are free, no API keys required except Census (free registration).
Run this once — outputs land in data/raw/
"""

import json
import requests
import zipfile
import io
from pathlib import Path
from tqdm import tqdm

RAW = Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)

WARN = []  # collect non-fatal failures to summarize at end


def download_file(url: str, dest: Path, label: str):
    """Stream download with progress bar. Non-fatal on error."""
    if dest.exists():
        print(f"  [skip] {label} already downloaded")
        return
    print(f"  [fetch] {label}")
    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=label[:40]) as bar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
    except Exception as e:
        WARN.append(f"{label}: {e}")
        print(f"  [WARN] {label} failed: {e}")


def download_zip(url: str, dest_dir: Path, label: str):
    """Download and extract a zip. Non-fatal on error."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    sentinel = dest_dir / ".downloaded"
    if sentinel.exists():
        print(f"  [skip] {label} already extracted")
        return
    print(f"  [fetch+unzip] {label}")
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            z.extractall(dest_dir)
        sentinel.touch()
    except Exception as e:
        WARN.append(f"{label}: {e}")
        print(f"  [WARN] {label} failed: {e}")


def download_arcgis_rest(service_url: str, dest: Path, label: str,
                          where: str = "1=1", fields: str = "*"):
    """
    Paginated GeoJSON download from an ArcGIS REST FeatureService.
    More stable than Hub download API. Saves as .geojson.
    """
    if dest.exists():
        print(f"  [skip] {label} already downloaded")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    query_url = f"{service_url}/0/query"
    all_features = []
    offset = 0
    chunk = 2000
    print(f"  [fetch-rest] {label}")
    try:
        while True:
            params = {
                "where": where,
                "outFields": fields,
                "f": "geojson",
                "resultOffset": offset,
                "resultRecordCount": chunk,
                "returnGeometry": "true",
            }
            resp = requests.get(query_url, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise ValueError(data["error"])
            features = data.get("features", [])
            all_features.extend(features)
            print(f"    {len(all_features)} features...", end="\r")
            if len(features) < chunk:
                break
            offset += chunk
        print(f"    {len(all_features)} features total        ")
        dest.write_text(json.dumps({"type": "FeatureCollection", "features": all_features}))
        print(f"  [saved] {dest}")
    except Exception as e:
        WARN.append(f"{label}: {e}")
        print(f"\n  [WARN] {label} failed: {e}")


# ── 1. BIA Tribal Lands (Census TIGER 2023) ────────────────────────────────
download_zip(
    url="https://www2.census.gov/geo/tiger/TIGER2023/AIANNH/tl_2023_us_aiannh.zip",
    dest_dir=RAW / "tribal_boundaries",
    label="BIA Tribal Boundaries (TIGER 2023)"
)

# ── 2. EIA Electric Transmission Lines (HIFLD via ArcGIS REST) ─────────────
# Paginated GeoJSON — more stable than Hub download API
download_arcgis_rest(
    service_url="https://services.arcgis.com/G4S1dGvn7PIgYd6Y/ArcGIS/rest/services/HIFLD_electric_power_transmission_lines/FeatureServer",
    dest=RAW / "transmission_lines" / "transmission_lines.geojson",
    label="EIA Transmission Lines (HIFLD REST)"
)

# ── 3. EIA Electric Substations (HIFLD via ArcGIS REST) ────────────────────
download_arcgis_rest(
    service_url="https://services.arcgis.com/G4S1dGvn7PIgYd6Y/ArcGIS/rest/services/HIFLD_electric_power_substations/FeatureServer",
    dest=RAW / "substations" / "substations.geojson",
    label="Electric Substations (HIFLD REST)"
)

# ── 4. EIA Electricity Rates by State/Utility ──────────────────────────────
download_file(
    url="https://www.eia.gov/electricity/data/eia861/zip/f8612022.zip",
    dest=RAW / "eia861_rates.zip",
    label="EIA Form 861 Electricity Rates"
)

# ── 5. USGS Principal Aquifers ──────────────────────────────────────────────
download_zip(
    url="https://water.usgs.gov/GIS/dsdl/aquifers_us.zip",
    dest_dir=RAW / "aquifers",
    label="USGS Principal Aquifers"
)

# ── 6. IRS Opportunity Zones ────────────────────────────────────────────────
download_file(
    url="https://www.cdfifund.gov/sites/cdfi/files/2018-06/OZ_Shapefile.zip",
    dest=RAW / "opportunity_zones.zip",
    label="IRS Opportunity Zones (CDFI Fund)"
)

# ── 7. EPA EJScreen ─────────────────────────────────────────────────────────
download_zip(
    url="https://gaftp.epa.gov/EJSCREEN/2023/EJSCREEN_2023_BG_with_AS_CNMI_GU_VI.csv.zip",
    dest_dir=RAW / "ejscreen",
    label="EPA EJScreen 2023 (block group)"
)

# ── 8. USGS NHD Water Bodies ────────────────────────────────────────────────
# ~10GB — skip for a quick first run, pipeline has fallback using Census water area
print("  [skip-large] USGS NHD national file (~10GB) — download manually if needed:")
print("    https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/National/HighResolution/GDB/NHD_H_National_GDB.zip")
print("    Extract to: data/raw/nhd_water/")

# ── 9. FCC Broadband (fiber) ────────────────────────────────────────────────
print("  [manual] FCC Broadband: https://broadbandmap.fcc.gov/data-download")
print("           Fixed Broadband Availability > Fiber → data/raw/fcc_fiber/")

# ── 10. FEMA National Flood Hazard Layer ───────────────────────────────────
print("  [manual] FEMA NFHL: https://msc.fema.gov/portal/advanceSearch")
print("           Download national NFHL shapefile → data/raw/fema_flood/")

# ── 11. Census ACS Poverty ──────────────────────────────────────────────────
print("  [api] Census ACS pulled in step 03 — get key: https://api.census.gov/data/key_signup.html")

# ── 12. Honor the Earth Known Sites ────────────────────────────────────────
print("  [manual] HTE tracker: export Google Form CSV → data/raw/honor_earth_tracker.csv")

# ── Summary ─────────────────────────────────────────────────────────────────
print("\n" + "="*50)
if WARN:
    print(f"[done] {len(WARN)} download(s) failed (pipeline has fallbacks):")
    for w in WARN:
        print(f"  - {w}")
else:
    print("[done] All auto-downloads complete.")
print("Manual steps above still needed for full scoring.")
