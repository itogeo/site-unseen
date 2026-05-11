"""
Scores each tribal land unit on criteria hyperscale data center developers use.
Higher score = more attractive to a developer.

Infrastructure scoring:
  - Transmission line proximity       (0-20 pts)
  - Substation capacity proximity     (0-15 pts)
  - Water availability                (0-20 pts)
  - Aquifer access                    (0-10 pts)
  - Land area (large contiguous)      (0-15 pts)
  - Terrain flatness                  (0-10 pts)
  - Fiber backbone proximity          (0-10 pts)
  - Power cost (state commercial avg) (0-15 pts)
  - Highway (motorway) proximity      (0-10 pts)
  - IXP colocation hub proximity      (0-15 pts)
  - Opportunity Zone overlap          (0-5 pts)
  - Flood risk penalty                (0 to -10 pts)

Economic / regulatory scoring:
  - State corporate income tax rate   (0-10 pts)
  - State DC tax incentive programs   (0-10 pts)
  - Major metro demand proximity      (0-10 pts)
  - State renewable energy mix        (0-10 pts)
  - Climate / cooling efficiency      (0-8 pts)
  - Natural disaster hazard penalty   (-6 to 0 pts)
  - Tech workforce availability       (0-8 pts)

  Max raw score: ~191 pts -> normalized 0-1
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

RAW  = Path("data/raw")
PROC = Path("data/processed")
WORKING_CRS = "EPSG:5070"


def load_tribal_lands() -> gpd.GeoDataFrame:
    return gpd.read_file(PROC / "tribal_lands.gpkg")


def score_transmission_proximity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Distance to nearest high-voltage (>= 115kV) transmission line.
    Score: 20 pts at 0km, 0 pts at 50km+
    """
    candidates = [
        RAW / "overlays" / "transmission_lines.geojson",
        *list((RAW / "transmission_lines").glob("*.geojson")),
        *list((RAW / "transmission_lines").glob("*.shp")),
    ]
    shp = next((p for p in candidates if p.exists()), None)
    if shp is None:
        print("  [warn] Transmission lines not found — scoring 0")
        return pd.Series(0, index=tribal.index)

    lines = gpd.read_file(shp).to_crs(WORKING_CRS)

    volt_cols = [c for c in lines.columns if "volt" in c.lower() or "kv" in c.lower()]
    if volt_cols:
        col = volt_cols[0]
        lines[col] = pd.to_numeric(lines[col], errors="coerce")
        hv_lines = lines[lines[col] >= 115].copy()
        if len(hv_lines) < 100:
            hv_lines = lines
    else:
        hv_lines = lines

    hv_union = hv_lines.geometry.unary_union
    distances = tribal.geometry.centroid.distance(hv_union) / 1000  # km
    scores = np.clip(20 * (1 - distances / 50), 0, 20)
    return scores.rename("score_transmission")


def score_substation_proximity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Distance to nearest substation >= 115kV.
    Score: 15 pts at 0km, 0 pts at 30km+
    """
    candidates = [
        RAW / "overlays" / "substations.geojson",
        *list((RAW / "substations").glob("*.geojson")),
        *list((RAW / "substations").glob("*.shp")),
    ]
    shp = next((p for p in candidates if p.exists()), None)
    if shp is None:
        print("  [warn] Substations not found — scoring 0")
        return pd.Series(0, index=tribal.index)

    subs = gpd.read_file(shp).to_crs(WORKING_CRS)

    volt_cols = [c for c in subs.columns if "volt" in c.lower() or "kv" in c.lower() or "max_volt" in c.lower()]
    if volt_cols:
        col = volt_cols[0]
        subs[col] = pd.to_numeric(subs[col], errors="coerce")
        hv_subs = subs[subs[col] >= 115].copy()
        if len(hv_subs) < 50:
            hv_subs = subs
    else:
        hv_subs = subs

    sub_union = hv_subs.geometry.unary_union
    distances = tribal.geometry.centroid.distance(sub_union) / 1000  # km
    scores = np.clip(15 * (1 - distances / 30), 0, 15)
    return scores.rename("score_substation")


def score_water_availability(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Proximity to perennial water bodies / rivers (NHD).
    Score: 20 pts intersecting or adjacent, decay to 0 at 25km.
    """
    nhd_dir = RAW / "nhd_water"
    water_files = list(nhd_dir.glob("**/*.shp"))

    if not water_files:
        print("  [warn] NHD water data not found — using tribal water_area as proxy")
        if "water_area_sqm" in tribal.columns:
            water_km2 = tribal["water_area_sqm"] / 1_000_000
            scores = np.clip(20 * water_km2 / 50, 0, 20)
        else:
            scores = pd.Series(10, index=tribal.index)
        return scores.rename("score_water")

    water_gdf = None
    for f in water_files:
        name = f.stem.lower()
        if "flowline" in name or "waterbody" in name or "nhd" in name:
            try:
                water_gdf = gpd.read_file(f).to_crs(WORKING_CRS)
                if "FCode" in water_gdf.columns:
                    water_gdf = water_gdf[water_gdf["FCode"].isin([46006, 39004, 39009])]
                break
            except Exception:
                continue

    if water_gdf is None or len(water_gdf) == 0:
        print("  [warn] Could not parse NHD — defaulting water score to 10")
        return pd.Series(10, index=tribal.index).rename("score_water")

    water_union = water_gdf.geometry.unary_union
    distances = tribal.geometry.centroid.distance(water_union) / 1000  # km
    scores = np.clip(20 * (1 - distances / 25), 0, 20)
    return scores.rename("score_water")


def score_aquifer_access(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Whether tribal land overlaps a principal aquifer.
    Score: 10 pts if overlapping, 0 if not.
    """
    shp = next((RAW / "aquifers").glob("*.shp"), None)
    if shp is None:
        print("  [warn] Aquifer shapefile not found — scoring 5 (neutral)")
        return pd.Series(5, index=tribal.index).rename("score_aquifer")

    aquifers = gpd.read_file(shp).to_crs(WORKING_CRS)
    aquifer_union = aquifers.geometry.unary_union
    overlaps = tribal.geometry.intersects(aquifer_union).astype(int) * 10
    return overlaps.rename("score_aquifer")


def score_land_area(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Large contiguous land area. Score: 15 pts at >= 500 km², log scaled.
    """
    area = tribal["area_km2"] if "area_km2" in tribal.columns else \
           tribal.geometry.area / 1_000_000
    scores = np.clip(15 * np.log1p(area) / np.log1p(500), 0, 15)
    return scores.rename("score_land_area")


def score_terrain_flatness(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Flat terrain reduces grading costs. Proxy: bbox aspect ratio.
    TODO: implement rasterstats mean slope once 3DEP tiles downloaded.
    Score: 0-10 pts
    """
    bounds = tribal.geometry.bounds
    width  = bounds["maxx"] - bounds["minx"]
    height = bounds["maxy"] - bounds["miny"]
    aspect = np.minimum(width, height) / (np.maximum(width, height) + 1e-9)
    scores = np.clip(10 * aspect, 0, 10)
    return scores.rename("score_terrain")


def score_fiber_proximity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Distance to nearest long-haul fiber optic backbone cable.
    Score: 10 pts at 0km, 0 pts at 100km
    """
    candidates = [
        RAW / "overlays" / "fiber_optic.geojson",
        *list((RAW / "fiber_optic").glob("*.geojson")),
        *list((RAW / "fiber_optic").glob("*.shp")),
    ]
    shp = next((p for p in candidates if p.exists()), None)
    if shp is None:
        print("  [warn] Fiber optic data not found — scoring 0")
        return pd.Series(0, index=tribal.index).rename("score_fiber_proximity")

    fiber = gpd.read_file(shp).to_crs(WORKING_CRS)
    fiber_union = fiber.geometry.unary_union
    distances = tribal.geometry.centroid.distance(fiber_union) / 1000  # km
    scores = np.clip(10 * (1 - distances / 100), 0, 10)
    return scores.rename("score_fiber_proximity")


def score_highway_proximity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Distance to nearest interstate highway (motorway).
    Score: 10 pts at 0km, 0 pts at 100km. Proxy for logistics / construction access.
    """
    candidates = [
        RAW / "overlays" / "highways.geojson",
        *list((RAW / "highways").glob("*.geojson")),
        *list((RAW / "highways").glob("*.shp")),
    ]
    shp = next((p for p in candidates if p.exists()), None)
    if shp is None:
        print("  [warn] Highways data not found — scoring 0")
        return pd.Series(0, index=tribal.index).rename("score_highway_proximity")

    hw = gpd.read_file(shp).to_crs(WORKING_CRS)
    hw_union = hw.geometry.unary_union
    distances = tribal.geometry.centroid.distance(hw_union) / 1000  # km
    scores = np.clip(10 * (1 - distances / 100), 0, 10)
    return scores.rename("score_highway_proximity")


def score_ixp_proximity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Distance to nearest major US internet exchange point.
    Score: 15 pts within 500km, decaying to 0 at 2500km.
    """
    import shapely.geometry as sg
    IXP_COORDS = [
        (-77.487, 39.043), (-74.006, 40.713), (-87.629, 41.878),
        (-96.797, 32.776), (-121.886, 37.338), (-118.244, 34.052),
        (-84.388, 33.749), (-122.335, 47.608), (-104.990, 39.739),
        (-80.197, 25.775), (-71.059, 42.360), (-112.074, 33.449),
        (-93.265, 44.977), (-122.676, 45.523),
    ]
    ixp_gdf = gpd.GeoDataFrame(
        geometry=[sg.Point(lon, lat) for lon, lat in IXP_COORDS],
        crs="EPSG:4326",
    ).to_crs(WORKING_CRS)
    ixp_union = ixp_gdf.geometry.unary_union

    centroids = tribal.geometry.centroid
    distances = centroids.distance(ixp_union) / 1000  # km
    scores = np.clip(15 * (1 - distances / 2500), 0, 15)
    return scores.rename("score_ixp_proximity")


_POWER_RATES = {
    '01': 12.5, '02': 19.8, '04': 10.4, '05':  8.2, '06': 20.9,
    '08': 11.4, '09': 19.1, '10': 12.7, '11': 12.8, '12': 10.9,
    '13': 10.7, '15': 33.7, '16':  8.4, '17':  9.9, '18':  9.5,
    '19': 10.1, '20':  9.6, '21':  9.7, '22':  9.6, '23': 15.4,
    '24': 13.0, '25': 18.6, '26': 10.3, '27': 11.0, '28':  9.6,
    '29':  9.3, '30': 10.2, '31':  9.5, '32': 10.5, '33': 17.7,
    '34': 14.1, '35': 10.6, '36': 17.3, '37': 10.7, '38': 10.1,
    '39':  9.9, '40':  8.4, '41': 10.5, '42': 10.8, '44': 20.1,
    '45': 10.7, '46':  9.2, '47': 10.3, '48':  9.5, '49':  8.2,
    '50': 18.9, '51': 10.3, '53':  9.8, '54':  9.7, '55': 10.3,
    '56':  8.1,
}
_POWER_RATES_NATL = 10.7


def score_power_cost(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    State-level average commercial electricity rate from EIA Form 861.
    Lower cost = better for data centers (power is ~40% of opex).
    Score: 15 pts at ≤8 ¢/kWh, 0 pts at ≥20 ¢/kWh, linear.
    """
    STATE_RATES = dict(_POWER_RATES)  # local copy so EIA override doesn't mutate module dict
    NATIONAL_AVG = _POWER_RATES_NATL

    # Try to parse EIA 861 to refine rates
    eia_zip = RAW / "eia861_rates.zip"
    if eia_zip.exists():
        try:
            import zipfile as _zipfile
            with _zipfile.ZipFile(eia_zip) as z:
                xlsx_files = [n for n in z.namelist() if 'Sales_Ult_Cust' in n and n.endswith('.xlsx')]
            if xlsx_files:
                with _zipfile.ZipFile(eia_zip) as z:
                    df_raw = pd.read_excel(z.open(xlsx_files[0]), skiprows=2)

                # Find state column and commercial revenue/sales columns
                str_cols = [c for c in df_raw.columns if 'state' in str(c).lower()]
                rev_cols = [c for c in df_raw.columns if 'commercial' in str(c).lower() and 'revenue' in str(c).lower()]
                mwh_cols = [c for c in df_raw.columns if 'commercial' in str(c).lower() and ('sales' in str(c).lower() or 'mwh' in str(c).lower())]

                if str_cols and rev_cols and mwh_cols:
                    ST_FIPS = {
                        'AL':'01','AK':'02','AZ':'04','AR':'05','CA':'06','CO':'08','CT':'09',
                        'DE':'10','DC':'11','FL':'12','GA':'13','HI':'15','ID':'16','IL':'17',
                        'IN':'18','IA':'19','KS':'20','KY':'21','LA':'22','ME':'23','MD':'24',
                        'MA':'25','MI':'26','MN':'27','MS':'28','MO':'29','MT':'30','NE':'31',
                        'NV':'32','NH':'33','NJ':'34','NM':'35','NY':'36','NC':'37','ND':'38',
                        'OH':'39','OK':'40','OR':'41','PA':'42','RI':'44','SC':'45','SD':'46',
                        'TN':'47','TX':'48','UT':'49','VT':'50','VA':'51','WA':'53','WV':'54',
                        'WI':'55','WY':'56',
                    }
                    df_raw[str_cols[0]] = df_raw[str_cols[0]].astype(str).str.strip().str.upper()
                    df_raw[rev_cols[0]] = pd.to_numeric(df_raw[rev_cols[0]], errors='coerce')
                    df_raw[mwh_cols[0]] = pd.to_numeric(df_raw[mwh_cols[0]], errors='coerce')
                    grp = df_raw.groupby(str_cols[0])[[rev_cols[0], mwh_cols[0]]].sum()
                    for abbr, fips in ST_FIPS.items():
                        if abbr in grp.index:
                            rev = grp.loc[abbr, rev_cols[0]]
                            mwh = grp.loc[abbr, mwh_cols[0]]
                            if mwh > 0:
                                STATE_RATES[fips] = round(rev * 100 / mwh, 2)
                    print(f"  [eia861] Updated state rates from {xlsx_files[0]}")
        except Exception as e:
            print(f"  [warn] EIA 861 parse failed ({e}) — using hardcoded 2022 rates")

    if 'state_fips' not in tribal.columns:
        print("  [warn] state_fips column missing — using national average for power cost")
        rate = NATIONAL_AVG
        scores = pd.Series(np.clip(15 * (1 - (rate - 8) / 12), 0, 15), index=tribal.index)
        return scores.rename("score_power_cost")

    def fips_to_rate(fips):
        if pd.isna(fips) or str(fips).strip() in ('', '00', '0'):
            return NATIONAL_AVG
        key = str(fips).strip().zfill(2)
        return STATE_RATES.get(key, NATIONAL_AVG)

    rates = tribal['state_fips'].apply(fips_to_rate)
    scores = np.clip(15 * (1 - (rates - 8) / 12), 0, 15)
    return scores.rename("score_power_cost")


def score_flood_risk_penalty(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Flood zone overlap is a disqualifier for most developers.
    Score: 0 (no penalty) to -10 (high overlap)
    """
    fema_dir = RAW / "fema_flood"
    fema_files = list(fema_dir.glob("**/*.shp"))

    if not fema_files:
        print("  [warn] FEMA flood data not found — no penalty applied")
        return pd.Series(0, index=tribal.index).rename("score_flood_penalty")

    flood_zones = []
    for f in fema_files[:5]:
        try:
            gdf = gpd.read_file(f).to_crs(WORKING_CRS)
            if "ZONE" in gdf.columns or "FLD_ZONE" in gdf.columns:
                zcol = "FLD_ZONE" if "FLD_ZONE" in gdf.columns else "ZONE"
                high_risk = gdf[gdf[zcol].str.startswith("A", na=False)].copy()
                flood_zones.append(high_risk)
        except Exception:
            continue

    if not flood_zones:
        return pd.Series(0, index=tribal.index).rename("score_flood_penalty")

    flood_gdf = gpd.GeoDataFrame(pd.concat(flood_zones), crs=WORKING_CRS)
    flood_union = flood_gdf.geometry.unary_union

    def flood_fraction(geom):
        try:
            intersection = geom.intersection(flood_union)
            return intersection.area / geom.area
        except Exception:
            return 0

    fractions = tribal.geometry.apply(flood_fraction)
    penalties = -10 * fractions
    return penalties.rename("score_flood_penalty")


def _derive_state_fips(tribal: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Spatial join: assigns state_fips to each tribal land from Census TIGER state boundaries.
    Downloads the shapefile once and caches it at data/raw/us_states/.
    """
    states_dir = RAW / "us_states"
    states_shp = states_dir / "tl_2023_us_state.shp"
    states_zip = states_dir / "tl_2023_us_state.zip"

    if not states_shp.exists():
        import urllib.request
        states_dir.mkdir(parents=True, exist_ok=True)
        url = "https://www2.census.gov/geo/tiger/TIGER2023/STATE/tl_2023_us_state.zip"
        print(f"  Downloading Census TIGER state boundaries → {states_zip}")
        urllib.request.urlretrieve(url, states_zip)
        import zipfile
        with zipfile.ZipFile(states_zip) as z:
            z.extractall(states_dir)
        print("  Extracted state boundaries")

    states = gpd.read_file(states_shp).to_crs(WORKING_CRS)
    states = states[["STATEFP", "STUSPS", "geometry"]].copy()

    centroids = tribal.copy()
    centroids["geometry"] = tribal.geometry.centroid
    joined = gpd.sjoin(
        centroids[["geometry"]],
        states.rename(columns={"STATEFP": "state_fips", "STUSPS": "state_abbr"}),
        how="left",
        predicate="within",
    )
    tribal = tribal.copy()
    tribal["state_fips"] = joined["state_fips"].values
    tribal["state_abbr"] = joined["state_abbr"].values
    assigned = tribal["state_fips"].notna().sum()
    print(f"  state_fips assigned to {assigned}/{len(tribal)} tribal lands")
    return tribal


# ── Economic / regulatory tables (hardcoded from public data sources) ──────────

# 2024 state corporate income tax top rates (%) — Tax Foundation
_CORP_TAX = {
    '01': 6.5,  '02': 9.4,  '04': 4.9,  '05': 5.3,  '06': 8.84,
    '08': 4.4,  '09': 7.5,  '10': 8.7,  '11': 8.25, '12': 5.5,
    '13': 5.75, '15': 6.4,  '16': 5.8,  '17': 9.5,  '18': 4.9,
    '19': 8.4,  '20': 7.0,  '21': 5.0,  '22': 7.5,  '23': 8.93,
    '24': 8.25, '25': 8.0,  '26': 6.0,  '27': 9.8,  '28': 5.0,
    '29': 4.0,  '30': 6.75, '31': 5.84, '32': 0.0,  '33': 7.6,
    '34': 9.0,  '35': 5.9,  '36': 7.25, '37': 2.5,  '38': 4.31,
    '39': 0.0,  '40': 4.0,  '41': 7.6,  '42': 8.99, '44': 7.0,
    '45': 5.0,  '46': 0.0,  '47': 6.5,  '48': 0.0,  '49': 4.85,
    '50': 8.5,  '51': 6.0,  '53': 0.0,  '54': 6.5,  '55': 7.9,  '56': 0.0,
}
_CORP_TAX_NATL = 6.0

# Data center–specific tax incentives (0-10) — sales tax exemptions, abatements
# Source: state economic development agencies, datacentermap.com
_DC_INCENTIVE = {
    '04': 10, '13': 10, '49': 10,  # AZ, GA, UT — flagship programs
    '19':  9, '39':  9, '37':  9, '56':  9, '46':  9, '32':  9,  # IA,OH,NC,WY,SD,NV
    '48':  8, '41':  8, '31':  8, '16':  7, '35':  7, '51':  7, '08':  7,
    '55':  6, '21':  6, '47':  6, '45':  6, '18':  6, '29':  6,
    '28':  5, '40':  5, '20':  5, '12':  5, '26':  5,
    '17':  4, '27':  4, '24':  4, '11':  3,
    '06':  3, '25':  3, '23':  3, '09':  4, '50':  3,
}
_DC_INCENTIVE_NATL = 4

# State renewable energy share of net generation (%) — EIA Electric Power Monthly 2023
_RENEWABLE_PCT = {
    '02': 33, '01': 16, '04': 17, '05': 18, '06': 37, '08': 34,
    '09': 16, '10':  5, '11':  4, '12':  7, '13': 13, '15': 34,
    '16': 83, '17': 11, '18': 11, '19': 59, '20': 44, '21':  9,
    '22':  7, '23': 84, '24':  8, '25': 21, '26': 12, '27': 28,
    '28':  6, '29': 13, '30': 56, '31': 27, '32': 30, '33': 32,
    '34': 10, '35': 30, '36': 29, '37': 16, '38': 31, '39':  8,
    '40': 33, '41': 71, '42':  9, '44': 18, '45':  9, '46': 79,
    '47': 22, '48': 27, '49': 22, '50': 99, '51': 12, '53': 76,
    '54':  8, '55': 16, '56': 28,
}
_RENEWABLE_NATL = 22

# Annual mean temperature (°F) — NOAA 30-year climate normals
# Cooler = lower PUE = cheaper to cool a data center
_MEAN_TEMP_F = {
    '02': 28, '30': 43, '38': 41, '56': 44, '16': 45, '27': 42,
    '46': 45, '23': 44, '55': 46, '50': 43, '08': 49, '33': 46,
    '31': 50, '26': 47, '53': 48, '41': 48, '19': 50, '36': 48,
    '32': 52, '25': 48, '09': 50, '34': 53, '44': 51, '42': 51,
    '39': 51, '18': 52, '17': 53, '49': 49, '06': 60, '29': 57,
    '54': 54, '20': 56, '51': 57, '10': 56, '24': 57, '11': 58,
    '21': 57, '37': 60, '47': 59, '04': 65, '35': 54, '05': 62,
    '45': 64, '13': 64, '48': 67, '01': 65, '28': 65, '22': 69,
    '12': 71, '15': 75, '40': 60, '14': 77, '72': 78,
}
_MEAN_TEMP_NATL = 53

# Natural disaster risk penalty (0 to -6) — combined wildfire, earthquake, hurricane
_HAZARD_PENALTY = {
    '06': -6,  # CA — wildfire + earthquake
    '53': -4,  # WA — Cascadia earthquake + wildfire
    '41': -4,  # OR — earthquake + wildfire
    '02': -4,  # AK — earthquake + volcano
    '12': -4,  # FL — hurricane
    '22': -3,  # LA — hurricane
    '32': -3,  # NV — seismic
    '49': -3,  # UT — Wasatch Front seismic
    '04': -2,  # AZ — wildfire
    '16': -2,  # ID — wildfire
    '30': -2,  # MT — wildfire
    '08': -2,  # CO — wildfire
    '35': -2,  # NM — wildfire
    '48': -2,  # TX — tornado + Gulf coast hurricane
    '29': -2,  # MO — New Madrid fault
    '56': -1,  # WY — some wildfire
    '05': -1,  # AR — New Madrid fringe
    '47': -1,  # TN — New Madrid fringe
    '45': -1,  # SC — Charleston seismic
    '36': -1,  # NY — Ramapo fault
}

# IT workers per 1000 employed (0-8 scale) — BLS OES 2022 state estimates
_TECH_WORKFORCE = {
    '53': 8, '06': 8, '51': 8,  # WA, CA, VA — top tech labor markets
    '08': 7, '48': 7, '25': 7, '11': 7, '36': 7,  # CO,TX,MA,DC,NY
    '13': 6, '17': 6, '37': 6, '41': 6, '04': 6, '27': 6, '32': 6, '34': 6, '09': 6, '24': 6,
    '39': 5, '26': 5, '42': 5, '55': 5, '18': 5, '12': 5, '29': 5, '49': 5,
    '31': 4, '40': 4, '20': 4, '16': 4, '35': 4, '19': 4,
}
_TECH_WORKFORCE_NATL = 3


def score_opportunity_zone(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Opportunity Zone overlap = major tax incentive for developers.
    Score: +5 pts if overlapping.
    """
    oz_file = RAW / "opportunity_zones.zip"
    if not oz_file.exists():
        print("  [warn] Opportunity Zones not found — scoring 0")
        return pd.Series(0, index=tribal.index).rename("score_opp_zone")

    import zipfile
    with zipfile.ZipFile(oz_file) as z:
        shp_name = [n for n in z.namelist() if n.endswith(".shp")]
        if not shp_name:
            return pd.Series(0, index=tribal.index).rename("score_opp_zone")
        oz = gpd.read_file(z.open(shp_name[0])).to_crs(WORKING_CRS)

    oz_union = oz.geometry.unary_union
    overlaps = tribal.geometry.intersects(oz_union).astype(int) * 5
    return overlaps.rename("score_opp_zone")


def _state_lookup(tribal: gpd.GeoDataFrame, table: dict, default) -> pd.Series:
    """Map state_fips column → table value, falling back to default."""
    def get(fips):
        if pd.isna(fips) or str(fips).strip() in ('', '00', '0'):
            return default
        return table.get(str(fips).strip().zfill(2), default)
    return tribal['state_fips'].apply(get)


def score_corp_tax_rate(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    State corporate income tax rate (Tax Foundation 2024).
    Score: 10 pts at 0%, 0 pts at 12%+. Lower tax = more profitable.
    """
    if 'state_fips' not in tribal.columns:
        rates = pd.Series(_CORP_TAX_NATL, index=tribal.index)
    else:
        rates = _state_lookup(tribal, _CORP_TAX, _CORP_TAX_NATL)
    scores = np.clip(10 * (1 - rates / 12), 0, 10)
    return scores.rename("score_corp_tax")


def score_dc_incentives(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    State data center tax incentive programs (sales tax exemptions, abatements).
    Score: 0-10 pts based on program strength.
    """
    if 'state_fips' not in tribal.columns:
        return pd.Series(_DC_INCENTIVE_NATL, index=tribal.index, dtype=float).rename("score_dc_incentives")
    scores = _state_lookup(tribal, _DC_INCENTIVE, _DC_INCENTIVE_NATL).astype(float)
    return scores.rename("score_dc_incentives")


def score_metro_proximity(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Distance to nearest large US urban area (Census TIGER Urban Areas, top 200 by land area).
    Score: 10 pts within 50km, 0 pts at 500km+. Proxy for enterprise demand + workforce.
    """
    ua_shp = RAW / "urban_areas" / "tl_2023_us_uac20.shp"
    if not ua_shp.exists():
        print("  [warn] Urban areas shapefile not found — metro proximity = 0")
        return pd.Series(0, index=tribal.index).rename("score_metro_proximity")

    ua = gpd.read_file(ua_shp)
    ua = ua.sort_values("ALAND20", ascending=False).head(200).to_crs(WORKING_CRS)
    ua_union = ua.geometry.unary_union

    distances = tribal.geometry.centroid.distance(ua_union) / 1000  # km
    scores = np.clip(10 * (1 - (distances - 50) / 450), 0, 10)
    return scores.rename("score_metro_proximity")


def score_renewable_energy(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    State renewable energy share of net generation (EIA 2023).
    Score: 10 pts at 70%+, 0 pts at 0%. Green DCs need renewable power commitments.
    """
    if 'state_fips' not in tribal.columns:
        pct = pd.Series(_RENEWABLE_NATL, index=tribal.index)
    else:
        pct = _state_lookup(tribal, _RENEWABLE_PCT, _RENEWABLE_NATL)
    scores = np.clip(10 * pct / 70, 0, 10)
    return scores.rename("score_renewable_energy")


def score_climate_cooling(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Annual mean temperature — colder climates reduce data center cooling costs (PUE).
    Score: 8 pts at ≤35°F, 0 pts at ≥80°F. NOAA 30-year normals.
    """
    if 'state_fips' not in tribal.columns:
        temp_f = pd.Series(_MEAN_TEMP_NATL, index=tribal.index)
    else:
        temp_f = _state_lookup(tribal, _MEAN_TEMP_F, _MEAN_TEMP_NATL)
    scores = np.clip(8 * (80 - temp_f) / 45, 0, 8)
    return scores.rename("score_climate_cooling")


def score_natural_hazard(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    Natural disaster risk penalty — combined wildfire, earthquake, hurricane exposure.
    Score: 0 (safe) to -6 (high risk). Source: FEMA National Risk Index, USGS seismic.
    """
    if 'state_fips' not in tribal.columns:
        return pd.Series(0, index=tribal.index).rename("score_natural_hazard")
    penalties = _state_lookup(tribal, _HAZARD_PENALTY, 0)
    return penalties.rename("score_natural_hazard")


def score_tech_workforce(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    State tech workforce availability — IT workers per 1000 employed (BLS OES 2022).
    Score: 0-8 pts. Data centers need specialized ops staff.
    """
    if 'state_fips' not in tribal.columns:
        return pd.Series(_TECH_WORKFORCE_NATL, index=tribal.index, dtype=float).rename("score_tech_workforce")
    scores = _state_lookup(tribal, _TECH_WORKFORCE, _TECH_WORKFORCE_NATL).astype(float)
    return scores.rename("score_tech_workforce")


def compute_raw_values(tribal: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Compute and attach raw (unscored) physical values for webapp display.
    Distances in km, rates in ¢/kWh, percentages, °F, etc.
    """
    tribal = tribal.copy()
    centroids = tribal.geometry.centroid

    # Transmission line distance
    candidates = [
        RAW / "overlays" / "transmission_lines.geojson",
        *list((RAW / "transmission_lines").glob("*.geojson")),
        *list((RAW / "transmission_lines").glob("*.shp")),
    ]
    shp = next((p for p in candidates if p.exists()), None)
    if shp:
        lines = gpd.read_file(shp).to_crs(WORKING_CRS)
        volt_cols = [c for c in lines.columns if "volt" in c.lower() or "kv" in c.lower()]
        if volt_cols:
            col = volt_cols[0]
            lines[col] = pd.to_numeric(lines[col], errors="coerce")
            hv = lines[lines[col] >= 115]
            if len(hv) < 100:
                hv = lines
        else:
            hv = lines
        tribal["dist_transmission_km"] = (centroids.distance(hv.geometry.unary_union) / 1000).round(1)
    else:
        tribal["dist_transmission_km"] = np.nan

    # Substation distance
    candidates = [
        RAW / "overlays" / "substations.geojson",
        *list((RAW / "substations").glob("*.geojson")),
        *list((RAW / "substations").glob("*.shp")),
    ]
    shp = next((p for p in candidates if p.exists()), None)
    if shp:
        subs = gpd.read_file(shp).to_crs(WORKING_CRS)
        volt_cols = [c for c in subs.columns if "volt" in c.lower() or "kv" in c.lower() or "max_volt" in c.lower()]
        if volt_cols:
            col = volt_cols[0]
            subs[col] = pd.to_numeric(subs[col], errors="coerce")
            hv = subs[subs[col] >= 115]
            if len(hv) < 50:
                hv = subs
        else:
            hv = subs
        tribal["dist_substation_km"] = (centroids.distance(hv.geometry.unary_union) / 1000).round(1)
    else:
        tribal["dist_substation_km"] = np.nan

    # Fiber backbone distance
    candidates = [
        RAW / "overlays" / "fiber_optic.geojson",
        *list((RAW / "fiber_optic").glob("*.geojson")),
        *list((RAW / "fiber_optic").glob("*.shp")),
    ]
    shp = next((p for p in candidates if p.exists()), None)
    if shp:
        fiber = gpd.read_file(shp).to_crs(WORKING_CRS)
        tribal["dist_fiber_km"] = (centroids.distance(fiber.geometry.unary_union) / 1000).round(1)
    else:
        tribal["dist_fiber_km"] = np.nan

    # Highway distance
    candidates = [
        RAW / "overlays" / "highways.geojson",
        *list((RAW / "highways").glob("*.geojson")),
        *list((RAW / "highways").glob("*.shp")),
    ]
    shp = next((p for p in candidates if p.exists()), None)
    if shp:
        hw = gpd.read_file(shp).to_crs(WORKING_CRS)
        tribal["dist_highway_km"] = (centroids.distance(hw.geometry.unary_union) / 1000).round(1)
    else:
        tribal["dist_highway_km"] = np.nan

    # IXP distance
    import shapely.geometry as sg
    IXP_COORDS = [
        (-77.487, 39.043), (-74.006, 40.713), (-87.629, 41.878),
        (-96.797, 32.776), (-121.886, 37.338), (-118.244, 34.052),
        (-84.388, 33.749), (-122.335, 47.608), (-104.990, 39.739),
        (-80.197, 25.775), (-71.059, 42.360), (-112.074, 33.449),
        (-93.265, 44.977), (-122.676, 45.523),
    ]
    ixp_gdf = gpd.GeoDataFrame(
        geometry=[sg.Point(lon, lat) for lon, lat in IXP_COORDS],
        crs="EPSG:4326",
    ).to_crs(WORKING_CRS)
    tribal["dist_ixp_km"] = (centroids.distance(ixp_gdf.geometry.unary_union) / 1000).round(0).astype(int)

    # Metro proximity — nearest urban area name + distance
    ua_shp = RAW / "urban_areas" / "tl_2023_us_uac20.shp"
    if ua_shp.exists():
        ua = gpd.read_file(ua_shp).sort_values("ALAND20", ascending=False).head(200).to_crs(WORKING_CRS)
        centroids_gdf = gpd.GeoDataFrame(geometry=centroids, crs=WORKING_CRS)
        joined = gpd.sjoin_nearest(
            centroids_gdf,
            ua[["NAME20", "geometry"]],
            how="left",
            distance_col="_dist_m",
        )
        tribal["dist_metro_km"] = (joined["_dist_m"].values / 1000).round(0).astype(int)
        tribal["nearest_metro"] = joined["NAME20"].values
    else:
        tribal["dist_metro_km"] = np.nan
        tribal["nearest_metro"] = ""

    # State-level lookups
    if "state_fips" in tribal.columns:
        def fips_to_rate(fips):
            if pd.isna(fips) or str(fips).strip() in ("", "00", "0"):
                return _POWER_RATES_NATL
            return _POWER_RATES.get(str(fips).strip().zfill(2), _POWER_RATES_NATL)
        tribal["power_cost_cents"]   = tribal["state_fips"].apply(fips_to_rate).round(2)
        tribal["corp_tax_pct"]       = _state_lookup(tribal, _CORP_TAX, _CORP_TAX_NATL).round(2)
        tribal["renewable_pct"]      = _state_lookup(tribal, _RENEWABLE_PCT, _RENEWABLE_NATL).astype(int)
        tribal["mean_temp_f"]        = _state_lookup(tribal, _MEAN_TEMP_F, _MEAN_TEMP_NATL).astype(int)
        tribal["dc_incentive_score"] = _state_lookup(tribal, _DC_INCENTIVE, _DC_INCENTIVE_NATL).astype(int)
        tribal["hazard_level"]       = _state_lookup(tribal, _HAZARD_PENALTY, 0).astype(int)
        tribal["tech_workforce"]     = _state_lookup(tribal, _TECH_WORKFORCE, _TECH_WORKFORCE_NATL).astype(int)
    else:
        tribal["power_cost_cents"]   = _POWER_RATES_NATL
        tribal["corp_tax_pct"]       = _CORP_TAX_NATL
        tribal["renewable_pct"]      = _RENEWABLE_NATL
        tribal["mean_temp_f"]        = _MEAN_TEMP_NATL
        tribal["dc_incentive_score"] = _DC_INCENTIVE_NATL
        tribal["hazard_level"]       = 0
        tribal["tech_workforce"]     = _TECH_WORKFORCE_NATL

    return tribal


def combine_infrastructure_scores(tribal: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Run all scoring functions and attach results."""
    # Derive state_fips via spatial join if missing
    if 'state_fips' not in tribal.columns or tribal['state_fips'].isna().all():
        print("Deriving state_fips via spatial join...")
        tribal = _derive_state_fips(tribal)

    # ── Infrastructure ─────────────────────────────────────────────────────────
    print("Scoring transmission proximity...")
    tribal["score_transmission"]  = score_transmission_proximity(tribal).values

    print("Scoring substation proximity...")
    tribal["score_substation"]    = score_substation_proximity(tribal).values

    print("Scoring water availability...")
    tribal["score_water"]         = score_water_availability(tribal).values

    print("Scoring aquifer access...")
    tribal["score_aquifer"]       = score_aquifer_access(tribal).values

    print("Scoring land area...")
    tribal["score_land_area"]     = score_land_area(tribal).values

    print("Scoring terrain flatness...")
    tribal["score_terrain"]           = score_terrain_flatness(tribal).values

    print("Scoring fiber backbone proximity...")
    tribal["score_fiber_proximity"]   = score_fiber_proximity(tribal).values

    print("Scoring power cost by state...")
    tribal["score_power_cost"]        = score_power_cost(tribal).values

    print("Scoring highway proximity...")
    tribal["score_highway_proximity"] = score_highway_proximity(tribal).values

    print("Scoring IXP colocation hub proximity...")
    tribal["score_ixp_proximity"]     = score_ixp_proximity(tribal).values

    print("Scoring flood risk penalty...")
    tribal["score_flood_penalty"]     = score_flood_risk_penalty(tribal).values

    print("Scoring opportunity zone overlap...")
    tribal["score_opp_zone"]          = score_opportunity_zone(tribal).values

    # ── Economic / regulatory ──────────────────────────────────────────────────
    print("Scoring corporate tax rate...")
    tribal["score_corp_tax"]          = score_corp_tax_rate(tribal).values

    print("Scoring data center incentive programs...")
    tribal["score_dc_incentives"]     = score_dc_incentives(tribal).values

    print("Scoring metro demand proximity...")
    tribal["score_metro_proximity"]   = score_metro_proximity(tribal).values

    print("Scoring renewable energy mix...")
    tribal["score_renewable_energy"]  = score_renewable_energy(tribal).values

    print("Scoring climate / cooling efficiency...")
    tribal["score_climate_cooling"]   = score_climate_cooling(tribal).values

    print("Scoring natural hazard penalty...")
    tribal["score_natural_hazard"]    = score_natural_hazard(tribal).values

    print("Scoring tech workforce availability...")
    tribal["score_tech_workforce"]    = score_tech_workforce(tribal).values

    score_cols = [
        # Infrastructure
        "score_transmission", "score_substation", "score_water",
        "score_aquifer", "score_land_area", "score_terrain",
        "score_fiber_proximity", "score_power_cost",
        "score_highway_proximity", "score_ixp_proximity",
        "score_flood_penalty", "score_opp_zone",
        # Economic / regulatory
        "score_corp_tax", "score_dc_incentives", "score_metro_proximity",
        "score_renewable_energy", "score_climate_cooling",
        "score_natural_hazard", "score_tech_workforce",
    ]
    tribal["corp_score_raw"] = tribal[score_cols].sum(axis=1)

    raw_min = tribal["corp_score_raw"].min()
    raw_max = tribal["corp_score_raw"].max()
    tribal["corp_score"] = (tribal["corp_score_raw"] - raw_min) / (raw_max - raw_min + 1e-9)

    print("Computing raw display values...")
    tribal = compute_raw_values(tribal)

    return tribal


def main():
    print("Loading tribal lands...")
    tribal = load_tribal_lands()
    print(f"  {len(tribal)} tribal land units")

    tribal = combine_infrastructure_scores(tribal)

    out = PROC / "tribal_lands_corp_scored.gpkg"
    tribal.to_file(out, driver="GPKG")
    print(f"\nSaved to {out}")
    print(f"Corp score range: {tribal['corp_score'].min():.3f} – {tribal['corp_score'].max():.3f}")
    print(f"Top 10 most attractive tribal lands:")
    top = tribal.nlargest(10, "corp_score")[["tribe_name", "corp_score", "area_km2"]]
    print(top.to_string())


if __name__ == "__main__":
    main()
