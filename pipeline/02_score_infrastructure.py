"""
Scores each tribal land unit on criteria hyperscale data center developers use.
Higher score = more attractive to a developer.

Scoring dimensions:
  - Transmission line proximity       (0-20 pts)
  - Substation capacity proximity     (0-15 pts)
  - Water availability                (0-20 pts)
  - Aquifer access                    (0-10 pts)
  - Land area (large contiguous)      (0-15 pts)
  - Terrain flatness                  (0-10 pts)
  - Fiber backbone proximity          (0-10 pts)
  - Power cost (state commercial avg) (0-15 pts)
  - Opportunity Zone overlap          (0-5 pts)
  - Flood risk penalty                (0 to -10 pts)
  Max raw score: ~120 pts -> normalized 0-1
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
    shp = (next((RAW / "transmission_lines").glob("*.geojson"), None) or
           next((RAW / "transmission_lines").glob("*.shp"), None))
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
    shp = (next((RAW / "substations").glob("*.geojson"), None) or
           next((RAW / "substations").glob("*.shp"), None))
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
    shp = (next((RAW / "fiber_optic").glob("*.geojson"), None) or
           next((RAW / "fiber_optic").glob("*.shp"), None))
    if shp is None:
        print("  [warn] Fiber optic data not found — scoring 0")
        return pd.Series(0, index=tribal.index).rename("score_fiber_proximity")

    fiber = gpd.read_file(shp).to_crs(WORKING_CRS)
    fiber_union = fiber.geometry.unary_union
    distances = tribal.geometry.centroid.distance(fiber_union) / 1000  # km
    scores = np.clip(10 * (1 - distances / 100), 0, 10)
    return scores.rename("score_fiber_proximity")


def score_power_cost(tribal: gpd.GeoDataFrame) -> pd.Series:
    """
    State-level average commercial electricity rate from EIA Form 861.
    Lower cost = better for data centers (power is ~40% of opex).
    Score: 15 pts at ≤8 ¢/kWh, 0 pts at ≥20 ¢/kWh, linear.
    """
    # 2022 EIA average commercial rates (¢/kWh) by state FIPS
    STATE_RATES = {
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
    NATIONAL_AVG = 10.7  # fallback for multi-state / unknown

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


def combine_infrastructure_scores(tribal: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Run all infrastructure scoring functions and attach results."""
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

    print("Scoring flood risk penalty...")
    tribal["score_flood_penalty"]     = score_flood_risk_penalty(tribal).values

    print("Scoring opportunity zone overlap...")
    tribal["score_opp_zone"]          = score_opportunity_zone(tribal).values

    score_cols = [
        "score_transmission", "score_substation", "score_water",
        "score_aquifer", "score_land_area", "score_terrain",
        "score_fiber_proximity", "score_power_cost",
        "score_flood_penalty", "score_opp_zone"
    ]
    tribal["corp_score_raw"] = tribal[score_cols].sum(axis=1)

    raw_min = tribal["corp_score_raw"].min()
    raw_max = tribal["corp_score_raw"].max()
    tribal["corp_score"] = (tribal["corp_score_raw"] - raw_min) / (raw_max - raw_min + 1e-9)

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
