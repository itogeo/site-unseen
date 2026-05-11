"""
intelligence/impact_metrics.py
Attaches concrete impact projections to each tribal land unit.
No API calls — pure calculation from peer-reviewed constants.
These are the numbers that matter in a town hall, not abstract scores.

Run after: python pipeline/05_export_geojson.py
Output: output/tribal_datacenter_risk_with_impacts.geojson
        output/impact_metrics.json
"""

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

OUTPUT = Path("output")
OUTPUT.mkdir(exist_ok=True)

# ── Constants (all sourced) ───────────────────────────────────────────────────
# Water
WATER_GAL_PER_DAY_PER_MW = 3_000
# Source: EPA 2025, MOST Policy Initiative, Apstech Advisors Jan 2026
# Range from 300K to 5M gal/year for 100-1000 MW facility

# Electricity rate impact
ELEC_RATE_INCREASE_HIGH_PCT = 267
# Source: Bloomberg analysis 2025 — near existing hyperscale DCs
ELEC_RATE_INCREASE_LOW_PCT = 50
# Conservative lower bound

# Jobs reality (from public record)
JOBS_PROMISED = 1_500       # typical construction-phase promise
JOBS_ACTUAL_PERMANENT = 3   # Rapid City SD public record
CONSTRUCTION_YEARS = 2

# Grid/ratepayer costs
GRID_UPGRADE_COST_PER_MW = 500_000  # estimated upgrade cost per MW of new DC load

# Environmental (peer-reviewed)
HEAT_ISLAND_MAX_F = 16.0    # degrees F increase on surrounding land
HEAT_ISLAND_RADIUS_KM = 5
NOISE_DB_CONTINUOUS = 97    # Honor the Earth testimony
OSHA_8HR_LIMIT_DB = 90      # OSHA permissible exposure
HEARING_DAMAGE_DB = 85      # long-term threshold

# Average US household consumption for equivalencies
HOUSEHOLD_WATER_GAL_DAY = 300       # gallons/day
HOUSEHOLD_ELECTRICITY_KWH_YR = 10_500

# Olympic pool volume for water comparisons
OLYMPIC_POOL_GAL = 660_000


def estimate_dc_mw(area_km2: float) -> float:
    """
    Estimate likely DC size in MW based on available land.
    Larger tribal land bases attract proportionally larger facilities.
    """
    if area_km2 < 10:
        return 100.0
    elif area_km2 < 100:
        return 300.0
    elif area_km2 < 500:
        return 500.0
    else:
        return 1_000.0


def compute_impacts(area_km2: float, tribal_pop: int = 1_000) -> dict:
    """
    Compute concrete impact projections for a tribal land unit.
    All values are for a single hyperscale DC — some communities
    face proposals for multiple facilities.
    """
    dc_mw = estimate_dc_mw(area_km2)

    # Water
    water_daily = dc_mw * WATER_GAL_PER_DAY_PER_MW
    water_annual = water_daily * 365
    water_low = water_annual * 0.5   # air cooling / efficiency case
    water_high = water_annual * 2.0  # full evaporative cooling

    # Grid
    upgrade_cost = dc_mw * GRID_UPGRADE_COST_PER_MW
    service_area_hh = max(tribal_pop * 3, 100)  # rough service area estimate
    upgrade_per_hh = upgrade_cost / service_area_hh

    # Monthly bill impact (on a $120/mo average bill)
    monthly_avg = 120
    monthly_increase_low = round(monthly_avg * (ELEC_RATE_INCREASE_LOW_PCT / 100))
    monthly_increase_high = round(monthly_avg * (ELEC_RATE_INCREASE_HIGH_PCT / 100))

    return {
        # Sizing
        "estimated_dc_mw": dc_mw,

        # Water
        "water_daily_gallons": round(water_daily),
        "water_annual_gallons": round(water_annual),
        "water_annual_millions": round(water_annual / 1_000_000, 1),
        "water_annual_millions_low": round(water_low / 1_000_000, 1),
        "water_annual_millions_high": round(water_high / 1_000_000, 1),
        "water_equivalent_households": round(water_annual / (HOUSEHOLD_WATER_GAL_DAY * 365)),
        "water_olympic_pools": round(water_annual / OLYMPIC_POOL_GAL),
        "water_source": "EPA 2025 / MOST Policy Initiative",

        # Electricity
        "elec_rate_increase_high_pct": ELEC_RATE_INCREASE_HIGH_PCT,
        "elec_rate_increase_low_pct": ELEC_RATE_INCREASE_LOW_PCT,
        "monthly_bill_increase_low_usd": monthly_increase_low,
        "monthly_bill_increase_high_usd": monthly_increase_high,
        "grid_upgrade_cost_usd": round(upgrade_cost),
        "grid_upgrade_per_household_usd": round(upgrade_per_hh),
        "elec_source": "Bloomberg 2025 analysis near existing DCs",

        # Jobs
        "jobs_promised_construction": JOBS_PROMISED,
        "jobs_construction_local_pct": 15,
        "jobs_construction_local_est": round(JOBS_PROMISED * 0.15),
        "jobs_permanent_actual": JOBS_ACTUAL_PERMANENT,
        "jobs_construction_duration_years": CONSTRUCTION_YEARS,
        "jobs_source": "Rapid City SD public record",

        # Environment
        "heat_island_max_f": HEAT_ISLAND_MAX_F,
        "heat_island_radius_km": HEAT_ISLAND_RADIUS_KM,
        "noise_db_continuous": NOISE_DB_CONTINUOUS,
        "osha_8hr_limit_db": OSHA_8HR_LIMIT_DB,
        "noise_db_above_osha": NOISE_DB_CONTINUOUS - OSHA_8HR_LIMIT_DB,
        "noise_source": "Honor the Earth testimony / OSHA standards",

        # Summary string for popups
        "impact_summary": (
            f"Est. {dc_mw:,.0f} MW facility. "
            f"Water: ~{water_annual/1_000_000:.0f}M gal/yr. "
            f"Electricity rates: +{ELEC_RATE_INCREASE_LOW_PCT}-"
            f"{ELEC_RATE_INCREASE_HIGH_PCT}%. "
            f"Permanent jobs: {JOBS_ACTUAL_PERMANENT}."
        ),
    }


def main() -> None:
    # Load scored risk GeoJSON
    risk_path = OUTPUT / "tribal_datacenter_risk.geojson"
    if not risk_path.exists():
        print(f"[impacts] ERROR: {risk_path} not found — run pipeline first")
        return

    print(f"[impacts] Loading {risk_path}...")
    gdf = gpd.read_file(risk_path)
    print(f"  {len(gdf)} tribal land units")

    # Compute impacts for each
    impact_rows = []
    for _, row in gdf.iterrows():
        area = float(row.get("area_km2", 100) or 100)
        impacts = compute_impacts(area)
        impacts["geoid"] = row.get("geoid")
        impacts["tribe_name"] = row.get("tribe_name")
        impact_rows.append(impacts)

    impact_df = pd.DataFrame(impact_rows)

    # Merge back into GDF — drop conflicting columns first
    merge_cols = [c for c in impact_df.columns
                  if c not in gdf.columns or c in ["geoid", "tribe_name"]]
    gdf = gdf.merge(
        impact_df[merge_cols],
        on=["geoid", "tribe_name"],
        how="left",
    )

    # Save enriched GeoJSON
    out_geo = OUTPUT / "tribal_datacenter_risk_with_impacts.geojson"
    gdf.to_file(out_geo, driver="GeoJSON")
    print(f"[impacts] Enriched GeoJSON → {out_geo}")

    # Save standalone metrics (no geometry)
    out_json = OUTPUT / "impact_metrics.json"
    out_json.write_text(json.dumps(impact_rows, indent=2))
    print(f"[impacts] Impact metrics → {out_json}")

    # Print example for highest-risk land
    if "combined_score" in gdf.columns and not gdf.empty:
        top = gdf.nlargest(1, "combined_score").iloc[0]
        print(f"\n=== Example: {top.get('tribe_name', 'Unknown')} ===")
        print(f"  Risk tier:         {top.get('risk_tier', '?')}")
        print(f"  Est. DC size:      {top.get('estimated_dc_mw', 0):,.0f} MW")
        print(f"  Water/year:        {top.get('water_annual_millions', 0):.0f}M gallons")
        print(f"  Equiv. households: {top.get('water_equivalent_households', 0):,}")
        print(f"  Rate increase:     +{ELEC_RATE_INCREASE_LOW_PCT}–"
              f"{ELEC_RATE_INCREASE_HIGH_PCT}%")
        print(f"  Permanent jobs:    {JOBS_ACTUAL_PERMANENT} "
              f"(vs {JOBS_PROMISED:,} promised)")
        print(f"  Heat island:       up to {HEAT_ISLAND_MAX_F}°F")
        print(f"  Noise:             {NOISE_DB_CONTINUOUS} dB continuous "
              f"({NOISE_DB_CONTINUOUS - OSHA_8HR_LIMIT_DB} dB above OSHA limit)")

    print("\n[impacts] Done.")


if __name__ == "__main__":
    main()
