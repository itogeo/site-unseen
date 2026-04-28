"""
Assigns opportunity tiers based solely on infrastructure siting score.
Drops vulnerability scoring — this tool is for tribal economic development,
not for flagging risk.

Opportunity Tiers:
  PRIME     — corp_score >= 0.65  (top-tier siting potential)
  HIGH      — corp_score >= 0.40  (strong candidate)
  MODERATE  — corp_score >= 0.20  (viable with some development)
  LOW       — corp_score <  0.20  (limited near-term potential)
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path

PROC = Path("data/processed")


def classify_opportunity_tier(siting_score: float) -> str:
    if siting_score >= 0.65:
        return "CRITICAL"   # stored as CRITICAL for webapp backward-compat, displayed as "Prime"
    elif siting_score >= 0.40:
        return "HIGH"        # displayed as "Strong"
    elif siting_score >= 0.20:
        return "MODERATE"
    else:
        return "LOW"


def load_existing_dc_sites() -> gpd.GeoDataFrame | None:
    """
    Optionally load known data center sites near tribal lands.
    CSV columns: tribe_name, lat, lon, status, company_name, notes
    """
    csv_path = Path("data/raw/honor_earth_tracker.csv")
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path)
    if "lat" not in df.columns or "lon" not in df.columns:
        return None

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
        crs="EPSG:4326"
    )
    print(f"  Loaded {len(gdf)} known data center sites")
    return gdf


def main():
    print("Loading infrastructure-scored tribal lands...")
    tribal = gpd.read_file(PROC / "tribal_lands_corp_scored.gpkg")
    print(f"  {len(tribal)} tribal land units")

    # siting_score = corp_score (infrastructure attractiveness, 0-1)
    tribal["siting_score"]   = tribal["corp_score"]
    tribal["combined_score"] = tribal["corp_score"]   # kept for webapp compat

    tribal["risk_tier"] = tribal["siting_score"].apply(classify_opportunity_tier)

    tier_order = {"CRITICAL": 4, "HIGH": 3, "MODERATE": 2, "LOW": 1}
    tribal["priority"] = tribal["risk_tier"].map(tier_order).fillna(1)

    # Known existing DC sites (optional)
    tribal["known_datacenter"] = False
    tribal["known_dc_status"]  = None
    tribal["known_dc_company"] = None

    dc_sites = load_existing_dc_sites()
    if dc_sites is not None:
        dc_sites = dc_sites.to_crs(tribal.crs)
        for _, site in dc_sites.iterrows():
            mask = tribal.geometry.contains(site.geometry)
            if mask.any():
                tribal.loc[mask, "known_datacenter"] = True
                if "status" in site.index:
                    tribal.loc[mask, "known_dc_status"] = site.get("status")
                if "company_name" in site.index:
                    tribal.loc[mask, "known_dc_company"] = site.get("company_name")

    print("\n=== Opportunity Tier Summary ===")
    tier_counts = tribal["risk_tier"].value_counts()
    display = {"CRITICAL": "Prime", "HIGH": "Strong", "MODERATE": "Moderate", "LOW": "Low"}
    for tier in ["CRITICAL", "HIGH", "MODERATE", "LOW"]:
        count = tier_counts.get(tier, 0)
        print(f"  {display[tier]:10s}: {count:4d} tribal land units")

    print(f"\nKnown data center sites: {tribal['known_datacenter'].sum()}")

    print("\nTop 20 Prime + Strong tribal lands:")
    top = tribal[tribal["risk_tier"].isin(["CRITICAL", "HIGH"])].nlargest(20, "siting_score")[
        ["tribe_name", "risk_tier", "siting_score", "area_km2", "known_datacenter"]
    ]
    print(top.to_string())

    out = PROC / "tribal_datacenter_risk_full.gpkg"
    tribal.to_file(out, driver="GPKG")
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
