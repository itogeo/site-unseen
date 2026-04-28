"""
Merges corporate attractiveness + community vulnerability scores.
Computes combined priority score and assigns risk tiers.

Risk Tiers:
  CRITICAL  — High corp score + High vuln score (immediate alert)
  HIGH      — High on either dimension
  MODERATE  — Medium on both
  LOW       — Low attractiveness or low vulnerability
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path

PROC = Path("data/processed")


def classify_risk_tier(corp: float, vuln: float) -> str:
    """Assign a risk tier based on combined scores."""
    if corp >= 0.65 and vuln >= 0.65:
        return "CRITICAL"
    elif corp >= 0.65 or vuln >= 0.65:
        return "HIGH"
    elif corp >= 0.35 and vuln >= 0.35:
        return "MODERATE"
    else:
        return "LOW"


def load_honor_earth_known_sites() -> gpd.GeoDataFrame | None:
    """
    Load known data center sites from Honor the Earth's crowdsource tracker.
    Export their Google Form CSV and save to data/raw/honor_earth_tracker.csv.

    Expected CSV columns: tribe_name, location_description, lat, lon,
                          status, company_name, notes
    """
    csv_path = Path("data/raw/honor_earth_tracker.csv")
    if not csv_path.exists():
        print("  [info] No Honor the Earth tracker CSV found")
        print("         Export from their Google Form and save to data/raw/honor_earth_tracker.csv")
        return None

    df = pd.read_csv(csv_path)
    print(f"  Loaded {len(df)} known sites from Honor the Earth tracker")

    if "lat" in df.columns and "lon" in df.columns:
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["lon"], df["lat"]),
            crs="EPSG:4326"
        )
        return gdf

    return None


def main():
    print("Loading scored tribal lands...")
    tribal = gpd.read_file(PROC / "tribal_lands_vuln_scored.gpkg")
    print(f"  {len(tribal)} tribal land units")

    # Combined score: weight vulnerability slightly higher
    # (goal is protecting communities, not just finding attractive sites)
    w_corp = 0.45
    w_vuln = 0.55
    tribal["combined_score"] = (
        w_corp * tribal["corp_score"] +
        w_vuln * tribal["vuln_score"]
    )

    tribal["risk_tier"] = tribal.apply(
        lambda row: classify_risk_tier(row["corp_score"], row["vuln_score"]),
        axis=1
    )

    tier_order = {"CRITICAL": 5, "HIGH": 4, "MODERATE": 3, "LOW": 2}
    tribal["priority"] = tribal["risk_tier"].map(tier_order).fillna(1)

    tribal["known_datacenter"] = False
    tribal["known_dc_status"]  = None
    tribal["known_dc_company"] = None

    hte_sites = load_honor_earth_known_sites()
    if hte_sites is not None:
        hte_sites = hte_sites.to_crs(tribal.crs)
        joined = gpd.sjoin(hte_sites, tribal[["geoid", "geometry"]], how="left", predicate="within")
        confirmed_geoids = joined["geoid"].dropna().unique()
        tribal.loc[tribal["geoid"].isin(confirmed_geoids), "known_datacenter"] = True

        for _, site in hte_sites.iterrows():
            mask = tribal.geometry.contains(site.geometry)
            if mask.any():
                if "status" in site.index:
                    tribal.loc[mask, "known_dc_status"] = site.get("status")
                if "company_name" in site.index:
                    tribal.loc[mask, "known_dc_company"] = site.get("company_name")

    print("\n=== Risk Tier Summary ===")
    tier_counts = tribal["risk_tier"].value_counts()
    for tier in ["CRITICAL", "HIGH", "MODERATE", "LOW"]:
        count = tier_counts.get(tier, 0)
        print(f"  {tier:10s}: {count:4d} tribal land units")

    print(f"\nKnown active data center sites: {tribal['known_datacenter'].sum()}")

    print("\nTop 20 CRITICAL + HIGH priority tribal lands:")
    top = tribal[tribal["risk_tier"].isin(["CRITICAL", "HIGH"])].nlargest(20, "combined_score")[
        ["tribe_name", "risk_tier", "corp_score", "vuln_score", "combined_score", "known_datacenter"]
    ]
    print(top.to_string())

    out = PROC / "tribal_datacenter_risk_full.gpkg"
    tribal.to_file(out, driver="GPKG")
    print(f"\nSaved full scored dataset to {out}")


if __name__ == "__main__":
    main()
