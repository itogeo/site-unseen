"""Basic sanity checks on scoring output."""

import geopandas as gpd
import pytest
from pathlib import Path

OUTPUT = Path("output")
PROC   = Path("data/processed")


def test_output_exists():
    assert (OUTPUT / "tribal_datacenter_risk.geojson").exists()
    assert (OUTPUT / "stats.json").exists()


def test_scores_in_range():
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    assert gdf["corp_score"].between(0, 1).all(),      "corp_score out of 0-1 range"
    assert gdf["vuln_score"].between(0, 1).all(),      "vuln_score out of 0-1 range"
    assert gdf["combined_score"].between(0, 1).all(),  "combined_score out of 0-1 range"


def test_risk_tiers_valid():
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    valid_tiers = {"CRITICAL", "HIGH", "MODERATE", "LOW"}
    assert set(gdf["risk_tier"].unique()).issubset(valid_tiers)


def test_no_null_geometries():
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    assert gdf.geometry.notna().all(),     "Null geometries found"
    assert (~gdf.geometry.is_empty).all(), "Empty geometries found"


def test_critical_tier_exists():
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    critical = gdf[gdf["risk_tier"] == "CRITICAL"]
    assert len(critical) > 0, "No CRITICAL tier results — check scoring weights"
    print(f"  {len(critical)} CRITICAL tier tribal lands identified")


def test_score_variance():
    """Scores should have meaningful variance, not all clumped."""
    gdf = gpd.read_file(OUTPUT / "tribal_datacenter_risk.geojson")
    assert gdf["corp_score"].std() > 0.05, "corp_score has very low variance"
    assert gdf["vuln_score"].std() > 0.05, "vuln_score has very low variance"
