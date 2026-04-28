import geopandas as gpd
import numpy as np
from pathlib import Path


def normalize_series(s, out_min=0, out_max=1):
    """Min-max normalize a pandas Series."""
    s_min, s_max = s.min(), s.max()
    if s_max == s_min:
        return s * 0 + (out_min + out_max) / 2
    return out_min + (s - s_min) / (s_max - s_min) * (out_max - out_min)


def distance_score(distances_km, max_dist_km, max_pts):
    """Linear decay score from 0 to max_dist_km."""
    return np.clip(max_pts * (1 - distances_km / max_dist_km), 0, max_pts)


def nearest_feature_distance(origins: gpd.GeoDataFrame,
                              targets: gpd.GeoDataFrame,
                              unit: str = "km") -> "pd.Series":
    """
    Compute distance from each origin centroid to nearest target feature.
    Returns Series of distances in specified unit.
    """
    target_union = targets.geometry.unary_union
    distances = origins.geometry.centroid.distance(target_union)
    if unit == "km":
        return distances / 1000
    elif unit == "m":
        return distances
    elif unit == "mi":
        return distances / 1609.34
    return distances


def print_score_summary(gdf, score_col, label):
    """Print distribution summary for a score column."""
    s = gdf[score_col]
    print(f"\n{label} ({score_col}):")
    print(f"  min={s.min():.3f}  mean={s.mean():.3f}  max={s.max():.3f}  "
          f"p25={s.quantile(0.25):.3f}  p75={s.quantile(0.75):.3f}")
