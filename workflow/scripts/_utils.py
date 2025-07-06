"""General utilities shared across rules."""



import geopandas as gpd
import pandas as pd

# Average year where disaggregated datasets were last updated.
# MUST BE ADJUSTED WHENEVER DATASOURCES ARE UDPATED!
REFERENCE_YR = 2024


def get_point_col(
    raw: pd.DataFrame, lon_col: str, lat_col: str, crs: str = "EPSG:4326"
) -> gpd.GeoSeries:
    """Converts latitude / longitude columns to a point geometry."""
    return gpd.points_from_xy(raw[lon_col], raw[lat_col], crs=crs)


def get_combined_text_col(
    raw: pd.DataFrame,
    cols: list[str],
    sep: str = "-",
    prefix: str = "",
    suffix: str = "",
):
    return prefix + raw[cols].astype(str).agg(sep.join, axis="columns") + suffix
