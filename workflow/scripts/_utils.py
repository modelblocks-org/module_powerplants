"""General utilities shared across rules."""

from datetime import datetime

import geopandas as gpd
import pandas as pd

CURRENT_YEAR = datetime.now().year

def get_point(
    raw: pd.DataFrame, lon_col: str, lat_col: str, crs: str = "EPSG:4326"
) -> gpd.GeoSeries:
    """Converts latitude / longitude columns to a point geometry."""
    return gpd.points_from_xy(raw[lon_col], raw[lat_col], crs=crs)
