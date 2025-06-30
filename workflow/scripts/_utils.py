"""General utilities shared across rules."""

import re
from datetime import datetime
from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd

CURRENT_YEAR = datetime.now().year


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


_INVALID_STATUS_VALUES = ["cancelled", "shelved"]
_DROPPED_NA_COLUMNS = ["Capacity (MW)", "Latitude", "Longitude"]


def read_gem_dataset(path: str, sheets: list[str]) -> pd.DataFrame:
    """Get a GEM dataset for a type/category of powerplant."""
    gem_df = pd.concat([pd.read_excel(path, sheet) for sheet in sheets], axis="index")
    # Get raw dataset without cancelled projects
    pattern = "|".join(map(re.escape, _INVALID_STATUS_VALUES))
    mask = gem_df["Status"].str.contains(pattern, na=False)
    gem_df = gem_df[~mask]
    # Remove rows with problematic empty values
    gem_df = gem_df.dropna(subset=_DROPPED_NA_COLUMNS)
    gem_df = gem_df.reset_index(drop=True)
    return gem_df


def gem_year_col(gem_df: pd.DataFrame, option: Literal["start", "end"]):
    """Get start/end year, ensuring typing is respected."""
    mapping = {"start": "Start year", "end": "Retired year"}
    return gem_df[mapping[option]].apply(lambda x: np.nan if x == "not found" else x)
