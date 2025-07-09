"""General utilities shared across rules."""

from typing import Literal

import geopandas as gpd
import pandas as pd
from pandas.api.types import is_list_like

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
    """Vectorised combination of string columns with prefix, suffix and separators.

    Form: {prefix}col1{sep}col2{sep}...coln{suffix}.
    """
    return prefix + raw[cols].astype(str).agg(sep.join, axis="columns") + suffix


def check_single_category(df: pd.DataFrame) -> str:
    """Quick validation for single-category datasets."""
    categories = df["category"].unique()
    if len(categories) != 1:
        raise ValueError(f"Cannot impute multi-category datasets. Found '{categories}'")
    return categories[0]


def listify(item) -> list:
    """Avoids ambiguity in YAML list parameters."""
    return item if is_list_like(item) else [item]


def filter_years(
    powerplants_df: pd.DataFrame,
    year: int,
    how: Literal["operating", "future"] = "operating",
) -> pd.DataFrame:
    """Standardised filtering of powerplants based on start / end years.

    Args:
        powerplants_df (pd.DataFrame): powerplant dataset to filter.
        year (int): year to filter.
        how (Literal["operating", "future"], optional): filtering approach. Defaults to "operating".
        - "operating": only powerplants active in the given year.
        - "future": powerplants active and planned projects in the given year.

    Returns:
        pd.DataFrame: copy of the given dataframe after filtering applied.
    """
    if how == "operating":
        filtered = powerplants_df[
            (powerplants_df["start_year"] <= year) & (year < powerplants_df["end_year"])
        ].copy()
    elif how == "future":
        filtered = powerplants_df[(powerplants_df["start_year"] <= year)].copy()
    return filtered
