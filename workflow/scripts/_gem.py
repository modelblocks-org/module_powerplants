"""General utilities for procesing GEM datasets."""

import re
from typing import Literal

import numpy as np
import pandas as pd

GEM_GSPT_SHEETS = ["20 MW+", "1-20 MW"]
GSPT_CAPACITY_RATING_MAPPING = {"MWac": "AC", "MWp/dc": "DC"}

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


def output_capacity_mw_gspt(
    gem_df: pd.DataFrame, dc_ac_ratio: float, default_rating: Literal["AC", "DC"]
):
    """Obtain capacity, applying DC-to-AC ratios where necessary."""
    rating = (
        gem_df["Capacity Rating"]
        .fillna("unknown")
        .replace("unknown", default_rating)
        .replace(GSPT_CAPACITY_RATING_MAPPING)
    )
    invalid_ratings = set(rating.unique()) - set(GSPT_CAPACITY_RATING_MAPPING.values())
    assert not invalid_ratings, f"GEM GSPT: invalid capacity ratings {invalid_ratings}."

    cap_df = pd.concat([gem_df["Capacity (MW)"], rating], axis="columns")
    return cap_df.apply(
        lambda x: x["Capacity (MW)"]
        if x["Capacity Rating"] == "AC"
        else x["Capacity (MW)"] / dc_ac_ratio,
        axis="columns",
    )
