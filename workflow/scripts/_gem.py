"""General utilities for procesing GEM datasets."""

import re
from typing import Literal

import numpy as np
import pandas as pd

GEM_GWPT_SHEETS = ["Data", "Below Threshold"]

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


def technology_col(
    gem_df: pd.DataFrame, mapping: dict[str, str], col: str = "Technology"
) -> pd.Series:
    """Remap technology names, cleaning CCS specifics and other inconsistencies."""
    return (
        gem_df[col]
        .fillna("unknown")
        .replace("unknown type", "unknown")
        .apply(lambda x: mapping[x.replace("/CCS", "").strip()])
    )


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


def fuel_col(cell: str, fuel_mapping: dict) -> list[str]:
    """Find and replace fuel names using pattern matching.

    Ambiguous cases should raise errors.
    """
    fuel_patterns = {
        key: (
            re.compile(rf"\b{re.escape(key)}\b", flags=re.IGNORECASE)
            if re.fullmatch(
                r"\w+", key.replace(" ", "")
            )  # only wrap in \b if key is a single word/phrase
            else re.compile(re.escape(key), flags=re.IGNORECASE)
        )
        for key in fuel_mapping
    }

    default_fuel = fuel_mapping["unknown"]
    fuels = []
    if pd.isna(cell):
        fuels.append(default_fuel)
    else:
        for value in cell.split(","):
            if any([i in value for i in ["unknown", "other: other"]]):
                fuels.append(default_fuel)
                continue
            matched_keys = [
                fuel_mapping[key]
                for key, pattern in fuel_patterns.items()
                if pattern.search(value.strip())
            ]
            if len(set(matched_keys)) != 1:
                raise ValueError(
                    f"Ambiguous fuel definition for '{value}': found '{matched_keys}'."
                )
            fuels.append(matched_keys[0])
    return fuels
