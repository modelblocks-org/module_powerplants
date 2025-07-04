"""General utilities for procesing GEM datasets."""

import re
from typing import Literal

import pandas as pd

GEM_GWPT_SHEETS = ["Data", "Below Threshold"]

GEM_GSPT_SHEETS = ["20 MW+", "1-20 MW"]
GSPT_CAPACITY_RATING_MAPPING = {"MWac": "AC", "MWp/dc": "DC"}

_INVALID_STATUS_VALUES = ["cancelled", "shelved"]
_DROPPED_NA_COLUMNS = ["capacity_(mw)", "latitude", "longitude"]


_STATUS_MAPPING = {
    "announced": "planned",
    "pre-construction": "planned",
    "construction": "planned",
    "operating": "operating",
    "mothballed": "retired",
    "retired": "retired",
}


def read_gem_dataset(path: str, sheets: list[str], dropped_na_cols: list[str]| None=None) -> pd.DataFrame:
    """Get a GEM dataset for a type/category of powerplant."""
    if dropped_na_cols is None:
        dropped_na_cols = _DROPPED_NA_COLUMNS

    gem_df = pd.concat([pd.read_excel(path, sheet) for sheet in sheets], axis="index")
    # Harmonise column names
    gem_df.columns = gem_df.columns.str.strip().str.lower().str.replace(" ", "_")
    # Get raw dataset without cancelled projects
    pattern = "|".join(map(re.escape, _INVALID_STATUS_VALUES))
    mask = gem_df["status"].str.contains(pattern, na=False)
    gem_df = gem_df[~mask]
    # Remove rows with problematic empty values
    gem_df = gem_df.dropna(subset=dropped_na_cols)
    gem_df = gem_df.reset_index(drop=True)

    return gem_df


def year_col(gem_df: pd.DataFrame, option: Literal["start", "end"]):
    """Get start/end year, ensuring typing is respected."""
    mapping = {"start": "start_year", "end": "retired_year"}
    return gem_df[mapping[option]].apply(lambda x: pd.to_numeric(x, errors="coerce"))


def status_col(gem_df: pd.DataFrame, mapping: dict | None = None):
    """Get standardised plant status."""
    if mapping is None:
        mapping = _STATUS_MAPPING
    return gem_df["status"].map(mapping)


def technology_col(
    gem_df: pd.DataFrame, mapping: dict[str, str], col: str = "technology"
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
        gem_df["capacity_rating"]
        .fillna("unknown")
        .replace("unknown", default_rating)
        .replace(GSPT_CAPACITY_RATING_MAPPING)
    )
    invalid_ratings = set(rating.unique()) - set(GSPT_CAPACITY_RATING_MAPPING.values())
    assert not invalid_ratings, f"GEM GSPT: invalid capacity ratings {invalid_ratings}."

    cap_df = pd.concat([gem_df["capacity_(mw)"], rating], axis="columns")
    return cap_df.apply(
        lambda x: x["capacity_(mw)"]
        if x["capacity_rating"] == "AC"
        else x["capacity_(mw)"] / dc_ac_ratio,
        axis="columns",
    )


def fuel_col(cell: str, fuel_mapping: dict, default: str) -> list[str]:
    """Find and replace fuel names using pattern matching.

    Ambiguous cases should raise errors.
    """
    fuels = []
    if pd.isna(cell):
        fuels.append(default)
    else:
        for value in cell.split(","):
            # removes shares within brackets (e.g., fossil gas: LNG [50%])
            value = re.sub(r"\s*\[.*?\]", "", value).strip()
            if ":" not in value:
                # removes ambiguous cases (e.g., fossil gas -> fossil gas: unknown)
                value = value + ": unknown"
            if value in ["other: other", "other: unknown"]:
                # Too ambiguous to map usefully
                continue
            try:
                fuels.append(fuel_mapping[value])
            except KeyError:
                raise KeyError(
                    f"No mapped fuel for '{value}'."
                )
    if len(fuels) == 0:
        # Handle edge cases where only ambiguous fuels are given.
        fuels.append(default)
    return fuels
