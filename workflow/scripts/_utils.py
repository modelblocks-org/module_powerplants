"""General utilities shared across rules."""

from typing import Literal

import geopandas as gpd
import pandas as pd
from pandas.api.types import is_list_like

# Average year where disaggregated datasets were last updated.
# MUST BE ADJUSTED WHENEVER DATASOURCES ARE UPDATED!
DATASET_YEAR = 2024


def listify(item) -> list:
    """Avoids ambiguity in YAML list parameters."""
    return item if is_list_like(item) else [item]


EIA_CAT_MAPPING = {
    "bioenergy": "biomass and waste",
    "fossil": "fossil fuels",
    "geothermal": "geothermal",
    "hydropower": ["hydropower", "pumped storage"],
    "nuclear": "nuclear",
    "solar": "solar",
    "wind": "wind",
}
EIA_CAT_MAPPING = {k: listify(v) for k, v in EIA_CAT_MAPPING.items()}


def get_eia_stats_in_cat_yr(
    stats: pd.DataFrame, year: int, category: str
) -> pd.DataFrame:
    """Get EIA statistics for a given year and category."""
    stats = stats[stats["year"] == year]
    stats = stats[stats["category"].isin(EIA_CAT_MAPPING[category])]
    return stats


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
        raise ValueError(
            f"Cannot compute dataset with ambiguous category. Found '{categories}'"
        )
    return categories[0]


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
        - operating: only active powerplants in the given year.
        - future: active and planned powerplant projects in the given year.

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


def get_adjusted_capacity(
    operating_plants: pd.DataFrame, expected_capacity: pd.Series
) -> pd.Series:
    """Adjust powerplant capacity the total expected capacity per country.

    Args:
        operating_plants (pd.DataFrame): dataframe with all operating plants to adjust.
        expected_capacity (pd.Series): expected category capacity per country.

    Returns:
        pd.Series: adjusted powerplant capacity.
    """
    adjusted_cap_mw = (
        operating_plants["output_capacity_mw"]
        / operating_plants.groupby("country_id")["output_capacity_mw"].transform("sum")
    ) * operating_plants["country_id"].map(expected_capacity)
    return adjusted_cap_mw


def adjust_capacity(plants, stats, year, is_disagg):
    """Adjust capacity to national statistics in the given year.

    Will keep future projects in the disaggregated case.
    """
    category = check_single_category(plants)
    stats = get_eia_stats_in_cat_yr(stats, year, category)

    expected_capacity = stats.groupby("country_id")["capacity_mw"].sum()

    if is_disagg:
        operating = filter_years(plants, year, how="operating").copy()
    else:
        operating = plants.copy()

    adjusted_cap = get_adjusted_capacity(operating, expected_capacity)

    if is_disagg:
        adjusted = filter_years(plants, year, how="future").copy()
    else:
        adjusted = operating.copy()

    # --- identify rows with missing stats ---
    mask_nan = adjusted_cap.isna()

    if mask_nan.any():
        kept_unadjusted = (
            operating.loc[mask_nan, ["category", "country_id"]]
            .drop_duplicates()
            .sort_values(["category", "country_id"])
        )

        print(
            "[adjust_capacity] Kept unadjusted capacity for the following "
            "category-country pairs:"
        )
        for _, row in kept_unadjusted.iterrows():
            print(f"  - {row['category']} / {row['country_id']}")

    # --- only overwrite where adjustment is valid ---
    adjusted.loc[adjusted_cap.index[~mask_nan], "output_capacity_mw"] = adjusted_cap[
        ~mask_nan
    ]

    return adjusted.reset_index(drop=True)
