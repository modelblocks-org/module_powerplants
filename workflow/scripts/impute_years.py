"""Imputation of missing values."""

import sys
from typing import TYPE_CHECKING, Any

import _plots
import _schemas
import _utils
import geopandas as gpd
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any

HISTORICAL = {"operating", "retired"}
SCENARIO_MAP = {
    "historical": HISTORICAL,
    "construction": HISTORICAL | {"construction"},
    "pre_construction": HISTORICAL | {"construction", "pre-construction"},
    "announced": HISTORICAL | {"construction", "pre-construction", "announced"},
}


def _impute_start_year(
    prepared_df: pd.DataFrame, lifetimes: dict[str, int]
) -> pd.Series:
    """Fill start year using reasonable assumptions and user settings."""
    start_year = prepared_df["start_year"].copy()

    # Impute using the lifetime if possible.
    mask_life = start_year.isna() & prepared_df["end_year"].notna()
    start_year[mask_life] = prepared_df.loc[mask_life, "end_year"] - prepared_df.loc[
        mask_life, "technology"
    ].map(lifetimes)
    # Impute using country averages.
    averages = (
        prepared_df.groupby(["country_id", "category", "technology", "status"])[
            "start_year"
        ]
        .transform("mean")
        .round()
    )
    mask_na = start_year.isna()
    start_year.loc[mask_na] = averages[mask_na]

    return start_year


def _impute_end_year(
    df: pd.DataFrame, lifetimes: dict[str, int], delay: dict[str, int]
) -> pd.Series:
    """Impute end_year using lifetime.

    Old plants operating beyond lifetime will retired with a given delay.
    """
    ref_year = _utils.DATASET_YEAR

    # Impute expected end year only if no data is present.
    expected_end = df["start_year"] + df["technology"].map(lifetimes)
    result = df["end_year"].copy().fillna(expected_end)

    # Plants operating beyond expected lifetime will be retired after a delay of >=1 yr
    needs_delay = (result <= ref_year) & (df["status"] == "operating")
    delayed_end = result + df["technology"].map(delay).fillna(0).astype(int)
    delayed_end = delayed_end.clip(lower=ref_year + 1)
    result.loc[needs_delay] = delayed_end.loc[needs_delay]

    return result


def _impute_status(df: pd.DataFrame) -> pd.Series:
    """Impute powerplant status.

    Must be called after start/end years are complete.
    """
    status = df["status"].copy()
    ref_year = _utils.DATASET_YEAR
    status.loc[ref_year < df["start_year"]] = "planned"
    status.loc[(df["start_year"] <= ref_year) & (ref_year < df["end_year"])] = (
        "operating"
    )
    status.loc[df["end_year"] <= ref_year] = "retired"

    if status.isna().any():
        raise ValueError("Entries with ambiguous states were left in the dataframe.")

    return status


def impute(
    relocated_gdf: gpd.GeoDataFrame, imputation: dict, technology_mapping: dict
) -> gpd.GeoDataFrame:
    """Add automatic and user imputations to fill missing data.

    Args:
        relocated_gdf (gpd.GeoDataFrame): relocated powerplants (must have country_id).
        imputation (str): imputation configuration.
        technology_mapping (str): technology mapping configuration.
    """
    _utils.check_single_category(relocated_gdf)

    if (relocated_gdf.geometry.geom_type != "Point").any():
        raise ValueError(
            "Polygon powerplant geometries detected. Only Points are supported."
        )

    lifetimes = imputation["lifetime_years"]
    retirement_delay_years = imputation["retirement_delay_years"]
    scenario = SCENARIO_MAP[imputation["scenario"]]

    # Get facilities within the requested scenario
    imputed = relocated_gdf[relocated_gdf["status"].isin(scenario)].copy()

    if not imputed.empty:
        # Adjust project dates
        imputed["start_year"] = _impute_start_year(imputed, lifetimes)
        imputed["end_year"] = _impute_end_year(
            imputed, lifetimes, retirement_delay_years
        )
        # Drop projects with insufficient date data
        imputed = imputed.dropna(subset=["start_year", "end_year"])
        # Update the powerplant status
        imputed["status"] = _impute_status(imputed)

    schema = _schemas.build_schema(technology_mapping, "impute")
    return schema.validate(imputed)


def explore(imputed: gpd.GeoDataFrame, output_path: str, colormap="tab20"):
    """Create a HTML map for users to explore."""
    if imputed.empty:
        with open(output_path, "w") as f:
            f.write("No data")
    else:
        explorer = imputed.explore(
            column="technology", legend=True, popup=True, cmap=colormap
        )
        explorer.save(output_path)


def main() -> None:
    """Main snakemake process."""
    imputed_gdf = impute(
        relocated_gdf=gpd.read_parquet(snakemake.input.relocated),
        imputation=snakemake.params.imputation,
        technology_mapping=snakemake.params.tech_map,
    )
    imputed_gdf.to_parquet(snakemake.output.imputed)

    _plots.plot_powerplant_capacity_buildup(
        imputed_gdf, snakemake.output.plot, "seaborn:tab20"
    )
    explore(imputed_gdf, snakemake.output.explorer)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
