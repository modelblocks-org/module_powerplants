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
sys.stderr = open(snakemake.log[0], "w")

HISTORICAL = {"operating", "retired"}
SCENARIO_MAP = {
    "historical": HISTORICAL,
    "near-future": HISTORICAL | {"construction"},
    "far-future": HISTORICAL | {"construction", "pre-construction"},
    "far-off-future": HISTORICAL | {"construction", "pre-construction", "announced"},
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
    ref_year = _utils.REFERENCE_YR

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
    ref_year = _utils.REFERENCE_YR
    status.loc[ref_year < df["start_year"]] = "planned"
    status.loc[(df["start_year"] <= ref_year) & (ref_year < df["end_year"])] = (
        "operating"
    )
    status.loc[df["end_year"] <= ref_year] = "retired"

    if status.isna().any():
        raise ValueError("Entries with ambiguous states where left in the dataframe.")

    return status


def impute(
    prepared_path: str,
    borders_path: str,
    output_path: str,
    imputation: str,
    technology_mapping: str,
    projected_crs: str,
):
    """Add automatic and user imputations to fill missing data.

    Args:
        prepared_path (str): cleaned dataset following our schema.
        borders_path (str): country-level shapes to use.
        output_path (str): resulting dataset.
        imputation (str): imputation configuration.
        technology_mapping (str): technology mapping configuration.
        projected_crs (str): crs used to calculate centroids.
    """
    prepared = gpd.read_parquet(prepared_path)
    _utils.check_single_category(prepared)

    # Re-map polygons to their centroid to simplify further processing.
    # TODO: consider splitting them between shapes instead?
    polygon_mask = prepared[prepared.geometry.geom_type != "Point"].index
    if polygon_mask.any():
        if projected_crs:
            prev_crs = prepared.crs
            prepared.loc[polygon_mask, "geometry"] = (
                prepared.loc[polygon_mask, "geometry"]
                .to_crs(projected_crs)
                .centroid.to_crs(prev_crs)
            )
        else:
            raise ValueError(
                "Polygon powerplant geometries detected. Specify a projected CRS."
            )

    borders = _schemas.ShapeSchema.validate(gpd.read_parquet(borders_path))


    lifetimes = imputation["lifetime_yr"]
    retirement_delay_yr = imputation["retirement_delay_yr"]
    scenario = SCENARIO_MAP[imputation["scenario"]]

    # Get facilities within the provided regions and for the given scenario
    imputed = gpd.sjoin(
        prepared[prepared["status"].isin(scenario)],
        borders[["country_id", "geometry"]].dissolve("country_id").reset_index(),
        predicate="intersects",
        how="inner",
    ).drop("index_right", axis="columns")

    if not imputed.empty:
        # Adjust project dates
        imputed["start_year"] = _impute_start_year(imputed, lifetimes)
        imputed["end_year"] = _impute_end_year(imputed, lifetimes, retirement_delay_yr)

        # Drop projects with insufficient date data and then adjust status.
        imputed = imputed.dropna(subset=["start_year", "end_year"])
        imputed["status"] = _impute_status(imputed)

    schema = _schemas.build_schema(technology_mapping, "impute")
    schema.validate(imputed).to_parquet(output_path)


def plot(imputed_path: str, output_path: str, colormap: str = "tab20"):
    """Plot stacked bar charts of active powerplant capacity over time per country."""
    df = pd.read_parquet(imputed_path)
    _plots.plot_disaggregated_capacity_buildup(df, output_path, colormap)


if __name__ == "__main__":
    impute(
        prepared_path=snakemake.input.prepared,
        borders_path=snakemake.input.borders,
        output_path=snakemake.output.imputed,
        imputation=snakemake.params.imputation,
        technology_mapping=snakemake.params.tech_map,
        projected_crs=snakemake.params.projected_crs,
    )
    plot(imputed_path=snakemake.output.imputed, output_path=snakemake.output.plot)
