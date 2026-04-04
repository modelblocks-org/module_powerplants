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
    prepared_cat_gdf: gpd.GeoDataFrame,
    countries_gdf: gpd.GeoDataFrame,
    imputation: dict,
    technology_mapping: dict,
    projected_crs: str,
) -> gpd.GeoDataFrame:
    """Add automatic and user imputations to fill missing data.

    Args:
        prepared_cat_gdf (gpd.GeoDataFrame): cleaned category dataset following our schema.
        countries_gdf (str): country-level shapes to use.
        output_path (str): resulting dataset.
        imputation (str): imputation configuration.
        technology_mapping (str): technology mapping configuration.
        projected_crs (str): crs used to calculate centroids.
    """
    _utils.check_single_category(prepared_cat_gdf)

    # Re-map polygons to their centroid to simplify further processing.
    # TODO: consider splitting them between shapes instead?
    polygon_mask = prepared_cat_gdf[
        prepared_cat_gdf.geometry.geom_type != "Point"
    ].index
    if polygon_mask.any():
        if projected_crs:
            prev_crs = prepared_cat_gdf.crs
            prepared_cat_gdf.loc[polygon_mask, "geometry"] = (
                prepared_cat_gdf.loc[polygon_mask, "geometry"]
                .to_crs(projected_crs)
                .centroid.to_crs(prev_crs)
            )
        else:
            raise ValueError(
                "Polygon powerplant geometries detected. Specify a projected CRS."
            )

    lifetimes = imputation["lifetime_years"]
    retirement_delay_years = imputation["retirement_delay_years"]
    scenario = SCENARIO_MAP[imputation["scenario"]]

    # Get facilities within the provided regions and for the given scenario
    imputed = gpd.sjoin(
        prepared_cat_gdf[prepared_cat_gdf["status"].isin(scenario)],
        countries_gdf,
        predicate="intersects",
        how="inner",
    ).drop("index_right", axis="columns")

    if not imputed.empty:
        # Adjust project dates
        imputed["start_year"] = _impute_start_year(imputed, lifetimes)
        imputed["end_year"] = _impute_end_year(
            imputed, lifetimes, retirement_delay_years
        )

        # Drop projects with insufficient date data and then adjust status.
        imputed = imputed.dropna(subset=["start_year", "end_year"])
        imputed["status"] = _impute_status(imputed)

        imputed = handle_polygon_overlaps(
            imputed, imputation["shape_overlap_correction"]
        )

    schema = _schemas.build_schema(technology_mapping, "impute")
    return schema.validate(imputed)


def handle_polygon_overlaps(df: gpd.GeoDataFrame, method: str) -> gpd.GeoDataFrame:
    """Handle duplicate powerplant assignments caused by overlapping shapes."""
    if method not in {"strict", "split_capacity"}:
        raise ValueError(
            f"Unsupported polygon overlap method {method!r}. "
            "Expected 'strict' or 'split_capacity'."
        )

    dup_mask = df["powerplant_id"].duplicated(keep=False)

    if dup_mask.any():
        duplicate_ids = df.loc[dup_mask, "powerplant_id"]

        if method == "strict":
            raise ValueError(
                "Found duplicate IDs, likely due to overlapping polygons: "
                f"{', '.join(map(str, duplicate_ids.unique()))}. "
                "Please adjust your shapes, or enable the 'split_capacity' correction."
            )

        counts = duplicate_ids.map(df["powerplant_id"].value_counts())

        df.loc[dup_mask, "output_capacity_mw"] = (
            df.loc[dup_mask, "output_capacity_mw"] / counts
        )

        suffixes = df.loc[dup_mask].groupby("powerplant_id").cumcount().astype(str)
        df.loc[dup_mask, "powerplant_id"] = duplicate_ids + "_duplicate_" + suffixes

    return df.reset_index(drop=True)


def main() -> None:
    """Main snakemake process."""
    imputed_gdf = impute(
        prepared_cat_gdf=gpd.read_parquet(snakemake.input.prepared),
        countries_gdf=gpd.read_parquet(snakemake.input.dissolved_shapes),
        imputation=snakemake.params.imputation,
        technology_mapping=snakemake.params.tech_map,
        projected_crs=snakemake.params.projected_crs,
    )
    imputed_gdf.to_parquet(snakemake.output.imputed)
    _plots.plot_disaggregated_capacity_buildup(
        imputed_gdf, snakemake.output.plot, "seaborn:tab20"
    )


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
