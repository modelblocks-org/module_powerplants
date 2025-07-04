"""Imputation of missing values."""

import _schemas
import _utils
import click
import geopandas as gpd
import pandas as pd
import yaml


def impute_start_year(
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


def impute_end_year(
    df: pd.DataFrame, lifetimes: dict[str, int], delay: dict[str, int]
) -> pd.Series:
    """Impute end_year using lifetime.

    Old plants operating beyond lifetime will retired with a given delay.
    """
    CURRENT = _utils.CURRENT_YEAR

    # Impute expected end year if no data is present.
    expected = df["start_year"] + df["technology"].map(lifetimes)
    result = df["end_year"].copy().fillna(expected)

    # Handle cases that need delay
    needs_delay = (result >= CURRENT) & (df["status"] == "operating")
    delayed_end = result + df["technology"].map(delay).astype(int)
    delayed_end = delayed_end.clip(lower=CURRENT + 1)
    result.loc[needs_delay] = delayed_end.loc[needs_delay]

    return result


def adjust_status(row: pd.Series) -> str:
    """Ensure the powerplant status is correct."""
    year = _utils.CURRENT_YEAR
    if year < row["start_year"]:
        status = "planned"
    elif row["end_year"] <= year:
        status = "retired"
    else:
        status = "operating"
    return status


@click.command()
@click.argument("prepared_path", type=str)
@click.argument("shapes_path", type=str)
@click.argument("lifetime_mapping", type=str)
@click.argument("delay_mapping", type=str)
@click.argument("output_path", type=str)
def main(
    prepared_path: str,
    shapes_path: str,
    lifetime_mapping: str,
    delay_mapping: str,
    output_path: str,
):
    """Add automatic and user imputations to fill missing data."""
    prepared = gpd.read_parquet(prepared_path)
    shapes = gpd.read_parquet(shapes_path)
    lifetimes = yaml.safe_load(lifetime_mapping)
    delay = yaml.safe_load(delay_mapping)

    imputed = gpd.sjoin(
        prepared,
        shapes[["country_id", "geometry"]],
        predicate="intersects",
        how="inner",
    )
    imputed = imputed.drop("index_right", axis="columns")
    lifetimes = yaml.safe_load(lifetime_mapping)
    imputed["start_year"] = impute_start_year(imputed, lifetimes)
    imputed["end_year"] = impute_end_year(imputed, lifetimes, delay)
    imputed["status"] = imputed.apply(adjust_status, axis="columns")

    imputed = imputed.dropna(subset=["start_year", "end_year", "status"])
    _schemas.PlantSchema.validate(imputed).to_parquet(output_path)


if __name__ == "__main__":
    main()
