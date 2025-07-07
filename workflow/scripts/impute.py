"""Imputation of missing values."""

import math

import _schemas
import _utils
import click
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

HISTORICAL = {"operating","retired"}
SCENARIO_MAP = {
    "historical": HISTORICAL,
    "near-future": HISTORICAL | {"construction"},
    "far-future": HISTORICAL | {"construction", "pre-construction"},
    "far-off-future": HISTORICAL | {"construction", "pre-construction", "announced"}
}


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


def impute_status(df: pd.DataFrame) -> pd.Series:
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


@click.group()
def cli():
    """Specify sub-command."""
    pass


@cli.command()
@click.argument("prepared_path", type=str)
@click.argument("shapes_path", type=str)
@click.argument("imputation", type=str)
@click.argument("technology_mapping", type=str)
@click.argument("output_path", type=str)
def main(
    prepared_path: str,
    shapes_path: str,
    imputation: str,
    technology_mapping: str,
    output_path: str,
):
    """Add automatic and user imputations to fill missing data."""
    prepared = gpd.read_parquet(prepared_path)
    shapes = gpd.read_parquet(shapes_path)

    tech_map = yaml.safe_load(technology_mapping)
    imputation_cnf = yaml.safe_load(imputation)
    lifetimes = imputation_cnf["lifetime_yr"]
    retirement_delay_yr = imputation_cnf["retirement_delay_yr"]
    scenario = SCENARIO_MAP[imputation_cnf["scenario"]]

    # Ensure we are working with a valid single-category file.
    categories = prepared["category"].unique()
    if len(categories) != 1:
        raise ValueError(f"Cannot impute multi-category datasets. Found '{categories}'")
    category = categories[0]

    # Get facilities within the provided regions and for the given scenario
    imputed = gpd.sjoin(
        prepared[prepared["status"].isin(scenario)],
        shapes[["country_id", "geometry"]],
        predicate="intersects",
        how="inner",
    )
    imputed = imputed.drop("index_right", axis="columns")

    # Adjust project dates
    imputed["start_year"] = impute_start_year(imputed, lifetimes)
    imputed["end_year"] = impute_end_year(imputed, lifetimes, retirement_delay_yr)

    # Drop projects with insufficient date data and then adjust status.
    imputed = imputed.dropna(subset=["start_year", "end_year"])
    imputed["status"] = impute_status(imputed)

    schema = _schemas.build_schema(category, tech_map, "impute")
    schema.validate(imputed).to_parquet(output_path)


@cli.command()
@click.argument("imputed_path", type=str)
@click.argument("output_path", type=str)
@click.option("--colormap", default="tab20")
def plot(imputed_path: str, output_path: str, colormap):
    """Plot stacked bar charts of active powerplant capacity over time per country."""

    def draw_empty(ax, title, message="No data available"):
        """Helper to render an empty-data placeholder."""
        ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=12, alpha=0.7)
        ax.set_title(title)
        ax.set_axis_off()

    df = pd.read_parquet(imputed_path)
    suptitle = "Active powerplant capacity by technology per country"

    if df.empty:
        fig, ax = plt.subplots()
        draw_empty(ax, "")
        fig.suptitle(suptitle, fontsize=14)
        fig.savefig(output_path)
        return

    # Year range (x-axis)
    start_year = df["start_year"].astype(int).min()
    end_year = df["end_year"].astype(int).max()
    years = list(range(start_year, end_year + 1))

    # Layout (per country in alphabetical order)
    countries = sorted(df["country_id"].unique())
    n_countries = len(countries)
    cols = 2
    rows = math.ceil(n_countries / cols)

    # Tech type color range
    tech_types = sorted(df["technology"].dropna().unique())
    cmap = plt.get_cmap(colormap)
    colors = [cmap(i) for i in np.linspace(0, 1, len(tech_types))]

    # Figure (always 2 columns, flexible rows)
    fig, axes = plt.subplots(
        rows,
        cols,
        figsize=(cols * 5, rows * 4),
        sharex=False,
        sharey=False,
        constrained_layout=True,
    )
    axes_flat = np.array(axes).ravel()

    # Plot per country
    for ax, country in zip(axes_flat, countries):
        country_df = df[df["country_id"] == country]
        if country_df.empty:
            draw_empty(ax, country, f"No data for {country}")
            continue

        cap_mw = pd.DataFrame(0.0, index=years, columns=tech_types)
        for year in years:
            active = country_df[
                (country_df["start_year"] <= year) & (year < country_df["end_year"])
            ]
            cap_mw.loc[year] = (
                active.groupby("technology")["output_capacity_mw"]
                .sum()
                .reindex(tech_types, fill_value=0)
            )

        cap_mw.plot(kind="bar", stacked=True, ax=ax, color=colors, legend=False, rot=45)
        ax.set_title(country)
        ax.set_ylabel("Capacity (MW)")
        ax.locator_params(axis="x", nbins=10)
        ax.minorticks_off()

    # Hide extra axes
    for ax in axes_flat[n_countries:]:
        ax.set_visible(False)

    # Add details
    handles, labels = axes_flat[0].get_legend_handles_labels()
    fig.legend(
        handles[::-1],
        labels[::-1],
        loc="center left",
        bbox_to_anchor=(1.0, 0.5),
        title="Technology",
        frameon=False,
    )
    fig.suptitle(suptitle, fontsize=14)

    fig.savefig(output_path, bbox_inches="tight")


if __name__ == "__main__":
    cli()
