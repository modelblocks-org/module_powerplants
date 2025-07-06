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

    # Handle cases that need delay (default: retired in 1 year)
    needs_delay = (result >= CURRENT) & (df["status"] == "operating")
    delayed_end = result + df["technology"].map(delay).fillna(0).astype(int)
    delayed_end = delayed_end.clip(lower=CURRENT + 1)
    result.loc[needs_delay] = delayed_end.loc[needs_delay]

    return result


def impute_status(df: pd.DataFrame) -> pd.Series:
    """Impute powerplant status.

    Must be called after start/end years are complete.
    """
    status = pd.Series("operating", index=df.index)
    status.loc[df["start_year"] > _utils.CURRENT_YEAR] = "planned"
    status.loc[df["end_year"] <= _utils.CURRENT_YEAR] = "retired"

    return status

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


@click.group()
def cli():
    """Specify sub-command."""
    pass

@cli.command()
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
    imputed["status"] = impute_status(imputed)

    # Drop projects with insufficient data
    imputed = imputed.dropna(subset=["start_year", "end_year", "status"])
    _schemas.PlantSchema.validate(imputed).to_parquet(output_path)


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
        plt.show()
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
        sharex=True,
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
