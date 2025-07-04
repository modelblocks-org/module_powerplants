"""Aggregated capacity data at a country level."""

import math
import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")

CAT_ID = {
    "total": 2,
    "nuclear": 27,
    "fossil fuels": 28,
    "hydropower": 33,
    "geothermal": 35,
    "tide and wave": 117,
    "solar": 116,
    "wind": 37,
    "biomass and waste": 38,
    "pumped storage": 82,
}


def _get_id_data(eia_df: pd.DataFrame, code: str) -> pd.DataFrame:
    idx = eia_df[eia_df["series_id"] == code].index[0]
    df = pd.DataFrame(eia_df.loc[idx, "data"], columns=["year", "value"])
    df = df.replace("NA", np.nan)
    return df


def _get_capacity_id_data(
    eia_df: pd.DataFrame, country_a3: str, category_id: int
) -> pd.DataFrame:
    """Return annual capacity in GW."""
    code = f"INTL.{category_id}-7-{country_a3}-MK.A"
    return _get_id_data(eia_df, code)


def _get_country_capacity(eia_df: pd.DataFrame, country_a3: str):
    """Parse country capacity from the EIA dataset."""
    results = []
    for category, identifier in CAT_ID.items():
        data = _get_capacity_id_data(eia_df, country_a3, identifier)
        data["category"] = category
        results.append(data)

    country_capacity = pd.concat(results, ignore_index=True)
    country_capacity.reset_index(drop=True)
    country_capacity["capacity_mw"] = (
        country_capacity.pop("value") * 1000
    )  # EIA data is in GW
    country_capacity["country_id"] = country_a3
    return country_capacity


def get_eia_capacity_statistics(
    shapes_file: str, eia_bulk_file: str, path_total: str, path_categories: str
):
    """Generate a file with annual capacity statistics per country."""
    shapes = gpd.read_parquet(shapes_file)
    shapes = _schemas.ShapeSchema.validate(shapes)

    eia_stats = pd.read_json(eia_bulk_file, lines=True)

    results = []
    for country in shapes["country_id"].unique():
        results.append(_get_country_capacity(eia_stats, country))
    all_statistics = pd.concat(results, ignore_index=True).reset_index(drop=True)
    all_statistics = _schemas.EIASchema.validate(all_statistics)

    total_statistics = all_statistics[all_statistics["category"] == "total"]
    total_statistics = total_statistics.reset_index(drop=True)
    category_statistics = all_statistics[all_statistics["category"] != "total"]
    category_statistics = category_statistics.reset_index(drop=True)

    total_cap_sum = total_statistics["capacity_mw"].sum()
    disaggregated_cap_sum = category_statistics["capacity_mw"].sum()
    assert math.isclose(total_cap_sum, disaggregated_cap_sum), (
        f"Aggregated capacity checksum failed: {total_cap_sum} vs {disaggregated_cap_sum}."
    )
    total_statistics.to_parquet(path_total)
    category_statistics.to_parquet(path_categories)


def plot_category_statistics(category_file: str, path_plot: str, figsize=(12, 4)):
    """Plot the evolution of country capacity over time for every country in df."""
    df = pd.read_parquet(category_file)
    countries = df["country_id"].unique()
    n = len(countries)

    fig, axes = plt.subplots(
        nrows=n,
        ncols=1,
        figsize=(figsize[0], figsize[1] * n),
        sharex=False,
        tight_layout=True,
    )
    if n == 1:
        axes = [axes]

    for ax, country in zip(axes, countries):
        sub = df[df["country_id"] == country]

        pivot = (
            sub.pivot_table(
                index="year", columns="category", values="capacity_mw", aggfunc="sum"
            )
            .fillna(0)
            .sort_index()
        )

        pivot.plot(kind="bar", stacked=True, ax=ax, legend=False)
        ax.set_title(f"{country}")
        ax.set_ylabel("Capacity (MW)")
        ax.set_xlabel("Year")

    # single legend on the right
    handles, labels = pivot.plot(kind="bar", stacked=True).get_legend_handles_labels()
    fig.legend(
        handles[::-1],
        labels[::-1],
        title="Technology",
        bbox_to_anchor=(1.02, 0.5),
        loc="center left",
    )
    fig.savefig(path_plot, bbox_inches="tight")


if __name__ == "__main__":
    get_eia_capacity_statistics(
        shapes_file=snakemake.input.shapes,
        eia_bulk_file=snakemake.input.eia_bulk,
        path_total=snakemake.output.total,
        path_categories=snakemake.output.categories,
    )
    plot_category_statistics(
        category_file=snakemake.output.categories,
        path_plot=snakemake.output.plot
    )
