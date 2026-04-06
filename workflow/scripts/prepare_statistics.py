"""Aggregated capacity data at a country level."""

import math
import sys
from typing import TYPE_CHECKING, Any

import _plots
import _schemas
import geopandas as gpd
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w", buffering=1)

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
        pd.to_numeric(country_capacity.pop("value"), errors="coerce") * 1000
    )  # EIA data is in GW
    country_capacity["country_id"] = country_a3
    return country_capacity


def prepare(
    input_shapes: str, input_eia_bulk: str, output_total: str, output_categories: str
):
    """Generate a file with annual capacity statistics per country.

    Args:
        input_shapes (str): shapes parquet file.
        input_eia_bulk (str): eia bulk txt database.
        output_total (str): total capacity per country parquet file.
        output_categories (str): per-category capacity parquet file.
    """
    shapes = gpd.read_parquet(input_shapes)
    shapes = _schemas.ShapeSchema.validate(shapes)

    eia_stats = pd.read_json(input_eia_bulk, lines=True)

    results = []
    missing = []
    for country in shapes["country_id"].unique():
        try:
            results.append(_get_country_capacity(eia_stats, country))
        except (ValueError, KeyError, IndexError):
            print(f"Failed to extract statistics for {country}")
            missing.append(country)

    all_statistics = pd.concat(results, ignore_index=True).reset_index(drop=True)
    all_statistics = _schemas.EIASchema.validate(all_statistics)

    total_statistics = all_statistics[all_statistics["category"] == "total"]
    total_statistics = total_statistics.reset_index(drop=True)
    category_statistics = all_statistics[all_statistics["category"] != "total"]
    category_statistics = category_statistics.reset_index(drop=True)

    total_cap_sum = total_statistics["capacity_mw"].sum()
    category_cap_sum = category_statistics["capacity_mw"].sum()
    assert math.isclose(total_cap_sum, category_cap_sum), (
        f"Aggregated capacity checksum failed: {total_cap_sum} vs {category_cap_sum}."
    )

    total_statistics.attrs["missing_countries"] = missing
    category_statistics.attrs["missing_countries"] = missing

    total_statistics.to_parquet(output_total)
    category_statistics.to_parquet(output_categories)


def plot(
    input_total: str,
    input_categories: str,
    output_plot: str,
    figsize: tuple[float, float] = (12, 6),
):
    """Plot the evolution of country capacity over time for every country."""
    df_cats = pd.read_parquet(input_categories)
    df_tot = pd.read_parquet(input_total)[["year", "capacity_mw", "country_id"]]

    missing_countries = df_cats.attrs["missing_countries"]

    countries = set(df_cats["country_id"].unique()) | set(missing_countries)
    n_countries = len(countries)

    fig, axes = plt.subplots(
        nrows=n_countries,
        ncols=1,
        figsize=(figsize[0], figsize[1] * n_countries),
        sharex=False,
        tight_layout=True,
    )
    if n_countries == 1:
        axes = [axes]

    for ax, country in zip(axes, sorted(countries)):
        cats = df_cats[df_cats["country_id"] == country]
        total = df_tot[df_tot["country_id"] == country]

        if any([cats.empty, total.empty]):
            _plots.draw_empty(ax, country)
        else:
            pivot = (
                cats.pivot_table(
                    index="year",
                    columns="category",
                    values="capacity_mw",
                    aggfunc="sum",
                )
                .fillna(0)
                .sort_index()
            )
            bar = pivot.plot(kind="bar", stacked=True, ax=ax, legend=False, zorder=1)

            x_pos = bar.get_xticks()
            total_idx = total.set_index("year").reindex(pivot.index)
            y_tot = total_idx["capacity_mw"].values

            ax.plot(
                x_pos,
                y_tot,
                "x",
                color="black",
                label="total",
                markersize=8,
                linewidth=0,
                zorder=5,
            )

            handles, labels = ax.get_legend_handles_labels()
            ax.legend(
                handles[::-1],
                labels[::-1],
                title="Technology",
                bbox_to_anchor=(1.02, 0.5),
                loc="center left",
                borderaxespad=0,
            )

            ax.xaxis.set_major_locator(MaxNLocator(nbins=10, integer=True))
            ax.tick_params(axis="x", rotation=45)
            ax.set_title(country)
            ax.set_ylabel("Capacity (MW)")
            ax.set_xlabel("Year")

    fig.savefig(output_plot, bbox_inches="tight")


if __name__ == "__main__":
    prepare(
        input_shapes=snakemake.input.shapes,
        input_eia_bulk=snakemake.input.eia_bulk,
        output_total=snakemake.output.total,
        output_categories=snakemake.output.categories,
    )
    plot(
        input_total=snakemake.output.total,
        input_categories=snakemake.output.categories,
        output_plot=snakemake.output.plot,
    )
