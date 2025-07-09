"""Combine a given number of files belonging to a technology category.

Optionally, remove a given number of powerplant_id.
"""

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


def impute(
    input_paths: list[str],
    output_path: str,
    tech_mapping: dict[str, str],
    excluded: list[str] | None = None,
):
    """Combine a given number of category files into a final dataset."""
    combined_capacity = pd.concat(
        (gpd.read_parquet(f) for f in input_paths),
        ignore_index=True,
        sort=False,
        axis="index",
    )
    if excluded:
        to_drop = _utils.listify(excluded)
        combined_capacity = combined_capacity[
            ~combined_capacity["powerplant_id"].isin(to_drop)
        ]

    category = _utils.check_single_category(combined_capacity)
    schema = _schemas.build_schema(category, tech_mapping, "impute")
    schema.validate(combined_capacity).to_parquet(output_path)


def plot(imputed_path: str, output_path: str, colormap="tab20"):
    """Plot stacked bar charts of active powerplant capacity over time per country."""
    df = pd.read_parquet(imputed_path)
    _plots.plot_disaggregated_capacity_buildup(df, output_path, colormap)


def explore(imputed_path: str, output_path: str, colormap="tab20"):
    """Create a HTML map for users to explore."""
    df = gpd.read_parquet(imputed_path)
    explorer = df.explore(column="technology", legend=True, popup=True, cmap=colormap)
    explorer.save(output_path)


if __name__ == "__main__":
    impute(
        input_paths=snakemake.input.to_combine,
        output_path=snakemake.output.combined,
        tech_mapping=snakemake.params.tech_map,
        excluded=snakemake.params.excluded,
    )
    plot(imputed_path=snakemake.output.combined, output_path=snakemake.output.plot)
    explore(
        imputed_path=snakemake.output.combined, output_path=snakemake.output.explore
    )
