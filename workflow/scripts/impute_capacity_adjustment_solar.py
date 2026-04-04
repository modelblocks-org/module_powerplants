"""Adjustment of powerplant capacity for cases that used proxies."""

import sys
from typing import TYPE_CHECKING, Any

import _plots
import _schemas
import _utils
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")


def adjust_aggregated(
    stats_file: str, unadjusted_file: str, year: int, output_file: str
):
    """Adjust aggregated powerplant capacity in the given year.

    Only provides the requested reference year.
    """
    stats = pd.read_parquet(stats_file)
    plants = pd.read_parquet(unadjusted_file)

    if plants.empty:
        adjusted_plants = plants
    else:
        adjusted_plants = _utils.adjust_aggregated_capacity(plants, stats, year)

    _schemas.AggregatedPlantSchema.validate(adjusted_plants).to_parquet(output_file)


if __name__ == "__main__":
    adjust_aggregated(
        stats_file=snakemake.input.stats,
        unadjusted_file=snakemake.input.unadjusted,
        year=snakemake.params.year,
        output_file=snakemake.output.adjusted,
    )

    _plots.plot_capacity_adjustment(
        stats_file=snakemake.input.stats,
        unadjusted_file=snakemake.input.unadjusted,
        adjusted_file=snakemake.output.adjusted,
        year=snakemake.params.year,
        output_file=snakemake.output.adj_plot,
        is_disagg=False,
    )

    _plots.plot_capacity_aggregation(
        aggregated_file=snakemake.output.adjusted,
        shapes_file=snakemake.input.shapes,
        output_file=snakemake.output.map_plot,
        category="solar",
    )
