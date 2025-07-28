"""Adjustment of powerplant capacity for generic cases."""

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


def adjust_disaggregated(
    stats_file: str, unadjusted_file: str, year: int, output_file: str
):
    """Adjust disaggregated powerplant capacity in the given year.

    Also appends future projects without altering their values.
    """
    stats = pd.read_parquet(stats_file)
    plants = gpd.read_parquet(unadjusted_file)

    # Filter only relevant countries
    plants = plants[plants["country_id"].isin(stats["country_id"].unique())]
    if plants.empty:
        adjusted_plants = plants
    else:
        adjusted_plants = _utils.adjust_capacity(plants, stats, year, is_disagg=True)

    _schemas.PlantSchema.validate(adjusted_plants).to_parquet(output_file)


if __name__ == "__main__":
    adjust_disaggregated(
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
        output_file=snakemake.output.plot,
        is_disagg=True,
    )
