"""Aggregate powerplant capacity into shapes."""

import sys
from typing import TYPE_CHECKING, Any

import _plots
import _schemas
import _utils
import geopandas as gpd
import pandas as pd
from gregor.aggregate import aggregate_raster_to_polygon

if TYPE_CHECKING:
    snakemake: Any


def capacity_solar(
    large_pv_agg_file: str,
    proxy_file: str,
    shapes_file: str,
    technology: str,
) -> gpd.GeoDataFrame:
    """Aggregate rooftop PV using a proxy raster."""
    large_pv = pd.read_parquet(large_pv_agg_file)
    shapes = gpd.read_parquet(shapes_file)
    agg_roof_pv_cap = aggregate_raster_to_polygon(proxy_file, shapes, stats="sum")

    agg_roof_pv_cap["category"] = "solar"
    agg_roof_pv_cap["technology"] = technology
    agg_roof_pv_cap = agg_roof_pv_cap.rename(columns={"sum": "output_capacity_mw"})
    agg_roof_pv_cap = agg_roof_pv_cap.dropna(subset=["output_capacity_mw"])

    # Keep only schema-approved columns
    valid_cols = set(_schemas.AggregatedPlantSchema.to_schema().columns)
    agg_roof_pv_cap = agg_roof_pv_cap[list(valid_cols & set(agg_roof_pv_cap.columns))]

    # Combine and clean the data
    solar_mw = pd.concat([agg_roof_pv_cap, large_pv], ignore_index=True)
    solar_mw = _utils._clean_positive_capacity(solar_mw)
    solar_mw.attrs = large_pv.attrs | agg_roof_pv_cap.attrs
    return _schemas.AggregatedPlantSchema.validate(solar_mw)


def main():
    """Main snakemake process."""
    solar_gdf = capacity_solar(
        large_pv_agg_file=snakemake.input.large_solar,
        proxy_file=snakemake.input.proxy,
        shapes_file=snakemake.input.shapes,
        technology=snakemake.params.technology,
    )
    solar_gdf.to_parquet(snakemake.output.aggregated)
    _plots.plot_capacity_aggregation(
        aggregated_file=snakemake.output.aggregated,
        shapes_file=snakemake.input.shapes,
        output_file=snakemake.output.plot_map,
        category=snakemake.params.category,
    )
    _plots.plot_capacity_adjustment(
        stats_file=snakemake.input.stats,
        unadjusted_file=snakemake.input.large_solar,
        adjusted_file=snakemake.output.aggregated,
        year=snakemake.params.year,
        output_file=snakemake.output.plot_stats,
        is_disagg=False,
    )


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w", buffering=1)
    main()

