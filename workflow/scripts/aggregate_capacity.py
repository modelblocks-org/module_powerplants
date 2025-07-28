"""Aggregate powerplant capacity into shapes."""

import sys
from typing import TYPE_CHECKING, Any

import _plots
import _schemas
import _utils
import geopandas as gpd
import pandas as pd
from gregor.aggregate import aggregate_point_to_polygon, aggregate_raster_to_polygon

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w", buffering=1)


CAPACITY_COLUMNS = {"category", "technology", "chp", "ccs", "fuel_class"}


def capacity(powerplant_file: str, shapes_file: str, year: int, output_file: str):
    """Aggregate operating capacity for the given year.

    Args:
        powerplant_file (str): powerplant data file.
        shapes_file (str): shapes file.
        year (float): reference year of adjustment.
        output_file (str): aggregated data file.
    """
    shapes_df = gpd.read_parquet(shapes_file)
    shapes_df = _schemas.ShapeSchema.validate(shapes_df)
    plants_df = gpd.read_parquet(powerplant_file)
    plants_df = _utils.filter_years(plants_df, year, "operating")

    if plants_df.empty:
        cols = {"shape_id"} | (
            set(plants_df.columns)
            & set(_schemas.AggregatedPlantSchema.to_schema().columns)
        )
        agg_plants_df = pd.DataFrame(columns=sorted(cols))
    else:
        plants_df = plants_df.to_crs(shapes_df.geometry.crs)
        shapes_df = shapes_df.set_index("shape_id")

        # set of technology-relevant columns
        group_cols = list(set(plants_df.columns) & CAPACITY_COLUMNS)
        agg_plants_arr = []
        for key_vals, group in plants_df.groupby(list(group_cols)):
            agg_df = aggregate_point_to_polygon(
                group[["output_capacity_mw", "geometry"]], shapes_df["geometry"]
            )
            agg_df = agg_df.drop("geometry", axis="columns")
            agg_df["output_capacity_mw"] = agg_df["output_capacity_mw"].fillna(0)
            agg_df["country_id"] = shapes_df["country_id"]
            for i, name in enumerate(group_cols):
                agg_df[name] = key_vals[i]
            agg_plants_arr.append(agg_df.reset_index())

        agg_plants_df = pd.concat(agg_plants_arr, axis="index", ignore_index=True)

    agg_plants_df.attrs["year"] = year
    _schemas.AggregatedPlantSchema.validate(agg_plants_df).to_parquet(output_file)


def capacity_solar(
    large_pv_agg_file: str,
    proxy_file: str,
    shapes_file: str,
    output_file: str,
    technology: str,
):
    """Aggregate rooftop PV using a proxy raster."""
    large_pv = pd.read_parquet(large_pv_agg_file)
    shapes = gpd.read_parquet(shapes_file)
    agg_roof_pv_cap = aggregate_raster_to_polygon(proxy_file, shapes, stats="sum")

    agg_roof_pv_cap["category"] = "solar"
    agg_roof_pv_cap["technology"] = technology
    agg_roof_pv_cap = agg_roof_pv_cap.rename(columns={"sum": "output_capacity_mw"})
    agg_roof_pv_cap = agg_roof_pv_cap.dropna(subset=["output_capacity_mw"])

    valid_cols = set(_schemas.AggregatedPlantSchema.to_schema().columns)
    agg_roof_pv_cap = agg_roof_pv_cap[list(valid_cols & set(agg_roof_pv_cap.columns))]

    solar_mw = pd.concat([agg_roof_pv_cap, large_pv], ignore_index=True)
    solar_mw.attrs = large_pv.attrs | agg_roof_pv_cap.attrs
    _schemas.AggregatedPlantSchema.validate(solar_mw).to_parquet(output_file)


if __name__ == "__main__":
    if snakemake.wildcards.category == "solar":
        capacity_solar(
            large_pv_agg_file=snakemake.input.large_solar,
            proxy_file=snakemake.input.proxy,
            shapes_file=snakemake.input.shapes,
            output_file=snakemake.output.aggregated,
            technology=snakemake.params.technology,
        )
    else:
        capacity(
            powerplant_file=snakemake.input.powerplants,
            shapes_file=snakemake.input.shapes,
            year=snakemake.params.year,
            output_file=snakemake.output.aggregated,
        )
    _plots.plot_capacity_aggregation(
        aggregated_file=snakemake.output.aggregated,
        shapes_file=snakemake.input.shapes,
        output_file=snakemake.output.plot,
        category=snakemake.wildcards.category,
    )
