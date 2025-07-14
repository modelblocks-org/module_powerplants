"""Aggregate powerplant capacity into shapes."""

import _plots
import _schemas
import _utils
import click
import geopandas as gpd
import numpy as np
import pandas as pd
from gregor.aggregate import aggregate_point_to_polygon
from matplotlib import pyplot as plt

CAPACITY_COLUMNS = {"category", "technology", "chp", "ccs", "fuel_class"}


@click.group()
def cli():
    """CLI for powerplant aggregation to shapes."""
    pass


@cli.command()
@click.argument("powerplant_file", type=click.Path(dir_okay=False))
@click.argument("shapes_file", type=click.Path(dir_okay=False))
@click.option("-y", "--year", type=int, required=True)
@click.option("-o", "--output_file", type=click.Path(dir_okay=False), required=True)
@click.option("-c", "--projected_crs", type=str)
def capacity(
    powerplant_file: str,
    shapes_file: str,
    year: int,
    output_file: str,
    projected_crs: str,
):
    """Aggregate operating capacity for the given year.

    Args:
        powerplant_file (str): powerplant data file.
        shapes_file (str): shapes file.
        year (float): reference year of adjustment.
        projected_crs (str): CRS to use when calculating centroids.
        output_file (str): aggregated data file.
    """
    shapes_df = gpd.read_parquet(shapes_file)
    shapes_df = _schemas.ShapeSchema.validate(shapes_df)
    plants_df = gpd.read_parquet(powerplant_file)
    plants_df = _utils.filter_years(plants_df, year, "operating")

    # Keep technology-relevant columns, if present.
    group_cols = list(set(plants_df.columns) & CAPACITY_COLUMNS)

    if plants_df.empty:
        agg_plants_df = pd.DataFrame(columns=group_cols + ["shape_id"])
    else:
        non_point_mask = plants_df[plants_df.geometry.geom_type != "Point"].index
        if non_point_mask.any():
            if not projected_crs:
                raise ValueError(
                    "Polygon powerplant geometries detected. Specify a projected CRS."
                )
            prev_crs = plants_df.crs
            plants_df.loc[non_point_mask, "geometry"] = (
                plants_df.loc[non_point_mask, "geometry"]
                .to_crs(projected_crs)
                .centroid.to_crs(prev_crs)
            )
        plants_df = plants_df.to_crs(shapes_df.geometry.crs)
        shapes_df = shapes_df.set_index("shape_id")
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


@cli.command()
@click.argument("aggregated_file", type=click.Path(dir_okay=False))
@click.argument("shapes_file", type=click.Path(dir_okay=False))
@click.option("-o", "--output_file", type=click.Path(dir_okay=False), required=True)
@click.option("-c", "--category", type=str, default="")
def plot(aggregated_file: str, shapes_file: str, output_file: str, category: str):
    """Plot aggregated capacity per region."""
    shapes = _schemas.ShapeSchema.validate(gpd.read_parquet(shapes_file))
    agg = _schemas.AggregatedPlantSchema.validate(pd.read_parquet(aggregated_file))

    title = f"Aggregated {category} capacity"

    if agg.empty:
        _plots.plot_empty(title, output_file)
    else:
        cap_by_shape = agg.groupby("shape_id")["output_capacity_mw"].sum()

        shapes = shapes.set_index("shape_id")
        shapes["output_capacity_mw"] = cap_by_shape.replace(0, np.nan)

        fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

        ax = shapes.plot(
            ax=ax,
            column="output_capacity_mw",
            cmap="magma",
            edgecolor="black",
            linewidth=0.5,
            legend=True,
            legend_kwds={"label": "Capacity ($MW$)"},
            missing_kwds={"color": "lightgrey", "alpha": 0.2},
        )
        ax.set_title(title + f" in year {agg.attrs['year']}")
        ax.set_xlabel("Longitude ($deg$)")
        ax.set_ylabel("Latitude ($deg$)")
        fig.savefig(output_file, bbox_inches="tight")


if __name__ == "__main__":
    cli()
