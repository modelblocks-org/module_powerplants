"""Combine a given number of files belonging to a technology category.

Optionally, remove a given number of powerplant_id.
"""

import _plots
import _schemas
import _utils
import click
import geopandas as gpd
import pandas as pd
import yaml


@click.group()
def cli():
    """Specify sub-command."""
    pass


@cli.command()
@click.argument("input_paths", nargs=-1, type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-o", "--output_path", required=True, type=click.Path(writable=True, dir_okay=False)
)
@click.option("-t", "--tech_mapping", required=True, type=str)
# @click.option("-d", "--drop-id", type=str)
def impute(
    input_paths: list[str],
    output_path: str,
    tech_mapping: str,
    # drop_ids: str | None = None,
):
    """Combine a given number of category files into a final dataset."""
    combined_capacity = pd.concat(
        (gpd.read_parquet(f) for f in input_paths),
        ignore_index=True,
        sort=False,
        axis="index",
    )
    # if drop_ids:
    #     to_drop = yaml.safe_load(drop_ids)
    #     combined_capacity = combined_capacity[
    #         ~combined_capacity["powerplant_id"].isin(to_drop)
    #     ]

    category = _utils.check_single_category(combined_capacity)
    schema = _schemas.build_schema(category, yaml.safe_load(tech_mapping), "impute")
    schema.validate(combined_capacity).to_parquet(output_path)


@cli.command()
@click.argument("imputed_path", type=click.Path(exists=True, dir_okay=False))
@click.argument("output_path", type=click.Path(exists=False, dir_okay=False))
@click.option("--colormap", default="tab20")
def plot(imputed_path: str, output_path: str, colormap):
    """Plot stacked bar charts of active powerplant capacity over time per country."""
    df = pd.read_parquet(imputed_path)
    _plots.plot_disaggregated_capacity_buildup(df, output_path, colormap)


@cli.command()
@click.argument("imputed_path", type=click.Path(exists=True, dir_okay=False))
@click.argument("output_path", type=click.Path(exists=False, dir_okay=False))
@click.option("--colormap", default="tab20")
def explore(imputed_path: str, output_path: str, colormap):
    """Create a HTML map for users to explore."""
    df = gpd.read_parquet(imputed_path)
    explorer = df.explore(column="technology", legend=True, cmap=colormap)
    explorer.save(output_path)


if __name__ == "__main__":
    cli()
