"""Fuel class harmonisation."""

import _schemas
import click
import geopandas as gpd
import pandas as pd


@click.group()
def cli():
    """Fuel preparation CLI."""
    pass


@cli.command()
@click.argument("fuel_class_file", nargs=-1, type=click.Path(dir_okay=False))
@click.option("-o", "--output_file", type=click.Path(dir_okay=False), required=True)
@click.option("-p", "--prefix", type=str, default="fc")
def prepare(fuel_class_file: tuple[str], output_file: str, prefix: str):
    """Prepare harmonised fuel classes across categories.

    Args:
        fuel_class_file (tuple[str]): fuel class datasets.
        output_file (str): harmonised fuel class dataset.
        prefix (str): prefix to append to each class name.
    """
    fuel_df = pd.concat(
        [pd.read_parquet(file) for file in fuel_class_file], axis="index"
    )

    # rebuild sorted tuples of fuels
    grouped = fuel_df.groupby("fuel_class")["fuel"].apply(lambda x: tuple(sorted(x)))
    # remove duplicates and resort
    fuel_combinations = sorted(set(grouped.values))
    fuel_class_df = pd.DataFrame(
        [
            (f"{prefix}{i}", fuel)
            for i, comb in enumerate(fuel_combinations)
            for fuel in comb
        ],
        columns=["fuel_class", "fuel"],
    ).reset_index(drop=True)
    _schemas.FuelSchema.validate(fuel_class_df).to_parquet(output_file)


@cli.command()
@click.argument("plants_file", type=click.Path(dir_okay=False))
@click.argument("old_class_file", type=click.Path(dir_okay=False))
@click.argument("new_class_file", type=click.Path(dir_okay=False))
@click.option("-o", "--output_file", type=str, required=True)
def remap(plants_file: str, old_class_file: str, new_class_file: str, output_file: str):
    """Re-map powerplant fuel classes to a different class dataset."""
    cat_df = gpd.read_parquet(plants_file)
    old_fuel_df = pd.read_parquet(old_class_file)
    new_fuel_df = pd.read_parquet(new_class_file)

    # Get 'flipped' map
    old_grouped = old_fuel_df.groupby("fuel_class")["fuel"].apply(
        lambda x: tuple(sorted(x))
    )
    new_grouped = new_fuel_df.groupby("fuel_class")["fuel"].apply(
        lambda x: tuple(sorted(x))
    )
    to_new = {combo: fuel_class for fuel_class, combo in new_grouped.items()}

    cat_df["fuel_class"] = cat_df["fuel_class"].apply(lambda x: to_new[old_grouped[x]])
    cat_df.to_parquet(output_file)


if __name__ == "__main__":
    cli()
