"""Aggregated capacity data at a country level."""

import sys
from typing import TYPE_CHECKING, Any

import geopandas as gpd
import numpy as np
import pandas as pd
from _schema import eia_capacity_schema, shape_schema

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")

CAT_ID = {
    "total": 2,
    "nuclear": 27,
    "fossil_fuels": 28,
    "hydroelectricity": 33,
    "geothermal": 35,
    "other": 117,
    "solar": 116,
    "wind": 37,
    "biomass and waste": 38,
}


def _get_id_data(eia_df: pd.DataFrame, code: str) -> pd.DataFrame:
    idx = eia_df[eia_df["series_id"] == code].index[0]
    df = pd.DataFrame(eia_df.loc[idx, "data"], columns=["year", "value"])
    df = df.replace("NA", np.nan)
    return df


def _get_capacity_id_data(eia_df: pd.DataFrame, country_a3: str, category_id: int) -> pd.DataFrame:
    """Return annual capacity in GW."""
    code = f"INTL.{category_id}-7-{country_a3}-MK.A"
    return _get_id_data(eia_df, code)


def _get_country_capacity(
    eia_df: pd.DataFrame, country_a3: str, disaggregate: bool = False
):
    """Parse country capacity from the EIA dataset."""
    if disaggregate:
        categories = [
            "nuclear",
            "fossil_fuels",
            "hydroelectricity",
            "geothermal",
            "other",
            "solar",
            "wind",
            "biomass and waste",
        ]
    else:
        categories = ["total"]

    results = []
    for category in categories:
        data = _get_capacity_id_data(eia_df, country_a3, CAT_ID[category])
        data["category"] = category
        results.append(data)

    country_capacity = pd.concat(results, ignore_index=True)
    country_capacity.reset_index(drop=True)
    country_capacity["capacity_mw"] = country_capacity.pop("value") * 1000  # EIA data is in GW
    country_capacity["country_id"] = country_a3
    return country_capacity


def get_eia_capacity_statistics(shapes_file: str, eia_bulk_file: str, output_file: str, disaggregate: bool):
    """Generate a file with annual capacity statistics per country."""
    shapes = gpd.read_parquet(shapes_file)
    shapes = shape_schema.validate(shapes)

    eia_stats = pd.read_json(eia_bulk_file, lines=True)

    results = []
    for country in shapes["country_id"].unique():
        results.append(_get_country_capacity(eia_stats, country, disaggregate=disaggregate))
    annual_statistics = pd.concat(results, ignore_index=True).reset_index(drop=True)
    annual_statistics = eia_capacity_schema.validate(annual_statistics)
    annual_statistics.to_parquet(output_file)


if __name__ == "__main__":
    get_eia_capacity_statistics(
        shapes_file=snakemake.input.shapes,
        eia_bulk_file=snakemake.input.eia_bulk,
        output_file=snakemake.output.annual_stats,
        disaggregate=snakemake.params.disaggregate
    )

