"""Aggregated capacity data at a country level."""

import math
import sys
from typing import TYPE_CHECKING, Any

import geopandas as gpd
import numpy as np
import pandas as pd
from workflow.scripts._schemas import EIASchema, ShapeSchema

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")

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
        country_capacity.pop("value") * 1000
    )  # EIA data is in GW
    country_capacity["country_id"] = country_a3
    return country_capacity


def get_eia_capacity_statistics(
    shapes_file: str, eia_bulk_file: str, path_total: str, path_disaggregated: str
):
    """Generate a file with annual capacity statistics per country."""
    shapes = gpd.read_parquet(shapes_file)
    shapes = ShapeSchema.validate(shapes)

    eia_stats = pd.read_json(eia_bulk_file, lines=True)

    results = []
    for country in shapes["country_id"].unique():
        results.append(_get_country_capacity(eia_stats, country))
    all_statistics = pd.concat(results, ignore_index=True).reset_index(drop=True)
    all_statistics = EIASchema.validate(all_statistics)

    total_statistics = all_statistics[all_statistics["category"] == "total"]
    total_statistics = total_statistics.reset_index(drop=True)
    disaggregated_statistics = all_statistics[all_statistics["category"] != "total"]
    disaggregated_statistics = disaggregated_statistics.reset_index(drop=True)

    total_cap_sum = total_statistics["capacity_mw"].sum()
    disaggregated_cap_sum = disaggregated_statistics["capacity_mw"].sum()
    assert math.isclose(total_cap_sum, disaggregated_cap_sum), (
        f"Aggregated capacity checksum failed: {total_cap_sum} vs {disaggregated_cap_sum}."
    )
    total_statistics.to_parquet(path_total)
    disaggregated_statistics.to_parquet(path_disaggregated)


if __name__ == "__main__":
    get_eia_capacity_statistics(
        shapes_file=snakemake.input.shapes,
        eia_bulk_file=snakemake.input.eia_bulk,
        path_total=snakemake.output.total,
        path_disaggregated=snakemake.output.disaggregated,
    )
