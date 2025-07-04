"""Prepare a clean hydropower dataset that fits our schema."""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import _utils
import geopandas as gpd
import numpy as np
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")


def _technology(gem_df: pd.DataFrame, tech_mapping: dict[str, str]) -> pd.Series:
    """Remap technology names, cleaning CCS specifics and inconsistencies."""
    return gem_df["plant_type"].fillna("unknown").apply(lambda x: tech_mapping[x])


# TODO: add back once a reasonable approach to imputing other head heights is found.
def _head_m(gem_df: pd.DataFrame) -> pd.Series:
    """Imputes the dam height if the head height is not available."""
    return gem_df.apply(
        lambda x: x["dam_height_m"]
        if pd.isna(x["head_m"]) and x["dam_height_m"] >= 0
        else x["head_m"],
        axis="columns",
    )


def _reservoir_km3(gem_df: pd.DataFrame) -> pd.Series:
    """Get reservoir volume if available."""
    return gem_df["res_vol_km3"].where(gem_df["res_vol_km3"] >= 0, np.nan)


def main(input_path: str, output_path: str, technology_mapping: dict[str, str]):
    """Prepare a cleaned hydropower dataset."""
    raw_df = pd.read_csv(input_path)
    raw_df = raw_df.dropna(subset=["capacity_mw", "plant_lon", "plant_lat"])
    hydro_df = gpd.GeoDataFrame(
        {
            "powerplant_id": raw_df["ID"].apply(lambda x: "GloHydroRes_" + x),
            "name": raw_df["name"],
            "category": "hydropower",
            "technology": _technology(raw_df, technology_mapping),
            "output_capacity_mw": raw_df["capacity_mw"],
            "start_year": raw_df["year"],
            "end_year": np.nan,
            "status": "operating",
            "geometry": _utils.get_point_col(raw_df, "plant_lon", "plant_lat"),
            "reservoir_km3": _reservoir_km3(raw_df),
        }
    )
    _schemas.HydroSchema.validate(hydro_df).to_parquet(output_path)


if __name__ == "__main__":
    main(
        input_path=snakemake.input.glohydrores_path,
        output_path=snakemake.output.output_path,
        technology_mapping=snakemake.params.technology_mapping,
    )
