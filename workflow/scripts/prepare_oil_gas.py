"""Prepare an oil and gas dataset using our schemas."""

import sys
from typing import TYPE_CHECKING, Any

import _gem as gem
import _schemas
import _utils
import geopandas as gpd
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")


def _retired_year(gem_df: pd.DataFrame):
    """Return retirement year.

    GOGPT has two separate columns for this, requiring special handling.
    """
    return gem_df.apply(
        lambda x: pd.to_numeric(x["retired_year"], errors="coerce")
        if pd.notna(x["retired_year"])
        else pd.to_numeric(x["planned_retire"], errors="coerce"),
        axis="columns",
    )


def main(
    gem_gogpt_path: str,
    technology_mapping: dict[str, str],
    fuel_mapping: dict[str, str],
    output_plants_path: str,
    output_fuels_path: str,
):
    """Obtain oil and gas power plants using GEM-GOGPT data."""
    raw_df = gem.read_gem_dataset(
        gem_gogpt_path, ["Gas & Oil Units", "sub-threshold units"]
    )

    powerplant_id = _utils.get_combined_text_col(
        raw_df, ["gem_location_id", "gem_unit_id"], prefix="GEM_"
    )
    oil_gas_df = gpd.GeoDataFrame(
        {
            "powerplant_id": powerplant_id,
            "name": _utils.get_combined_text_col(raw_df, ["plant_name", "unit_name"]),
            "category": "oil_gas",
            "technology": gem.technology_col(
                raw_df, technology_mapping, col="turbine/engine_technology"
            ),
            "output_capacity_mw": raw_df["capacity_(mw)"],
            "start_year": gem.year_col(raw_df, "start"),
            "end_year": _retired_year(raw_df),
            "status": raw_df["status"],
            "geometry": _utils.get_point_col(raw_df, "longitude", "latitude"),
            "ccs": raw_df["ccs_attachment?"] == "yes",
            "chp": raw_df["chp"] == "yes",
        }
    ).reset_index(drop=True)
    _schemas.CombustionSchema.validate(oil_gas_df).to_parquet(output_plants_path)

    fuels_df = pd.DataFrame(
        {
            "powerplant_id": powerplant_id,
            "fuel": raw_df["fuel"].apply(
                gem.fuel_col,
                fuel_mapping=fuel_mapping,
                default=fuel_mapping["fossil gas: unknown"],
            ),
        }
    )
    fuels_df = fuels_df.explode("fuel").reset_index(drop=True)
    _schemas.FuelSchema.validate(fuels_df).to_parquet(output_fuels_path)


if __name__ == "__main__":
    main(
        gem_gogpt_path=snakemake.input.gem_gogpt,
        technology_mapping=snakemake.params.technology_mapping,
        fuel_mapping=snakemake.params.fuel_mapping,
        output_plants_path=snakemake.output.plants,
        output_fuels_path=snakemake.output.fuels,
    )
