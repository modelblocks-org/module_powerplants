"""Prepare a bioenergy dataset using our schemas."""

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


def _start_year(gem_df: pd.DataFrame):
    """Return retirement year.

    GBPT has a separate column for fossil powerplants that were converted to bioenergy.
    """
    return gem_df.apply(
        lambda x: pd.to_numeric(x["unit_conversion_year"], errors="coerce")
        if pd.notna(x["unit_conversion_year"])
        else pd.to_numeric(x["start_year"], errors="coerce"),
        axis="columns",
    )


def main(
    gem_gbpt_path: str,
    technology_mapping: dict[str, str],
    fuel_mapping: dict[str, str],
    output_plants_path: str,
    output_fuels_path: str,
):
    """Obtain bioenergy power locations using GEM-GBPT data."""
    raw_df = gem.read_gem_dataset(gem_gbpt_path, ["Data", "Below Threshold"])

    powerplant_id = _utils.get_combined_text_col(
        raw_df, ["gem_location_id", "gem_phase_id"], prefix="GEM_"
    )
    bioenergy_df = gpd.GeoDataFrame(
        {
            "powerplant_id": powerplant_id,
            "name": _utils.get_combined_text_col(raw_df, ["project_name", "unit_name"]),
            "category": "bioenergy",
            "technology": technology_mapping["unknown"],
            "output_capacity_mw": raw_df["capacity_(mw)"],
            "start_year": _start_year(raw_df),
            "end_year": gem.year_col(raw_df, "end"),
            "status": gem.status_col(raw_df),
            "geometry": _utils.get_point_col(raw_df, "longitude", "latitude"),
            "ccs": False,
            "chp": False,
        }
    ).reset_index(drop=True)
    schema = _schemas.build_schema("bioenergy", technology_mapping, "prepare")
    schema.validate(bioenergy_df).to_parquet(output_plants_path)

    fuels_df = pd.DataFrame(
        {
            "powerplant_id": powerplant_id,
            "fuel": raw_df["fuel"].apply(
                gem.fuel_col,
                fuel_mapping=fuel_mapping,
                default=fuel_mapping["bioenergy: unknown"],
            ),
        }
    )
    fuels_df = fuels_df.explode("fuel").reset_index(drop=True)
    _schemas.FuelSchema.validate(fuels_df).to_parquet(output_fuels_path)


if __name__ == "__main__":
    main(
        gem_gbpt_path=snakemake.input.gem_gbpt,
        technology_mapping=snakemake.params.technology_mapping,
        fuel_mapping=snakemake.params.fuel_mapping,
        output_plants_path=snakemake.output.plants,
        output_fuels_path=snakemake.output.fuels,
    )
