"""Prepare a coal dataset using our schemas."""

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

STATUS_MAPPING = {
    "announced": "announced",
    "pre-permit": "pre-construction",
    "permitted": "pre-construction",
    "construction": "construction",
    "operating": "operating",
    "mothballed": "mothballed",
    "retired": "retired",
}


def _retired_year(gem_df: pd.DataFrame):
    """Return retirement year.

    GCPT has two separate columns for this, requiring special handling.
    """
    return gem_df.apply(
        lambda x: pd.to_numeric(x["Retired year"], errors="coerce")
        if pd.notna(x["Retired year"])
        else pd.to_numeric(x["Planned retirement"], errors="coerce"),
        axis="columns",
    )


def _ccs(gem_df: pd.DataFrame) -> pd.Series:
    """Get CCS status of powerplants.

    Can be inferred from technology and coal consumption nomenclature.
    """
    return (
        gem_df[["Combustion technology", "Coal type"]]
        .fillna("")
        .apply(
            lambda x: True
            if ("CCS" in x["Combustion technology"] or "CCS" in x["Coal type"])
            else False,
            axis="columns",
        )
    )

def _status(gem_df: pd.DataFrame) -> pd.Series:
    """Get harmonised plant status."""
    return gem_df["Status"].map(STATUS_MAPPING)

def main(
    gem_gcpt_path: str,
    technology_mapping: dict[str, str],
    fuel_mapping: dict[str, str],
    output_plants_path: str,
    output_fuels_path: str
):
    """Obtain concentrated solar power locations using GEM-GSPT data."""
    raw_df = gem.read_gem_dataset(gem_gcpt_path, ["Units"])

    coal_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _utils.get_combined_text_col(
                raw_df, ["GEM location ID", "GEM unit/phase ID"], prefix="GEM_"
            ),
            "name": _utils.get_combined_text_col(raw_df, ["Plant name", "Unit name"]),
            "category": "coal",
            "technology": gem.technology_col(
                raw_df, technology_mapping, col="Combustion technology"
            ),
            "output_capacity_mw": raw_df["Capacity (MW)"],
            "start_year": gem.gem_year_col(raw_df, "start"),
            "end_year": _retired_year(raw_df),
            "status": _status(raw_df),
            "geometry": _utils.get_point_col(raw_df, "Longitude", "Latitude"),
            "ccs": _ccs(raw_df),
            "chp": False
        }
    )
    coal_df = coal_df.reset_index(drop=True)
    _schemas.CombustionSchema.validate(coal_df).to_parquet(output_plants_path)

    combined_fuel_col = raw_df["Coal type"] + "," + raw_df["Alternate Fuel"]
    fuels_df = pd.DataFrame(
            {
                "powerplant_id": _utils.get_combined_text_col(
                raw_df, ["GEM location ID", "GEM unit/phase ID"], prefix="GEM_"
            ),
                "fuel": combined_fuel_col.apply(gem.fuel_col, fuel_mapping=fuel_mapping),
            }
        )
    fuels_df = fuels_df.explode("fuel").reset_index(drop=True)
    _schemas.FuelSchema.validate(fuels_df).to_parquet(output_fuels_path)


if __name__ == "__main__":
    main(
        gem_gcpt_path=snakemake.input.gem_gcpt,
        technology_mapping= snakemake.params.technology_mapping,
        fuel_mapping=snakemake.params.fuel_mapping,
        output_plants_path=snakemake.output.plants,
        output_fuels_path=snakemake.output.fuels
    )
