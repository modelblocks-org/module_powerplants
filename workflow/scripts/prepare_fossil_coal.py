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
    "mothballed": "retired",
    "retired": "retired",
}


def _retired_year(gem_df: pd.DataFrame):
    """Return retirement year.

    GCPT has two separate columns for this, requiring special handling.
    """
    return gem_df.apply(
        lambda x: pd.to_numeric(x["retired_year"], errors="coerce")
        if pd.notna(x["retired_year"])
        else pd.to_numeric(x["planned_retirement"], errors="coerce"),
        axis="columns",
    )


def _ccs(gem_df: pd.DataFrame) -> pd.Series:
    """Get CCS status of powerplants.

    Can be inferred from technology and coal consumption nomenclature.
    """
    return (
        gem_df[["combustion_technology", "coal_type"]]
        .fillna("")
        .apply(
            lambda x: True
            if ("CCS" in x["combustion_technology"] or "CCS" in x["coal_type"])
            else False,
            axis="columns",
        )
    )



def _fuel(gem_df: pd.DataFrame) -> pd.Series:
    """Harmonise fuel nomenclature with other GEM datasets."""
    # bituminous -> coal: bituminous
    coal_type = "coal: " + gem_df["coal_type"].str.replace("with CCS", "").fillna(
        "unknown"
    )
    # bioenergy - paper mill wastes -> bioenergy: paper mill wastes
    alternate_fuel = gem_df["alternate_fuel"].str.replace(" - ", ": ")
    return coal_type.combine(
        alternate_fuel, lambda a, b: f"{a.strip()}, {b.strip()}" if pd.notna(b) else a
    )


def main(
    gem_gcpt_path: str,
    technology_mapping: dict[str, str],
    fuel_mapping: dict[str, str],
    output_plants_path: str,
    output_fuels_path: str,
):
    """Obtain coal power locations using GEM-GCPT data."""
    raw_df = gem.read_gem_dataset(gem_gcpt_path, ["Units"])

    powerplant_id = _utils.get_combined_text_col(
        raw_df, ["gem_location_id", "gem_unit/phase_id"], prefix="GEM_"
    )
    coal_df = gpd.GeoDataFrame(
        {
            "powerplant_id": powerplant_id,
            "name": _utils.get_combined_text_col(raw_df, ["plant_name", "unit_name"]),
            "category": "fossil",
            "technology": gem.technology_col(
                raw_df, technology_mapping, col="combustion_technology"
            ),
            "output_capacity_mw": raw_df["capacity_(mw)"],
            "start_year": gem.year_col(raw_df, "start"),
            "end_year": _retired_year(raw_df),
            "status": gem.status_col(raw_df, mapping=STATUS_MAPPING),
            "geometry": _utils.get_point_col(raw_df, "longitude", "latitude"),
            "ccs": _ccs(raw_df),
            "chp": False,  # Not specified in GCPT
        }
    ).reset_index(drop=True)
    schema = _schemas.build_schema("fossil", technology_mapping, "prepare")
    schema.validate(coal_df).to_parquet(output_plants_path)

    combined_fuel_col = _fuel(raw_df)
    fuels_df = pd.DataFrame(
        {
            "powerplant_id": powerplant_id,
            "fuel": combined_fuel_col.apply(
                gem.fuel_col,
                fuel_mapping=fuel_mapping,
                default=fuel_mapping["coal: unknown"],
            ),
        }
    )
    fuels_df = fuels_df.explode("fuel").reset_index(drop=True)
    _schemas.FuelSchema.validate(fuels_df).to_parquet(output_fuels_path)


if __name__ == "__main__":
    main(
        gem_gcpt_path=snakemake.input.gem_gcpt,
        technology_mapping=snakemake.params.technology_mapping,
        fuel_mapping=snakemake.params.fuel_mapping,
        output_plants_path=snakemake.output.plants,
        output_fuels_path=snakemake.output.fuels,
    )
