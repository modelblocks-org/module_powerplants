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


def _retired_year(gem_df: pd.DataFrame):
    """Return retirement year.

    GOGPT has two separate columns for this, requiring special handling.
    """
    return gem_df.apply(
        lambda x: (
            pd.to_numeric(x["retired_year"], errors="coerce")
            if pd.notna(x["retired_year"])
            else pd.to_numeric(x["planned_retire"], errors="coerce")
        ),
        axis="columns",
    )


def prepare_gem_gogpt(
    gem_gogpt_path: str,
    technology_mapping: dict[str, str],
    fuel_mapping: dict[str, str],
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """Obtain oil and gas power plants using GEM-GOGPT data."""
    raw_df = gem.read_gem_dataset(
        gem_gogpt_path, ["Gas & Oil Units", "sub-threshold units"]
    )

    fuels_df, fuel_class = gem.get_unique_fuel_dataset(
        raw_df["fuel"], fuel_mapping, "fossil gas: unknown", "og"
    )

    oil_gas_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _utils.get_combined_text_col(
                raw_df, ["gem_location_id", "gem_unit_id"], prefix="GEM_"
            ),
            "name": _utils.get_combined_text_col(raw_df, ["plant_name", "unit_name"]),
            "category": "fossil",
            "technology": gem.technology_col(
                raw_df, technology_mapping, col="turbine/engine_technology"
            ),
            "output_capacity_mw": raw_df["capacity_(mw)"],
            "start_year": gem.year_col(raw_df, "start"),
            "end_year": _retired_year(raw_df),
            "status": gem.status_col(raw_df),
            "geometry": _utils.get_point_col(raw_df, "longitude", "latitude"),
            "ccs": raw_df["ccs_attachment?"] == "yes",
            "chp": raw_df["chp"] == "yes",
            "fuel_class": fuel_class,
        }
    ).reset_index(drop=True)
    schema = _schemas.build_schema(technology_mapping, "prepare")
    return schema.validate(oil_gas_df), _schemas.FuelSchema.validate(fuels_df)


def main() -> None:
    """Main snakemake process."""
    og_plants, og_fuels = prepare_gem_gogpt(
        gem_gogpt_path=snakemake.input.gem_gogpt,
        technology_mapping=snakemake.params.technology_mapping,
        fuel_mapping=snakemake.params.fuel_mapping,
    )
    og_plants.to_parquet(snakemake.output.plants)
    og_fuels.to_parquet(snakemake.output.fuels)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
