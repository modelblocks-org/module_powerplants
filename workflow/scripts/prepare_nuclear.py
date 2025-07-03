"""Prepare a nuclear power dataset using our schemas."""

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


def main(
    gem_gnpt_path: str, technology_mapping: dict[str, str], output_plants_path: str
):
    """Obtain nuclear power plants using GEM-GNPT data."""
    raw_df = gem.read_gem_dataset(gem_gnpt_path, ["Data"])

    nuclear_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _utils.get_combined_text_col(
                raw_df, ["gem_location_id", "gem_unit_id"], prefix="GEM_"
            ),
            "name": _utils.get_combined_text_col(raw_df, ["project_name", "unit_name"]),
            "category": "nuclear",
            "technology": gem.technology_col(
                raw_df, technology_mapping, col="reactor_type"
            ),
            "output_capacity_mw": raw_df["capacity_(mw)"],
            "start_year": pd.to_datetime(
                raw_df["commercial_operation_date"], format="mixed"
            ).dt.year,
            "end_year": pd.to_datetime(
                raw_df["retirement_date"], format="mixed"
            ).dt.year,
            "status": raw_df["status"],
            "geometry": _utils.get_point_col(raw_df, "longitude", "latitude"),
        }
    ).reset_index(drop=True)
    _schemas.PlantSchema.validate(nuclear_df).to_parquet(output_plants_path)


if __name__ == "__main__":
    main(
        gem_gnpt_path=snakemake.input.gem_gnpt,
        technology_mapping=snakemake.params.technology_mapping,
        output_plants_path=snakemake.output.plants,
    )
