"""Prepare a clean CSP dataset using our schemas."""

import sys
from typing import TYPE_CHECKING, Any

import _gem as gem
import _schemas
import _utils
import geopandas as gpd

if TYPE_CHECKING:
    snakemake: Any

def prepare_solar_csp(
    gem_gspt_path: str, tech_name: str, dc_ac_ratio: float = 1
) -> gpd.GeoDataFrame:
    """Obtain CSP power locations using GEM-GSPT data."""
    raw_df = gem.read_gem_dataset(gem_gspt_path, gem.GEM_GSPT_SHEETS)
    raw_df = raw_df[raw_df["technology_type"] == "Solar Thermal"]

    csp_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _utils.get_combined_text_col(
                raw_df, ["gem_location_id", "gem_phase_id"], prefix="GEM_"
            ),
            "name": _utils.get_combined_text_col(
                raw_df, ["project_name", "phase_name"]
            ),
            "category": "solar",
            "technology": tech_name,
            "output_capacity_mw": gem.output_capacity_mw_gspt(
                raw_df, dc_ac_ratio, "AC"
            ),
            "start_year": gem.year_col(raw_df, "start"),
            "end_year": gem.year_col(raw_df, "end"),
            "status": gem.status_col(raw_df),
            "geometry": _utils.get_point_col(raw_df, "longitude", "latitude"),
        }
    ).reset_index(drop=True)
    schema = _schemas.build_schema({"csp": tech_name}, "prepare")
    return schema.validate(csp_df)


def main() -> None:
    """Main snakemake process."""
    csp_gdf = prepare_solar_csp(snakemake.input.gem_gspt, snakemake.params.csp_name)
    csp_gdf.to_parquet(snakemake.output.path)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
