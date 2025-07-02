"""Prepare a clean CSP dataset using our schemas."""

import _gem as gem
import _schemas
import _utils
import click
import geopandas as gpd


@click.command()
@click.argument("gem_gspt_path")
@click.argument("output_path")
@click.option("--dc_ac_ratio", default=1.25)
def main(
    gem_gspt_path: str,
    output_path: str,
    dc_ac_ratio: float = 1.25,
):
    """Obtain concentrated solar power locations using GEM-GSPT data."""
    raw_gspt = gem.read_gem_dataset(gem_gspt_path, gem.GEM_GSPT_SHEETS)
    raw_gspt = raw_gspt[raw_gspt["Technology Type"] == "Solar Thermal"]

    csp_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _utils.get_combined_text_col(
                raw_gspt, ["GEM location ID", "GEM phase ID"], prefix="GEM_"
            ),
            "name": _utils.get_combined_text_col(
                raw_gspt, ["Project Name", "Phase Name"]
            ),
            "category": "solar",
            "technology": "concentrated solar",
            "output_capacity_mw": gem.output_capacity_mw_gspt(
                raw_gspt, dc_ac_ratio, "AC"
            ),
            "start_year": gem.gem_year_col(raw_gspt, "start"),
            "end_year": gem.gem_year_col(raw_gspt, "end"),
            "status": raw_gspt["Status"],
            "geometry": _utils.get_point_col(raw_gspt, "Longitude", "Latitude"),
        }
    )
    _schemas.PlantSchema.validate(csp_df).to_parquet(output_path)


if __name__ == "__main__":
    main()
