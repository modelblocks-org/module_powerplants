"""Prepare a clean concentrated solar dataset using our schemas."""

import _gem as gem
import _schemas
import _utils
import click
import geopandas as gpd


@click.command()
@click.argument("gem_gspt_path", type=click.Path(dir_okay=False))
@click.option("-o", "output_path", type=click.Path(dir_okay=False), required=True)
@click.option("-t", "tech_name", type=str, default="csp")
@click.option("-r", "dc_ac_ratio", default=1.25)
def main(
    gem_gspt_path: str, tech_name: str, output_path: str, dc_ac_ratio: float = 1.25
):
    """Obtain concentrated solar power locations using GEM-GSPT data."""
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
    schema.validate(csp_df).to_parquet(output_path)


if __name__ == "__main__":
    main()
