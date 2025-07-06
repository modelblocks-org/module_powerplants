"""Prepare a clean wind power dataset using our schemas.

Uses the Wind Energy Market Intelligence (WEMI) dataset.
This is a paid dataset updated yearly.
Available here: https://www.thewindpower.net/about_en.php
"""

import _gem as gem
import _schemas
import _utils
import click
import geopandas as gpd
import pandas as pd
import yaml

INTERNAL_TECH_MAPPING = {
    "Onshore": "onshore",
    "Offshore floating": "offshore",
    "Offshore hard mount": "offshore",
    "Offshore mount unknown": "offshore",
}


def _technology(gem_df: pd.DataFrame):
    return  gem_df["installation_type"].apply(lambda x: INTERNAL_TECH_MAPPING[x])


@click.command()
@click.argument("gem_gwpt_path", type=str)
@click.argument("tech_mapping", type=str)
@click.argument("output_path", type=str)
def main(
    gem_gwpt_path: str,
    tech_mapping: str,
    output_path: str,
):
    """Obtain concentrated wind power locations using GEM-GSPT data."""
    raw_df = gem.read_gem_dataset(gem_gwpt_path, gem.GEM_GWPT_SHEETS)
    # Remove unknown installation types to avoid misplacement
    raw_df = raw_df[raw_df["installation_type"] != "Unknown"]
    raw_df = raw_df.dropna(subset=["installation_type"])

    tech_map = yaml.safe_load(tech_mapping)

    wind_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _utils.get_combined_text_col(
                raw_df, ["gem_location_id", "gem_phase_id"], prefix="GEM_"
            ),
            "name": _utils.get_combined_text_col(
                raw_df, ["project_name", "phase_name"]
            ),
            "category": "wind",
            "technology": _technology(raw_df).map(tech_map),
            "output_capacity_mw": raw_df["capacity_(mw)"],
            "start_year": gem.year_col(raw_df, "start"),
            "end_year": gem.year_col(raw_df, "end"),
            "status": gem.status_col(raw_df),
            "geometry": _utils.get_point_col(raw_df, "longitude", "latitude"),
        }
    )
    schema = _schemas.build_schema("wind", tech_map, "prepare")
    schema.validate(wind_df).to_parquet(output_path)


if __name__ == "__main__":
    main()
