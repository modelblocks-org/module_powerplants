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

TECHNOLOGY_MAPPING = {
    "Onshore": "onshore",
    "Offshore floating": "offshore",
    "Offshore hard mount": "offshore",
    "Offshore mount unknown": "offshore",
}


def _technology(gem_df: pd.DataFrame):
    return gem_df["Installation Type"].apply(lambda x: TECHNOLOGY_MAPPING[x])


@click.command()
@click.argument("gem_gwpt_path")
@click.argument("output_path")
def main(
    gem_gwpt_path: str,
    output_path: str,
):
    """Obtain concentrated solar power locations using GEM-GSPT data."""
    raw_gspt = gem.read_gem_dataset(gem_gwpt_path, gem.GEM_GWPT_SHEETS)
    raw_gspt = raw_gspt[raw_gspt["Installation Type"] != "Unknown"]
    raw_gspt = raw_gspt.dropna(subset=["Installation Type"])

    wind_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _utils.get_combined_text_col(
                raw_gspt, ["GEM location ID", "GEM phase ID"], prefix="GEM_"
            ),
            "name": _utils.get_combined_text_col(
                raw_gspt, ["Project Name", "Phase Name"]
            ),
            "category": "wind",
            "technology": _technology(raw_gspt),
            "output_capacity_mw": raw_gspt["Capacity (MW)"],
            "start_year": gem.gem_year_col(raw_gspt, "start"),
            "end_year": gem.gem_year_col(raw_gspt, "end"),
            "status": raw_gspt["Status"],
            "geometry": _utils.get_point_col(raw_gspt, "Longitude", "Latitude"),
        }
    )
    _schemas.PlantSchema.validate(wind_df).to_parquet(output_path)


if __name__ == "__main__":
    main()
