"""Preparation of the Wind Energy Market Intelligence (WEMI) dataset.

This is a paid dataset updated yearly.
Available here: https://www.thewindpower.net/about_en.php
"""

import _schemas
import click
import geopandas as gpd
import numpy as np
import pandas as pd
from _utils import get_point_col

WEMI_CRS = "EPSG:4326"
KW_TO_MW = 1 / 1000
STATUS_MAPPING = {
    "Production": "operating",
    "Construction": "construction",
    "Dismantled": "retired",
    "Planned": "announced",
    "Approved": "pre-construction",
}


def _powerplant_id(raw_df: pd.DataFrame) -> pd.Series:
    return raw_df["ID"].apply(lambda x: f"WEMI_{x}")


def _technology(raw_df: pd.DataFrame) -> pd.Series:
    return raw_df["Offshore\nShore distance"].apply(
        lambda x: "offshore" if "yes" in str(x).lower() else "onshore"
    )


def _output_capacity_mw(raw_df: pd.DataFrame) -> pd.Series:
    return raw_df["Total power"].astype(float) * KW_TO_MW


def _start_year(raw_df: pd.DataFrame) -> pd.Series:
    year = raw_df["Commissioning date"].astype(str).str.extract(r"(\d{4})")[0]
    return pd.to_numeric(year, errors="coerce")


@click.command()
@click.argument("input_path")
@click.argument("output_path")
def main(input_path: str, output_path: str):
    """Saves a standardised and validated version of the WEMI dataset."""
    raw_df = pd.read_excel(
        input_path, sheet_name="Windfarms", skiprows=[1], na_values=["#ND"]
    )
    # Cleanup columns with problematic empty values.
    raw_df = raw_df.dropna(subset=["Total power", "Latitude", "Longitude"])

    start_year = _start_year(raw_df)
    processed_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _powerplant_id(raw_df),
            "name": raw_df["Name"],
            "category": "wind",
            "technology": _technology(raw_df),
            "output_capacity_mw": _output_capacity_mw(raw_df),
            "start_year": start_year,
            "end_year": np.nan,
            "status": raw_df["Status"].replace(STATUS_MAPPING),
            "geometry": get_point_col(raw_df, "Longitude", "Latitude", crs=WEMI_CRS),
        },
        crs=WEMI_CRS,
    )
    _schemas.PlantSchema.validate(processed_df).to_parquet(output_path)


if __name__ == "__main__":
    main()
