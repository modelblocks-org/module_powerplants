"""Prepare a clean hydropower dataset that fits our schema."""

import _schemas
import click
import geopandas as gpd
import numpy as np
import pandas as pd

CSV_PATH = "resources/automatic/glohydrores/data.csv"
CRS = "EPSG:4326"
TECH_MAPPING = {
    "STO": "reservoir",
    "Canal": "run of river",
    "ROR": "run of river",
    "PS": "pump storage",
    "unknown": "reservoir",
}


def _end_year(raw: pd.DataFrame, lifetime: int):
    """Estimate the end year using a lifetime."""
    return raw["year"] + lifetime


def _status(raw: pd.DataFrame, lifetime: int):
    """Get powerplant status using lifetime."""
    end_year = _end_year(raw, lifetime)
    start_year = raw["year"]
    diff = end_year - start_year
    return diff.apply(lambda x: "operating" if x > 0 else "retired")


def _geometry(raw: pd.DataFrame, lon_col: str, lat_col: str) -> gpd.GeoSeries:
    """Converts lat/long to point data."""
    return gpd.points_from_xy(raw[lon_col], raw[lat_col], crs=CRS)


def _technology(gem_df: pd.DataFrame) -> pd.Series:
    """Remap technology names, cleaning CCS specifics and inconsistencies."""
    return gem_df["plant_type"].fillna("unknown").apply(lambda x: TECH_MAPPING[x])


def _head_m(gem_df: pd.DataFrame) -> pd.Series:
    """Imputes the dam height if the head height is not available."""
    return gem_df.apply(
        lambda x: x["dam_height_m"]
        if pd.isna(x["head_m"]) and x["dam_height_m"] >= 0
        else x["head_m"],
        axis="columns",
    )


def _reservoir_km3(gem_df: pd.DataFrame) -> pd.Series:
    """Get reservoir volume if available."""
    return gem_df["res_vol_km3"].where(gem_df["res_vol_km3"] >= 0, np.nan)


@click.command()
@click.argument("input_path")
@click.argument("output_path")
@click.option("--lifetime", default=80)
def main(input_path: str, output_path: str, lifetime: int):
    """Prepare a cleaned hydropower dataset."""
    raw_df = pd.read_csv(input_path)
    hydro_df = gpd.GeoDataFrame(
        {
            "powerplant_id": raw_df["ID"].apply(lambda x: "GloHydroRes_" + x),
            "name": raw_df["name"],
            "category": "hydropower",
            "technology": _technology(raw_df),
            "output_capacity_mw": raw_df["capacity_mw"],
            "start_year": raw_df["year"],
            "end_year": _end_year(raw_df, lifetime),
            "status": _status(raw_df, lifetime),
            "geometry": _geometry(raw_df, "plant_lon", "plant_lat"),
            "head_m": _head_m(raw_df),
            "reservoir_km3": _reservoir_km3(raw_df),
        }
    )
    _schemas.HydroSchema.validate(hydro_df).to_parquet(output_path)


if __name__ == "__main__":
    main()
