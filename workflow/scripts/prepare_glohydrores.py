"""Prepare a clean hydropower dataset that fits our schema."""

import geopandas as gpd
import pandas as pd

CSV_PATH = "resources/automatic/glohydrores/data.csv"
LIFETIME = 80
CRS = "EPSG:4326"
TECH_MAPPING = {
    "STO": "reservoir",
    "Canal": "run of river",
    "ROR": "run of river",
    "PS": "pump storage",
    "unknown": "reservoir",
}

def get_end_year(raw: pd.DataFrame):
    """Estimate the end year using a lifetime."""
    return raw["year"] + LIFETIME


def get_status(raw: pd.DataFrame):
    """Get powerplant status using lifetime."""
    end_year = get_end_year(raw)
    start_year = raw["year"]
    diff = end_year - start_year
    return diff.apply(lambda x: "operating" if x < 0 else "retired")


def get_geometry(
    raw: pd.DataFrame, lon_col: str, lat_col: str
) -> gpd.GeoSeries:
    """Converts lat/long to point data."""
    return gpd.points_from_xy(raw[lon_col], raw[lat_col], crs=CRS)


def get_technology(gem_df: pd.DataFrame) -> pd.Series:
    """Remap technology names, cleaning CCS specifics and inconsistencies."""
    return gem_df["plant_type"].fillna("unknown").apply(lambda x: TECH_MAPPING[x])


def get_head_m(gem_df: pd.DataFrame) -> pd.Series:
    """Will impute the dam height if the head height is not available."""
    return gem_df.apply(
        lambda x: x["dam_height_m"]
        if pd.isna(x["head_m"]) and x["dam_height_m"] >= 0
        else x["head_m"],
        axis="columns",
    )
