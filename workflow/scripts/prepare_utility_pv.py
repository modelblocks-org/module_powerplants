"""Processing of the Tranzition Zero Solar Asset Mapper (TZ-SAM) dataset."""

from typing import Literal

import _schemas
import _utils
import click
import geopandas as gpd
import numpy as np
import pandas as pd

ASSUMED_CAPACITY_RATING = "AC"
TECHNOLOGY_MAPPING = {
    "PV": "utility pv",
    "Assumed PV": "utility pv",
    "Solar Thermal": "concentrated solar power",
}
CAPACITY_RATING_MAPPING = {"MWac": "AC", "MWp/dc": "DC"}

GEM_GSPT_SHEETS = ["20 MW+", "1-20 MW"]


def _start_year_tz_sam(tz_dam_df: pd.DataFrame):
    """Assume installation occured in the middle of the detection window."""
    delta = (tz_dam_df["constructed_before"] - tz_dam_df["constructed_after"]) / 2
    return (tz_dam_df["constructed_after"] + delta).dt.year


def _output_capacity_mw_gem_gspt(
    gem_df: pd.DataFrame, dc_ac_ratio: float, default_rating: Literal["AC", "DC"]
):
    """Obtain capacity, applying DC-to-AC ratios where necessary."""
    rating = (
        gem_df["Capacity Rating"]
        .fillna("unknown")
        .replace("unknown", default_rating)
        .replace(CAPACITY_RATING_MAPPING)
    )
    invalid_ratings = set(rating.unique()) - set(CAPACITY_RATING_MAPPING.values())
    assert not invalid_ratings, f"GEM GSPT: invalid capacity ratings {invalid_ratings}."

    cap_df = pd.concat([gem_df["Capacity (MW)"], rating], axis="columns")
    return cap_df.apply(
        lambda x: x["Capacity (MW)"]
        if x["Capacity Rating"] == "AC"
        else x["Capacity (MW)"] / dc_ac_ratio,
        axis="columns",
    )


def fill_tz_with_gem(
    gem_df: gpd.GeoDataFrame, tz_sam_df: gpd.GeoDataFrame, proj_crs="epsg:3857"
) -> gpd.GeoDataFrame:
    """Fills in GEM data that is ambiguous in TZ-SAM's satellite-based approach.

    Handles cases where multiple points fall within the same polygon.
    """
    filled_tz_sam = tz_sam_df.copy()

    within = gpd.sjoin(
        gem_df.to_crs(proj_crs),
        tz_sam_df.to_crs(proj_crs)[["geometry"]],
        how="inner",
        predicate="within",
    )

    # Assume start/end years are the mean of the value stated in GEM.
    start_year = within.groupby("index_right")["start_year"].mean().round().dropna()
    end_year = within.groupby("index_right")["end_year"].mean().round().dropna()

    # Assume the status is the mode (most common category).
    status = within.groupby("index_right")["status"].agg(pd.Series.mode)
    # Drop ambiguous cases (two status appear in equal amounts).
    status = status[status.apply(lambda x: isinstance(x, str))]

    filled_tz_sam.loc[start_year.index, "start_year"] = start_year
    filled_tz_sam.loc[end_year.index, "end_year"] = end_year
    filled_tz_sam.loc[status.index, "status"] = status

    return filled_tz_sam


def get_gem_mismatch(
    gem_df: gpd.GeoDataFrame,
    tz_sam_df: gpd.GeoDataFrame,
    valid_status: list[str] | None = None,
    buffer=1000,
    proj_crs="epsg:3857",
) -> gpd.GeoDataFrame:
    """Estimation of future projects missed by TZ-SAM's satellite-based approach.

    To avoid double counting:
    - Only projects with known pre-operation states are accepted.
    - Only projects outside a buffer are accepted (default 1 km).
    """
    if gem_df.crs != tz_sam_df.crs:
        raise ValueError("GEM and TZ-SAM CRS mismatch.")

    # Filter future projects
    if valid_status:
        future_gem_df = gem_df[gem_df["status"].isin(valid_status)]
    else:
        future_gem_df = gem_df

    # Buffer around the points using a projected CRS
    future_gem_buffered = future_gem_df.copy()
    future_gem_buffered["geometry"] = (
        future_gem_buffered["geometry"].to_crs(proj_crs).buffer(buffer)
    )

    intersecting = gpd.sjoin(
        future_gem_buffered,
        tz_sam_df.to_crs(proj_crs)[["geometry"]],
        how="inner",
        predicate="intersects",
    )

    return future_gem_df.loc[~future_gem_df.index.isin(intersecting.index)]

@click.command()
@click.argument("tz_sam_path")
@click.argument("gem_gspt_path")
@click.argument("output_path")
@click.option("--dc_ac_ratio", default=1.25)
def main(tz_sam_path: str, gem_gspt_path: str, output_path, dc_ac_ratio):
    """Combine GEM and TZ-SAM data."""
    raw_tz_df = gpd.read_file(tz_sam_path)
    tz_df = gpd.GeoDataFrame(
        {
            "powerplant_id": "TZ-SAM_" + raw_tz_df["cluster_id"],
            "name": "TZ-SAM cluster " + raw_tz_df["cluster_id"],
            "category": "solar",
            "technology": "utility pv",
            "output_capacity_mw": raw_tz_df["capacity_mw"],
            "start_year": _start_year_tz_sam(raw_tz_df),
            "end_year": np.nan,
            "status": "operating",
            "geometry": raw_tz_df["geometry"],
        }
    )

    # Get only Utility PV facilities.
    raw_gem_df = _utils.read_gem_dataset(gem_gspt_path, GEM_GSPT_SHEETS)
    raw_gem_df["Technology Type"] = raw_gem_df["Technology Type"].fillna("Assumed PV")
    raw_gem_df = raw_gem_df[raw_gem_df["Technology Type"].isin(["Assumed PV", "PV"])]
    gem_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _utils.get_combined_text_col(
                raw_gem_df, ["GEM location ID", "GEM phase ID"], prefix="GEM_"
            ),
            "name": _utils.get_combined_text_col(
                raw_gem_df, ["Project Name", "Phase Name"]
            ),
            "category": "solar",
            "technology": "utility pv",
            "output_capacity_mw": _output_capacity_mw_gem_gspt(
                raw_gem_df, dc_ac_ratio, "AC"
            ),
            "start_year": _utils.gem_year_col(raw_gem_df, "start"),
            "end_year": _utils.gem_year_col(raw_gem_df, "end"),
            "status": raw_gem_df["Status"],
            "geometry": _utils.get_point_col(raw_gem_df, "Longitude", "Latitude"),
        }
    )

    filled_tz_df = fill_tz_with_gem(gem_df, tz_df)
    gem_mismatch_df = get_gem_mismatch(
        gem_df,
        tz_df,
        valid_status=["announced", "pre-construction", "construction", "retired"],
    )

    utility_pv = pd.concat([filled_tz_df, gem_mismatch_df], axis="index")
    utility_pv = utility_pv.reset_index(drop=True)
    _schemas.PlantSchema.validate(utility_pv).to_parquet(output_path)


if __name__ == "__main__":
    main()
