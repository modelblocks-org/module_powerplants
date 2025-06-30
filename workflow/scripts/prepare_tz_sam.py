"""Processing of the Tranzition Zero Solar Asset Mapper (TZ-SAM) dataset."""
# %%

import sys
from typing import Literal

sys.path.append("workflow/scripts")

import _schemas
import _utils
import geopandas as gpd
import numpy as np
import pandas as pd

DC_AC_RATIO = 1.25
ASSUMED_CAPACITY_RATING = "AC"
TECHNOLOGY_MAPPING = {
    "PV": "utility pv",
    "Assumed PV": "utility pv",
    "Solar Thermal": "concentrated solar power",
}
CAPACITY_RATING_MAPPING = {"MWac": "AC", "MWp/dc": "DC"}


def _start_year_tz_sam(tz_dam_df: pd.DataFrame):
    """Assume installation occured in the middle of the detection window."""
    delta = (raw_tz_df["constructed_before"] - raw_tz_df["constructed_after"]) / 2
    return (raw_tz_df["constructed_after"] + delta).dt.year


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


# %%

raw_tz_df = gpd.read_file("resources/automatic/tz/sam.gpkg")
raw_tz_df.head()

# %%
raw_gem_df = _utils.read_gem_dataset(
    "resources/automatic/gem/solar.xlsx", ["20 MW+", "1-20 MW"]
)
raw_gem_df["Technology Type"] = raw_gem_df["Technology Type"].fillna("Assumed PV")
raw_gem_df.head()

# %%
processed_tz_df = gpd.GeoDataFrame(
    {
        "powerplant_id": "TZ-SAM_" + raw_tz_df["cluster_id"],
        "name": "TZ-SAM cluster " + raw_tz_df["cluster_id"],
        "category": "solar",
        "technology": "utility pv",
        "output_capacity_mw": raw_tz_df["capacity_mw"],
        "start_year": _start_year_tz_sam(raw_tz_df),
        "end_year": np.nan,
        "geometry": raw_tz_df["geometry"],
    }
)
processed_tz_df.head()
# %%
raw_gem_util_pv_df = raw_gem_df[
    raw_gem_df["Technology Type"].isin(["Assumed PV", "PV"])
]

processed_gem_util_pv_df = gpd.GeoDataFrame(
    {
        "powerplant_id": _utils.get_combined_text_col(
            raw_gem_util_pv_df, ["GEM location ID", "GEM phase ID"], prefix="GEM_"
        ),
        "name": _utils.get_combined_text_col(
            raw_gem_util_pv_df, ["Project Name", "Phase Name"]
        ),
        "category": "solar",
        "technology": "utility pv",
        "output_capacity_mw": _output_capacity_mw_gem_gspt(
            raw_gem_util_pv_df, DC_AC_RATIO, "AC"
        ),
        "start_year": _utils.gem_year_col(raw_gem_util_pv_df, "start"),
        "end_year": _utils.gem_year_col(raw_gem_util_pv_df, "end"),
        "geometry": _utils.get_point_col(raw_gem_util_pv_df, "Longitude", "Latitude"),
    }
)

# %%
points_in_polys = gpd.sjoin(
    processed_gem_util_pv_df, processed_tz_df[["geometry"]],
    how="inner",
    predicate="within"
)
inferred_start_year = points_in_polys.groupby("index_right")["start_year"].mean().round().dropna()
inferred_end_year = points_in_polys.groupby("index_right")["end_year"].mean().round().dropna()

# %%
processed_tz_df["start_year"].update(inferred_start_year)
processed_tz_df["end_year"].update(inferred_end_year)

# %%
