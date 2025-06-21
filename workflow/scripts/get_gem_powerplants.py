"""Process and clean GEM data to fit our powerplant schemas."""

import re
import sys
from typing import TYPE_CHECKING, Any, TypedDict

import _schemas
import geopandas as gpd
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")

CANCELLED_PROJECT_STATUS = ["cancelled", "shelved"]
GROSS_TO_NET_CAPACITY_RATIO = {
    "coal": 0.95
}
GEM_EPSG = "EPSG:4326"

class SmkOutputs(TypedDict):
    """Output paths specified by snakemake."""
    coal: str
    biomass: str
    oil_and_gas: str
    geothermal: str

def remove_cancelled_projects(gem_raw: pd.DataFrame):
    """Projects that are no longer in consideration should be removed."""
    idx_cancelled = []
    for idx, row in gem_raw.iterrows():
        if any([i in row["Status"] for i in CANCELLED_PROJECT_STATUS]):
            idx_cancelled.append(idx)
    return gem_raw.drop(idx_cancelled, axis="index")


def get_powerplant_id(gem_df: pd.DataFrame) -> pd.Series:
    """Generate a unique id by combining GEM identifiers."""
    return gem_df.apply(
        lambda x: f"GEM_{x['GEM location ID']}_{x['GEM unit/phase ID']}", axis="columns"
    )


def get_name(gem_df: pd.DataFrame) -> pd.Series:
    """Create a unique name by combining the powerplant and unit names."""
    return gem_df.apply(
        lambda x: f"{x['Plant / Project name']}_{x['Unit / Phase name']}",
        axis="columns",
    )


def get_status(gem_df: pd.DataFrame) -> pd.Series:
    """Clean the GEM status by removing chaff text.

    E.g.: 'mothballed - inferred 4 y' -> 'mothballed'.
    """
    return gem_df["Status"].apply(lambda x: x.split("-")[0].strip())


def get_geometry(gem_df: pd.DataFrame) -> gpd.GeoSeries:
    """Convert lat/lon values to point data."""
    return gpd.points_from_xy(gem_df["Longitude"], gem_df["Latitude"], crs=GEM_EPSG)


def get_ccs(gem_df: pd.DataFrame) -> pd.Series:
    """Ensures that CCS facilities are identified."""
    return (
        gem_df[["Technology", "CCS", "Fuel"]]
        .fillna("")
        .apply(
            lambda x: True
            if ("CCS" in x["Technology"] or "yes" in x["CCS"] or "CCS" in x["Fuel"])
            else False,
            axis="columns",
        )
    )


def get_chp(gem_df: pd.DataFrame) -> pd.Series:
    """Ensures that CHP facilities are identified."""
    return gem_df["CHP"].fillna("").apply(lambda x: True if "yes" in x else False)

def get_coal_powerplants(gem_raw: pd.DataFrame) -> gpd.GeoDataFrame:
    """Create a validated dataset of coal plants."""
    coal_raw = gem_raw[gem_raw["Type"] == "coal"]

    # ASSUMPTION: unknown facilities are steam turbines.
    def _get_coal_tech(raw_tech: str):
        if "unknown" in raw_tech:
            tech = "steam turbine"
        elif "IGCC" in raw_tech:
            tech = "gas turbine"
        elif any([i in raw_tech for i in ["subcritical", "supercritical", "ultra-supercritical", "CFB"]]):
            tech = "steam turbine"
        else:
            raise ValueError(f"Could not process coal tech correctly. Found {raw_tech}")
        return tech

    # ASSUMPTION: unknown facilities are subcritical
    def _get_coal_subtechnology(raw_tech: str):
        tech = raw_tech.replace("/CCS", "").strip()
        if "unknown" in tech:
            tech = "subcritical"
        return tech

    # ASSUMPTION: unknown fuel input is bituminous coal
    def _get_coal_fuel(raw_fuel: str):
        fuel = raw_fuel.replace("with CCS", "").strip()
        if "unknown" in raw_fuel:
            fuel = "bituminous"
        return fuel

    coal_processed = gpd.GeoDataFrame(
        {
            "powerplant_id": get_powerplant_id(coal_raw),
            "name": get_name(coal_raw),
            "category": coal_raw["Type"],
            "technology": coal_raw["Technology"].apply(_get_coal_tech),
            "subtechnology": coal_raw["Technology"].apply(_get_coal_subtechnology),
            "fuel": coal_raw["Fuel"].apply(_get_coal_fuel),
            "ccs": get_ccs(coal_raw),
            "chp": get_chp(coal_raw),
            "net_output_capacity_mw": coal_raw["Capacity (MW)"] * GROSS_TO_NET_CAPACITY_RATIO["coal"],
            "start_year": coal_raw["Start year"],
            "end_year": coal_raw["Retired year"],
            "status": coal_raw["Status"],
            "geometry": get_geometry(coal_raw)
        }
    )
    return _schemas.CoalSchema.validate(coal_processed)

def process_gem_powerplants(gem_path: str, outputs: SmkOutputs):
    gem_raw = pd.read_excel(gem_path, sheet_name="Power facilities")
    pattern = "|".join(map(re.escape, CANCELLED_PROJECT_STATUS))
    mask = gem_raw["Status"].str.contains(pattern, na=False)
    gem_raw = gem_raw[~mask]

    coal_plants = get_coal_powerplants(gem_raw)
    coal_plants.to_parquet(outputs["coal"])
