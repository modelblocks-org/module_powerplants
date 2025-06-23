"""Process and clean GEM data to fit our powerplant schemas."""

import re
import sys
from typing import TYPE_CHECKING, Any, Literal, TypedDict

import _schemas
import geopandas as gpd
import numpy as np
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")


class SmkInput(TypedDict):
    """Snakemake inputs."""

    gem_raw: str


class SmkOutput(TypedDict):
    """Snakemake outputs."""

    combustion_plants: str
    combustion_plant_fuels: str


class CategoryParam(TypedDict):
    """Powerplant categories."""

    bioenergy: dict[str, str]
    coal: dict[str, str]
    oil_n_gas: dict[str, str]


class SmkParams(TypedDict):
    """Snakemake parameters."""

    fuel_mapping: dict[str, str]
    default_fuel: CategoryParam
    technology_mapping: CategoryParam


FUEL_MAPPING = snakemake.params["fuel_mapping"]
FUEL_PATTERNS = {
    key: (
        re.compile(rf"\b{re.escape(key)}\b", flags=re.IGNORECASE)
        if re.fullmatch(
            r"\w+", key.replace(" ", "")
        )  # only wrap in \b if key is a single word/phrase
        else re.compile(re.escape(key), flags=re.IGNORECASE)
    )
    for key in FUEL_MAPPING
}
GEM_CRS = "EPSG:4326"

def _get_fuel(cell: str, default: str):
    """Find and replace fuel names using pattern matching.

    Ambiguous cases should raise errors.
    """
    fuels = []
    if pd.isna(cell):
        fuels.append(default)
    else:
        for value in cell.split(","):
            if any([i in value for i in ["unknown", "other: other"]]):
                fuels.append(default)
                continue
            matched_keys = [
                FUEL_MAPPING[key]
                for key, pattern in FUEL_PATTERNS.items()
                if pattern.search(value.strip())
            ]
            if len(set(matched_keys)) != 1:
                raise ValueError(
                    f"Ambiguous fuel definition for '{value}': found '{matched_keys}'."
                )
            fuels.append(matched_keys[0])
    return fuels


def _get_powerplant_id(gem_df: pd.DataFrame) -> pd.Series:
    """Create a unique identifier using GEM codes."""
    return gem_df.apply(
        lambda x: f"GEM_{x['GEM location ID']}_{x['GEM unit/phase ID']}", axis="columns"
    )


def _get_name(gem_df: pd.DataFrame) -> pd.Series:
    """Create a unique name using GEM data."""
    return gem_df.apply(
        lambda x: f"{x['Plant / Project name']}_{x['Unit / Phase name']}",
        axis="columns",
    )


def _get_geometry(gem_df: pd.DataFrame, crs: str) -> gpd.GeoSeries:
    """Converts lat/long to point data."""
    return gpd.points_from_xy(gem_df["Longitude"], gem_df["Latitude"], crs=crs)


def _get_technology(gem_df: pd.DataFrame, mapping: dict[str, str]) -> pd.Series:
    """Remap technology names, cleaning CCS specifics."""
    return (
        gem_df["Technology"]
        .fillna("unknown")
        .apply(lambda x: mapping[x.replace("/CCS", "").strip()])
    )


def _get_ccs(gem_df: pd.DataFrame) -> pd.Series:
    """Get CCS status of powerplants."""
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


def _get_year(gem_df: pd.DataFrame, option: str = Literal["start", "end"]):
    """Get start/end year, ensuring typing is respected."""
    mapping = {"start": "Start year", "end": "Retired year"}
    return gem_df[mapping[option]].apply(lambda x: np.NaN if x == "not found" else x)


def _get_chp(gem_df: pd.DataFrame) -> pd.Series:
    """Get CHP status of powerplants."""
    return gem_df["CHP"].fillna("").apply(lambda x: True if "yes" in x else False)


def get_combustion_capacity_df(
    raw_df: pd.DataFrame, tech_mapping: dict[str, str]
) -> gpd.GeoDataFrame:
    """Obtain a standardised dataset for combustion powerplants."""
    capacity_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _get_powerplant_id(raw_df),
            "name": _get_name(raw_df),
            "category": raw_df["Type"],
            "technology": _get_technology(raw_df, tech_mapping),
            "ccs": _get_ccs(raw_df),
            "chp": _get_chp(raw_df),
            "output_capacity_mw": raw_df["Capacity (MW)"],
            "start_year": _get_year(raw_df, "start"),
            "end_year": _get_year(raw_df, "end"),
            "status": raw_df["Status"],
            "geometry": _get_geometry(raw_df, GEM_CRS),
        }
    )
    return _schemas.CombustionSchema.validate(capacity_df)


def get_combustion_fuel_df(raw_df: pd.DataFrame, default: str):
    """Get a 'long' dataframe with fuels used per powerplant."""
    fuels_df = pd.DataFrame(
        {
            "powerplant_id": _get_powerplant_id(raw_df),
            "fuel": raw_df["Fuel"].apply(_get_fuel, default=default),
        }
    )
    fuels_df = fuels_df.explode("fuel").reset_index(drop=True)
    return _schemas.FuelSchema.validate(fuels_df)


def main(inputs: SmkInput, outputs: SmkOutput, params: SmkParams):
    """Process and save datasets for GEM powerplants.

    These include fossil powerplants (coal, oil_gas), geothermal and nuclear.

    Args:
        inputs (SmkInput): Snakemake inputs.
        outputs (SmkOutput): Snakemake outputs.
        params (SmkParams): Snakemake parameters.
    """
    # Get raw dataset without cancelled projects
    gem_raw = pd.read_excel(inputs["gem_raw"], sheet_name="Power facilities")
    pattern = "|".join(map(re.escape, ["cancelled", "shelved"]))
    mask = gem_raw["Status"].str.contains(pattern, na=False)
    gem_raw = gem_raw[~mask]
    gem_raw["Type"] = gem_raw["Type"].replace("oil/gas", "oil_gas")

    # Process combustion powerplants.
    capacity_dfs = []
    fuel_dfs = []
    for category in ["bioenergy", "coal", "oil_gas"]:
        raw = gem_raw[gem_raw["Type"] == category]
        capacity_dfs.append(
            get_combustion_capacity_df(raw, params["technology_mapping"][category])
        )
        fuel_dfs.append(get_combustion_fuel_df(raw, params["default_fuel"][category]))
    combined_combustion_cap = _schemas.CombustionSchema.validate(
        pd.concat(capacity_dfs)
    )
    combined_combustion_fuel = _schemas.FuelSchema.validate(pd.concat(fuel_dfs))
    combined_combustion_cap.to_parquet(outputs["combustion_plants"])
    combined_combustion_fuel.to_parquet(outputs["combustion_plant_fuels"])


if __name__ == "__main__":
    main(snakemake.input, snakemake.output, snakemake.params)
