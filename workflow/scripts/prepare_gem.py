"""Prepare clean GEM datasets that fit our schemas."""

import re
import sys
from typing import TYPE_CHECKING, Any

import _gem as gem
import _schemas
import _utils
import geopandas as gpd
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")

CATEGORY_MAPPING = {"oil/gas": "oil_gas"}
COMBUSTION_CATEGORIES = ["bioenergy", "coal", "oil_gas"]

FUEL_MAPPING = {
    "fossil gas: natural gas": "natural gas",
    "fossil gas: LNG": "natural gas",
    "fossil gas: waste heat from natural gas": "waste heat",
    "industrial by-product": "waste heat",
    "fossil gas: coalbed methane": "natural gas",
    "fossil liquids: crude oil": "oil",
    "fossil liquids: diesel": "diesel",
    "fossil liquids: fuel oil": "oil",
    "fossil liquids: heavy fuel oil": "oil",
    "fossil liquids: light fuel oil": "oil",
    "fossil liquids: petroleum coke": "oil",
    "fossil liquids: jet fuel": "oil",
    "fossil liquids: liquefied petroleum gas": "oil",
    "fossil liquids: naphtha": "oil",
    "fossil liquids: gasoline": "gasoline",
    "other: hydrogen (green)": "hydrogen",
    "other: hydrogen (unknown)": "hydrogen",
    "fossil liquids: waste/other oil": "oil",
    "fossil liquids: kerosene": "oil",
    "fossil gas: gaseous propane": "oil",
    "bituminous": "hard coal",
    "lignite": "brown coal",
    "subbituminous": "brown coal",
    "waste coal": "brown coal",
    "anthracite": "hard coal",
    "bioenergy: unknown": "biomass",
    "bioenergy: refuse (municipal and industrial wastes)": "solid waste",
    "bioenergy: paper mill wastes": "biomass",
    "bioenergy: refuse (landfill gas)": "biogas",
    "bioenergy: agricultural waste (biogas)": "biogas",
    "bioenergy: wood & other biomass (solids)": "biomass",
    "bioenergy: agricultural waste (solids)": "biomass",
    "bioenergy: agricultural waste (syngas)": "syngas",
    "bioenergy: agricultural waste (unknown)": "biomass",
    "bioenergy: biodiesel": "diesel",
    "bioenergy: ethanol": "ethanol",
    "bioenergy: refuse (syngas)": "syngas",
    "bioenergy: wastewater and sewage sludge (solids or biogas)": "biomass",
    "bioenergy: wood & other biomass (biocoal)": "biomass",
    "bioenergy: wood & other biomass (syngas)": "syngas",
    "other: tires": "solid waste",
    "hydrogen": "hydrogen",
}
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


def _fuel(cell: str, default: str):
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


def _powerplant_id(gem_df: pd.DataFrame) -> pd.Series:
    """Create a unique identifier using GEM codes."""
    return _utils.get_combined_text_col(
        gem_df, ["GEM location ID", "GEM unit/phase ID"], prefix="GEM_"
    )


def _name(gem_df: pd.DataFrame) -> pd.Series:
    """Create a unique name using GEM data."""
    return _utils.get_combined_text_col(
        gem_df, ["Plant / Project name", "Unit / Phase name"]
    )


def _technology(gem_df: pd.DataFrame, mapping: dict[str, str]) -> pd.Series:
    """Remap technology names, cleaning CCS specifics and inconsistencies."""
    return (
        gem_df["Technology"]
        .fillna("unknown")
        .replace("unknown type", "unknown")
        .apply(lambda x: mapping[x.replace("/CCS", "").strip()])
    )


def _ccs(gem_df: pd.DataFrame) -> pd.Series:
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


def _chp(gem_df: pd.DataFrame) -> pd.Series:
    """Get CHP status of powerplants."""
    return gem_df["CHP"].fillna("").apply(lambda x: True if "yes" in x else False)


def get_powerplant_df(
    gem_df: pd.DataFrame, tech_mapping: dict[str, str]
) -> gpd.GeoDataFrame:
    """Obtain a standardised dataset for non-combustion powerplants."""
    capacity_df = gpd.GeoDataFrame(
        {
            "powerplant_id": _powerplant_id(gem_df),
            "name": _name(gem_df),
            "category": gem_df["Type"],
            "technology": _technology(gem_df, tech_mapping),
            "output_capacity_mw": gem_df["Capacity (MW)"],
            "start_year": gem.gem_year_col(gem_df, "start"),
            "end_year": gem.gem_year_col(gem_df, "end"),
            "status": gem_df["Status"],
            "geometry": _utils.get_point_col(gem_df, "Longitude", "Latitude"),
        }
    )
    return _schemas.PlantSchema.validate(capacity_df)


def get_combustion_plant_df(
    raw_df: pd.DataFrame, tech_mapping: dict[str, str]
) -> gpd.GeoDataFrame:
    """Obtain a standardised dataset for combustion powerplants.

    Includes CCS and CHP attributes.
    """
    capacity_df = get_powerplant_df(raw_df, tech_mapping)
    capacity_df["ccs"] = _ccs(raw_df)
    capacity_df["chp"] = _chp(raw_df)
    return _schemas.CombustionSchema.validate(capacity_df)


def prepare_powerplants(
    gem_raw_path: str,
    output_plants_path: str,
    category: str,
    technology_mapping: dict[str, str],
    output_fuels_path: str | None,
    default_fuel: str | None,
):
    """Process powerplants that burn fuel."""
    gem_df = gem.read_gem_dataset(gem_raw_path, ["Power facilities"])
    # Type/category should match with our schema
    gem_df["Type"] = gem_df["Type"].replace(CATEGORY_MAPPING)
    gem_df = gem_df[gem_df["Type"] == category]
    plants_df = get_powerplant_df(gem_df, technology_mapping)

    if category in COMBUSTION_CATEGORIES:
        if output_fuels_path is None or default_fuel is None:
            raise ValueError(f"Incomplete fuel definition for {category}.")
        plants_df["ccs"] = _ccs(gem_df)
        plants_df["chp"] = _chp(gem_df)
        _schemas.CombustionSchema.validate(plants_df).to_parquet(output_plants_path)

        fuels_df = pd.DataFrame(
            {
                "powerplant_id": _powerplant_id(gem_df),
                "fuel": gem_df["Fuel"].apply(_fuel, default=default_fuel),
            }
        )
        fuels_df = fuels_df.explode("fuel").reset_index(drop=True)
        _schemas.FuelSchema.validate(fuels_df).to_parquet(output_fuels_path)
    else:
        _schemas.PlantSchema.validate(plants_df).to_parquet(output_plants_path)


if __name__ == "__main__":
    prepare_powerplants(
        category=snakemake.wildcards.category,
        gem_raw_path=snakemake.input.gem_raw,
        technology_mapping=snakemake.params.technology_mapping,
        default_fuel=snakemake.params.get("default_fuel", None),
        output_plants_path=snakemake.output.powerplant_capacity,
        output_fuels_path=snakemake.output.get("powerplant_fuels", None),
    )
