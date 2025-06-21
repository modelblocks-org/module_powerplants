"""Schemas for key files."""

from pandera.pandas import DataFrameModel, Field
from pandera.typing.geopandas import GeoSeries
from pandera.typing.pandas import Series


class EIASchema(DataFrameModel):
    class Config:
        coerce = True
        strict = True

    year: Series[int]
    "Sample year"
    category: Series[str]
    "Human readable name"
    capacity_mw: Series[float] = Field(nullable=True)
    "Electrical capacity in Megawatt"
    country_id: Series[str]
    "Country ISO-3 code"


class ShapeSchema(DataFrameModel):
    class Config:
        coerce = True
        strict = False

    shape_id: Series[str] = Field(unique=True)
    "Unique ID for this shape."
    country_id: Series[str]
    "ISO alpha-3 code."
    shape_class: Series[str] = Field(isin=["land", "maritime"])
    "Shape classifier"
    geometry: GeoSeries
    "Shape polygon."

PLANT_CATEGORIES = ["nuclear", "coal", "bioenergy", "geothermal", "hydropower", "oil and gas", "solar", "wind", "other"]
PLANT_STATUS = ["announced", "pre-construction", "construction", "operating", "mothballed", "retired"]



class PlantSchema(DataFrameModel):
    class Config:
        coerce = True
        strict = False

    # Identifiers
    powerplant_id: Series[str] = Field(unique=True)
    "Unique ID for the powerplant."
    name: Series[str] = Field(nullable=True, unique=True)
    "Human readable powerplant name."
    # Technology characteristics
    category: Series[str] = Field(isin=PLANT_CATEGORIES)
    "General category of the powerplant."
    technology: Series[str] = Field()
    "Subcategory of the powerplant, if necessary."
    net_output_capacity_mw: Series[float]
    "Powerplant net output capacity in Megawatts."
    # Temporal aspects
    start_year: Series[float] = Field(nullable=True)
    "Installation year."
    end_year: Series[float] = Field(nullable=True)
    "Expected decomissioning year."
    status: Series[str] = Field(isin=PLANT_STATUS)
    "Known state of the project."
    # Location / size
    geometry: GeoSeries
    "Shape polygon."

COMBUSTION_TECHS = ["steam turbine", "gas turbine", "reciprocating engine"]

class CombustionPlantSchema(PlantSchema):
    class Config:
        strict = True

    technology: Series[str] = Field(isin=COMBUSTION_TECHS)
    subtechnology: Series[str]
    fuel: Series[str]
    ccs: Series[bool]
    chp: Series[bool]


COAL_SUBTECHS = ["subcritical", "supercritical", "ultra-supercritical", "CFB", "IGCC"]
COAL_FUELS = ["anthracite", "bituminous", "lignite", "subbituminous", "waste coal"]

class CoalSchema(CombustionPlantSchema):
    category: Series[str] = Field(isin=["coal"])
    technology: Series[str] = Field(isin=["steam turbine", "gas turbine"])
    subtechnology: Series[str] = Field(isin=COAL_SUBTECHS)
    fuel: Series[str] = Field(isin=COAL_FUELS)

OIL_AND_GAS_FUELS = ["LNG", "natural gas", "fuel oil", "diesel", "hydrogen", ]
OIL_AND_GAS_SUBTECHS = ["OCGT", "CCGT", "ICE", "AFC", "subcritical"]

class OilGasSchema(CombustionPlantSchema):
    category: Series[str] = Field(isin=["oil and gas"])
    subtechnology: Series[str] = Field(isin=OIL_AND_GAS_SUBTECHS)
    fuel: Series[str] = Field(isin=[OIL_AND_GAS_FUELS])
    other_fuels: Series[list[str]] = Field(isin=[])

