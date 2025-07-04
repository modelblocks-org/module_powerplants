"""Schemas for key files."""

from pandera.pandas import DataFrameModel, Field, check
from pandera.typing.geopandas import GeoSeries
from pandera.typing.pandas import Index, Series
from shapely.geometry import Point


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


COMBUSTION_CATEGORIES = ["coal", "bioenergy", "oil_gas"]
PLANT_CATEGORIES = [
    "nuclear",
    "geothermal",
    "hydropower",
    "solar",
    "wind",
] + COMBUSTION_CATEGORIES

PLANT_STATUS = [
    "planned",
    "operating",
    "mothballed",
    "retired",
]


class PlantSchema(DataFrameModel):
    class Config:
        coerce = True
        strict = False

    index: Index[int] = Field(unique=True)

    # Identifiers
    powerplant_id: Series[str] = Field(unique=True)
    "Unique ID for the powerplant."
    name: Series[str] = Field(nullable=True)
    "Human readable powerplant name."
    # Technology characteristics
    category: Series[str] = Field(isin=PLANT_CATEGORIES)
    "General category of the powerplant."
    technology: Series[str]
    "Subcategory of the powerplant, if necessary."
    output_capacity_mw: Series[float] = Field(ge=0)
    "Powerplant gross output capacity in Megawatts."
    # Temporal aspects
    start_year: Series[float] = Field(nullable=True)
    "Installation year."
    end_year: Series[float] = Field(nullable=True)
    "Expected decomissioning year."
    status: Series[str] = Field(isin=PLANT_STATUS)
    "Known state of the project."
    # Location / size
    geometry: GeoSeries[Point] = Field()
    "Powerplant point data."
    @check("geometry", element_wise=True)
    def geom_not_empty(cls, geom):
        return (geom is not None) and (not geom.is_empty) and geom.is_valid


class CombustionSchema(PlantSchema):
    category: Series[str] = Field(isin=COMBUSTION_CATEGORIES)
    ccs: Series[bool]
    """Identifier for known CCS-enabled powerplants."""
    chp: Series[bool]
    """Identifier for known CHP-enabled powerplants."""


class FuelSchema(DataFrameModel):
    class Config:
        strict = True
        coerce = True

    powerplant_id: Series[str]
    "Unique ID for the powerplant."
    fuel: Series[str]
    "Fuel consumed."


class HydroSchema(PlantSchema):
    reservoir_km3: Series[float] = Field(nullable=True, ge=0)
