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


COMBUSTION_PLANT_CATEGORIES = ["coal", "bioenergy", "oil_gas"]

PLANT_CATEGORIES = [
    "nuclear",
    "geothermal",
    "hydropower",
    "solar",
    "wind",
] + COMBUSTION_PLANT_CATEGORIES
PLANT_STATUS = [
    "announced",
    "pre-construction",
    "construction",
    "operating",
    "mothballed",
    "retired",
]


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
    technology: Series[str]
    "Subcategory of the powerplant, if necessary."
    output_capacity_mw: Series[float]
    "Powerplant gross output capacity in Megawatts."
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


class CombustionSchema(PlantSchema):
    class Config:
        strict = True

    category: Series[str] = Field(isin=COMBUSTION_PLANT_CATEGORIES)
    ccs: Series[bool]
    chp: Series[bool]


class FuelSchema(DataFrameModel):
    class Config:
        strict = True
        coerce = True

    powerplant_id: Series[str] = Field(unique=False)
    "Unique ID for the powerplant."
    fuel: Series[str]
    "Fuel consumed."
