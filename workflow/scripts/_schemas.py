"""Schemas for key files."""

from collections.abc import Mapping
from typing import Literal

import pandera.pandas as pa
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

    @check("geometry", element_wise=True)
    def geom_not_empty(cls, geom):
        return (geom is not None) and (not geom.is_empty) and geom.is_valid


class PlantSchema(DataFrameModel):
    class Config:
        coerce = True
        strict = True

    index: Index[int] = Field(unique=True)

    # Identifiers
    powerplant_id: Series[str] = Field(unique=True)
    "Unique ID for the powerplant."
    name: Series[str]
    "Human readable powerplant name."
    # Technology characteristics
    category: Series[str]
    "General category of the powerplant."
    technology: Series[str]
    "Subcategory of the powerplant, if necessary."
    output_capacity_mw: Series[float] = Field(ge=0)
    "Powerplant gross output capacity in Megawatts."
    # Temporal aspects
    start_year: Series[float]
    "Installation year."
    end_year: Series[float]
    "Expected decomissioning year."
    status: Series[str]
    "Known state of the project."
    # Location / size
    geometry: GeoSeries[Point] = Field()
    "Powerplant point data."

    @check("geometry", element_wise=True)
    def geom_not_empty(cls, geom):
        return (geom is not None) and (not geom.is_empty) and geom.is_valid


class CombustionSchema(PlantSchema):
    category: Series[str] = Field(isin=["biofuel", "fossil"])
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


Category = Literal[
    "nuclear", "geothermal", "hydropower", "solar", "wind", "bioenergy", "fossil"
]
Stage = Literal["prepare", "impute"]

PLANT_CATEGORIES: Mapping[Category, type[PlantSchema]] = {
    "nuclear": PlantSchema,
    "geothermal": PlantSchema,
    "hydropower": HydroSchema,
    "solar": PlantSchema,
    "wind": PlantSchema,
    "bioenergy": CombustionSchema,
    "fossil": CombustionSchema,
}


# A diverse set of statuses to diminish oversimplification during gap filling.
PREPARED_STATUS = {
    "announced",
    "pre-construction",
    "construction",
    "operating",
    "retired",
}
# Status categorisation shown to users (and accepted in user imputed files).
IMPUTED_STATUS = {"planned", "operating", "retired"}


def build_schema(
    category: Category,
    tech_mapping: dict[str, str],
    stage: Literal["prepare", "impute"],
) -> pa.DataFrameSchema:
    """Construct an inflexible schema applicable to each processing stage."""
    schema = PLANT_CATEGORIES[category].to_schema()
    if stage == "prepare":
        status_set = PREPARED_STATUS
        # Years can be empty during preparation stages
        year_overrides: list[tuple[str, dict]] = [
            ("start_year", {"nullable": True}),
            ("end_year", {"nullable": True}),
        ]
    elif stage == "impute":
        status_set = IMPUTED_STATUS
        year_overrides = []
        schema = schema.add_columns(
            {"country_id": pa.Column(str, checks=pa.Check.str_length(3, 3))}
        )
    else:
        raise ValueError(f"Incorrect stage given: '{stage}'.")

    techs = set(tech_mapping.values())
    overrides = [
        *year_overrides,
        ("category", {"checks": pa.Check.equal_to(category)}),
        ("technology", {"checks": pa.Check.isin(techs)}),
        ("status", {"checks": pa.Check.isin(status_set)}),
    ]
    for col, kwargs in overrides:
        schema = schema.update_column(col, **kwargs)

    return schema
