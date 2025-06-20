"""Schemas for key files."""

import pandas as pd
import pandera as pa
from pandera import DataFrameModel, Field
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries


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


class PowerplantSchema(DataFrameModel):
    class Config:
        coerce = True
        strict = True

    id: Series[str] = Field(unique=True)
    "Powerplant identifier"
    name: Series[str] = Field(nullable=True)
    "Human readable name"
    technology: Series[str] = Field(nullable=True)
    "Technology specifics"
    capacity_mw: Series[float]
    "Net generation capacity"
    efficiency: Series[float] = Field(le=1)
    "Conversion efficiency"
    install_year: Series[float] = Field(nullable=True)
    "Year of installation"
    retire_year: Series[float] = Field(nullable=True)
    "Year of decomission"
    retrofit_year: Series[float] = Field(nullable=True)
    "Retrofit year"
    mothballed: Series[bool] = Field(nullable=True)
    "Innactive but not fully retired"
    geometry: GeoSeries
    "Point (latitude, longitude)"


class CoalPlantSchema(PowerplantSchema):
    technology: Series[str] = Field(isin=["subcritical", "supercritical", "ultra_supercritical", "cfb", "igcc"], nullable=True)
    "Coal technologies as depicted in https://www.gem.wiki/Coal_power_technologies"
    ccs_installed: Series[bool] = Field(nullable=True)
    "Whether the plant has CCS installed on it"
    fuel: Series[str] = Field(isin=["anthracite", "bituminous", "subbituminous"], nullable=True)
    """Coal fuels as depicted in https://www.eia.gov/energyexplained/coal/.
    `sub_bituminous` includes products like waste coal, lignite, etc."""

