"""Schemas for key files."""

from pandera import Check, Column, DataFrameSchema

eia_capacity_schema = DataFrameSchema(
    {
        "year": Column(int, description="Year"),
        "category": Column(str, description="Human readable name"),
        "capacity_mw": Column(float, description="Electrical capacity in Megawatt", nullable=True),
        "country_id": Column(str, description="Country ISO-3 code"),
    },
    coerce=True,
    strict=True,
)

shape_schema = DataFrameSchema(
    {
        "shape_id": Column(
            str, unique=True, description="A unique identifier for this shape"
        ),
        "country_id": Column(str, description="Country ISO-3 code"),
        "shape_class": Column(
            str,
            checks=Check.isin(["land", "maritime"]),
            description="Shape classifier",
        ),
        "geometry": Column("geometry", description="Shape (multi)polygon"),
    },
    coerce=True,
    strict=False,
)
