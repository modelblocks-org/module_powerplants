"""Prepare intermediate versions of the shape file used during processing."""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure

if TYPE_CHECKING:
    snakemake: Any


def dissolve_by_country(shapes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Dissolve shapes by their country."""
    countries_gdf = (
        shapes[["country_id", "geometry"]].dissolve("country_id").reset_index()
    )
    countries_gdf["geometry"] = countries_gdf.buffer(0)
    return countries_gdf


def plot(
    original: gpd.GeoDataFrame,
    new: gpd.GeoDataFrame,
    *,
    name: str,
    crs: str,
    cmap: str,
    col: str,
) -> tuple[Figure, Axes]:
    """Helper to view computed changes."""
    fig, axes = plt.subplots(1, 2, layout="constrained")
    original.to_crs(crs).plot("shape_class", ax=axes[0])
    axes[0].set_title("User")
    if new[col].is_unique:
        # FIXME: odd issue when plotting a single category
        # Upgrading geopandas might fix it.
        new.to_crs(crs).plot(ax=axes[1], color="lightcoral")
    else:
        new.to_crs(crs).plot(col, ax=axes[1], cmap=cmap, categorical=True)
    axes[1].set_title(name)
    for ax in axes:
        ax.set(xticks=[], yticks=[], xlabel="", ylabel="")
    return fig, axes


def main():
    """Prepare a cleaned hydropower dataset."""
    shapes = _schemas.ShapeSchema.validate(gpd.read_parquet(snakemake.input.shapes))
    dissolved = dissolve_by_country(shapes)
    dissolved.to_parquet(snakemake.output.dissolved)

    fig, _ = plot(
        shapes,
        dissolved,
        col="country_id",
        name="Dissolved",
        crs=snakemake.params.crs,
        cmap="glasbey:glasbey",
    )
    fig.savefig(snakemake.output.dissolved_plt, dpi=200, bbox_inches="tight")


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
