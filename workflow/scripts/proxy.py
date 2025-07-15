"""Functions for proxying missing data."""

import math

import _utils
import click
import geopandas as gpd
import gregor
import pandas as pd
import rioxarray as rxr
from cmap import Colormap
from matplotlib import pyplot as plt

FORCED_CRS = "EPSG:4326"


@click.group()
def cli():
    """CLI for proxy functionality."""
    pass


@cli.command()
@click.argument("borders_file", type=click.Path(dir_okay=False))
@click.argument("proxy_file", type=click.Path(dir_okay=False))
@click.argument("aggregated_unadjusted_file", type=click.Path(dir_okay=False))
@click.argument("stats_file", type=click.Path(dir_okay=False))
@click.option("-o", "--output_file", type=click.Path(dir_okay=False), required=True)
@click.option("-c", "--category", type=str, required=True)
@click.option("-y", "--year", type=int, required=True)
def capacity(
    borders_file: str,
    proxy_file: str,
    aggregated_unadjusted_file: str,
    stats_file: str,
    output_file: str,
    category: str,
    year: int,
):
    """Produce a proxy raster used to disaggregate missing capacity.

    Will only be computed for regions with missing capacity.
    If unadjusted capacity exceeds statistics, the country will be dismissed.

    Args:
        borders_file (str): country borders.
        proxy_file (str): proxy raster file.
        aggregated_unadjusted_file (str): file with unadjusted capacity.
        stats_file (str): country_statistics.
        output_file (str): output file location.
        category (str): category to process.
        year (int): year to use for adjustment.
    """
    borders_df = _utils.open_borders_gdf(borders_file).set_index("country_id")
    stats_df = pd.read_parquet(stats_file)
    area_potential_da = rxr.open_rasterio(proxy_file).squeeze()  # type: ignore[union-attr]
    area_potential_da = area_potential_da.rio.write_crs(FORCED_CRS)  # TODO: remove me

    agg_unadjusted_df = pd.read_parquet(aggregated_unadjusted_file)
    unadj_cap_mw = agg_unadjusted_df.groupby("country_id")["output_capacity_mw"].sum()

    stats_df = stats_df[stats_df["year"] == year]
    stats_df = stats_df[stats_df["category"].isin(_utils.EIA_CAT_MAPPING[category])]
    total_cap_mw = stats_df.set_index("country_id")["capacity_mw"]

    missing_cap_mw = total_cap_mw - unadj_cap_mw
    missing_cap_mw = missing_cap_mw.dropna()
    missing_cap_mw = missing_cap_mw.where(missing_cap_mw >= 0, 0)
    borders_df["missing"] = missing_cap_mw

    proxy = gregor.disaggregate.disaggregate_polygon_to_raster(
        borders_df, column="missing", proxy=area_potential_da
    )
    proxy.rio.to_raster(output_file)


@cli.command()
@click.argument("proxy_file", type=click.Path(dir_okay=False))
@click.argument("borders_file", type=click.Path(dir_okay=False))
@click.option("-o", "--output_file", type=click.Path(dir_okay=False), required=True)
@click.option("-p", "--pixels", type=int, default=500_000)
@click.option("-n", "--name", type=str)
def plot(proxy_file: str, borders_file: str, output_file: str, pixels: int, name: str):
    """Plot a figure of the generated proxy.

    Args:
        proxy_file (str): proxy file generated.
        borders_file (str): country borders used for the proxy.
        output_file (str): output image file location.
        pixels (int): pixel count.
        name (str): name of the proxied attribute.
    """
    borders_df = gpd.read_parquet(borders_file)

    area_potential_da = rxr.open_rasterio(proxy_file).squeeze()  # type: ignore[union-attr]
    # TODO: remove me
    if not area_potential_da.rio.crs:
        # Attempt to force a common CRS in broken files
        area_potential_da = area_potential_da.rio.write_crs(FORCED_CRS)

    # Compute a coarsening factor to avoid memory limits
    nx, ny = area_potential_da.sizes["x"], area_potential_da.sizes["y"]
    factor = math.ceil(math.sqrt((nx * ny) / pixels))

    # Coarsen the proxy data
    coarse = area_potential_da.coarsen(x=factor, y=factor, boundary="trim").mean()
    coarse = coarse.fillna(0)

    fig, ax = plt.subplots(figsize=(6, 6), layout="tight", dpi=300)
    coarse.plot.imshow(
        ax=ax,
        cmap=Colormap("seaborn:rocket").to_matplotlib(),
        add_colorbar=True,
        cbar_kwargs={"location": "bottom", "label": "Area potential"},
        alpha=1,
    )
    borders_df.to_crs(area_potential_da.rio.crs).geometry.boundary.plot(
        ax=ax, color="lightgrey", linewidth=0.3, alpha=0.3
    )
    ax.set_aspect("equal")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Aggregation proxy (coarsened to ~{pixel_count:.1e} pixels)")
    fig.savefig(output_file)


if __name__ == "__main__":
    cli()
