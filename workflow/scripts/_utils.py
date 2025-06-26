# Imports & global constants
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
import richdem as rd
from tqdm.auto import tqdm

GLOBAL_DEM_URL = (
    "https://s3.opengeohub.org/global/edtm/"
    "legendtm_rf_30m_m_s_20000101_20231231_go_epsg.4326_v20250130.tif"
)

# D8 neighbour offsets (RichDEM convention)
_MOVES_R = np.array([0, 0, -1, -1, -1, 0, 1, 1, 1])
_MOVES_C = np.array([0, -1, -1, 0, 1, 1, 1, 0, -1])

# How far to walk upstream/downstream (cells) by technology
N_CELLS = {"reservoir": 67, "pump storage": 67, "run of river": 17}


# Helper: derive integer D8 flow-direction grid from a RichDEM array
def d8_flowdir(dem: rd.rdarray) -> np.ndarray:
    """
    Return an uint8 array with codes 1-8 (RichDEM neighbour numbering),
    0 for pits / NoData.
    """
    rd.FillDepressions(dem, in_place=True)
    rd.ResolveFlats(dem, in_place=True)
    props = rd.FlowProportions(dem, method="D8")  # (rows, cols, 9)
    fdir = np.argmax(props[:, :, 1:], axis=-1) + 1  # best receiver 1-8
    fdir[props[:, :, 0] < 0] = 0  # keep pits/voids at 0
    return fdir.astype(np.uint8)


# Helper: read a DEM window around one plant
dem_src = rasterio.open(GLOBAL_DEM_URL, sharing=False)  # open once, stream


def dem_window(lon: float, lat: float, cells: int = 256):
    """
    Return (dem, transform, fdir) for a square window centred on lon/lat.
    `cells` is the side length (default 256 ≈ 2 km × 2 km).
    """
    row, col = dem_src.index(lon, lat)
    half = cells // 2
    window = rasterio.windows.Window(
        col_off=max(col - half, 0),
        row_off=max(row - half, 0),
        width=cells,
        height=cells,
    )
    arr = dem_src.read(1, window=window, masked=True).astype("float32")
    transform = dem_src.window_transform(window)
    dem = rd.rdarray(arr, no_data=np.nan)
    dem.geotransform = (
        transform.c,  # top-left X
        transform.a,  # pixel width  (positive)
        transform.b,  # rotation (always 0 for north-up)
        transform.f,  # top-left Y
        transform.d,  # rotation (0)
        transform.e,  # pixel height (negative in north-up rasters)
    )

    fdir = d8_flowdir(dem)
    return dem, transform, fdir


# Compute head from one plant’s DEM window
def head_from_dem(lon: float, lat: float, tech: str, cells: int = 256) -> float | None:
    """
    Gross head in metres: walk `n` cells upstream and downstream along
    RichDEM D8 flow paths.  Uses the latest (v2.3+) API.
    """
    dem, transform, fdir = dem_window(lon, lat, cells)

    r0, c0 = rasterio.transform.rowcol(transform, lon, lat)
    if np.isnan(dem[r0, c0]):
        return None

    n = N_CELLS.get(tech, 67)

    # ---------- upstream ------------------------------------------------
    elev_u = []
    r, c = r0, c0
    for _ in range(n):
        code = int(fdir[r, c])
        if code == 0:
            break  # source / pit / NoData
        code = opposite(code)  # go *against* the flow
        r += _MOVES_R[code]
        c += _MOVES_C[code]
        if 0 <= r < dem.shape[0] and 0 <= c < dem.shape[1]:
            e = dem[r, c]
            if not np.isnan(e):
                elev_u.append(e)
        else:
            break

    # ---------- downstream ----------------------------------------------
    elev_d = []
    r, c = r0, c0
    for _ in range(n):
        code = int(fdir[r, c])
        if code == 0:
            break  # sink / flat
        r += _MOVES_R[code]  # follow the flow
        c += _MOVES_C[code]
        if 0 <= r < dem.shape[0] and 0 <= c < dem.shape[1]:
            e = dem[r, c]
            if not np.isnan(e):
                elev_d.append(e)
        else:
            break

    if not elev_u or not elev_d:
        return None
    return max(elev_u) - min(elev_d)


# Main imputation loop – simple & sequential
def impute_head_simple(
    hydro_df: gpd.GeoDataFrame, window_cells: int = 256
) -> gpd.GeoDataFrame:
    out = hydro_df.copy()
    needs_head = out["head_m"].isna()
    for idx, row in tqdm(
        out[needs_head].iterrows(), total=needs_head.sum(), desc="Plants"
    ):
        h = head_from_dem(
            lon=row.geometry.x,
            lat=row.geometry.y,
            tech=row["technology"],
            cells=window_cells,
        )
        if h is not None:
            out.at[idx, "head_m"] = h
            out.at[idx, "head_flag"] = "dem_simple"
    return out


def _extent_from_transform(tr, height, width):
    """
    Return (xmin, xmax, ymin, ymax) from an affine transform.
    Works for north-up rasters (row-major, tr.e is negative).
    """
    xmin = tr.c
    xmax = tr.c + width * tr.a
    ymax = tr.f
    ymin = tr.f + height * tr.e
    return xmin, xmax, ymin, ymax


def opposite(code: int) -> int:
    """
    Return the neighbour code that is 180° from `code`
      1 ↔ 5   (W ↔ E)
      2 ↔ 6   (NW ↔ SE)
      3 ↔ 7   (N ↔ S)
      4 ↔ 8   (NE ↔ SW)
    """
    return ((code + 3) % 8) + 1


def walk_path(lon: float, lat: float, tech: str, cells: int = 256, ax=None):
    """
    Draw the D8 walk and annotate the computed gross head (m).
    Returns (dem, head_m).
    """
    dem, transform, fdir = dem_window(lon, lat, cells)
    r0, c0 = rasterio.transform.rowcol(transform, lon, lat)

    n = N_CELLS.get(tech, 67)
    upr, upc, dnr, dnc = [r0], [c0], [r0], [c0]

    # ---------- upstream ---------------------------------------------
    r, c = r0, c0
    for _ in range(n):
        code = int(fdir[r, c])
        if code == 0:
            break
        code = opposite(code)
        r += _MOVES_R[code]
        c += _MOVES_C[code]
        upr.append(r)
        upc.append(c)

    # ---------- downstream -------------------------------------------
    r, c = r0, c0
    for _ in range(n):
        code = int(fdir[r, c])
        if code == 0:
            break
        r += _MOVES_R[code]
        c += _MOVES_C[code]
        dnr.append(r)
        dnc.append(c)

    # head calculation
    elev_u = [dem[r, c] for r, c in zip(upr, upc) if not np.isnan(dem[r, c])]
    elev_d = [dem[r, c] for r, c in zip(dnr, dnc) if not np.isnan(dem[r, c])]
    head_m = float(max(elev_u) - min(elev_d)) if elev_u and elev_d else np.nan

    # pixel → lon/lat
    u_xy = [
        rasterio.transform.xy(transform, r, c, offset="center")
        for r, c in zip(upr, upc)
    ]
    d_xy = [
        rasterio.transform.xy(transform, r, c, offset="center")
        for r, c in zip(dnr, dnc)
    ]

    if ax is None:
        fig, ax = plt.subplots(figsize=(5, 5))
    extent = _extent_from_transform(transform, *dem.shape)
    im = ax.imshow(dem, cmap="terrain", extent=extent, interpolation="none")

    ax.plot(*zip(*u_xy), "o-", ms=3, lw=1, color="blue", label="upstream")
    ax.plot(*zip(*d_xy), "o-", ms=3, lw=1, color="red", label="downstream")
    ax.scatter(lon, lat, marker="x", color="k", zorder=10, label="plant")

    ax.set_title(f"DEM window - estimated head ≈ {head_m:.1f} m")
    ax.legend(loc="lower right")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal")
    ax.get_figure().colorbar(im, ax=ax, label="Elevation (m)")

    # place a text label near the plant
    ax.text(
        lon,
        lat,
        f"{head_m:.1f} m",
        ha="left",
        va="bottom",
        fontsize=9,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.7),
    )

    return dem, head_m


# Convenience wrapper that pulls coordinates from a dataframe row
def show_head_walk(row, cells: int = 256):
    """
    Convenience wrapper: call with a GeoDataFrame row.
    Example:
        show_head_walk(hydro_df.loc[1234])
    """
    walk_path(
        lon=row.geometry.x,
        lat=row.geometry.y,
        tech=row["technology"],
        cells=cells,
    )
