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
def head_from_dem(lon: float, lat: float, tech: str,
                  cells: int = 256) -> float | None:
    """
    Return the gross head (m) by marching `n` cells upstream and downstream
    along D8 flow paths derived with RichDEM.  Uses the latest (v2.3+) API.
    """
    dem, transform, fdir = dem_window(lon, lat, cells)
    r_u = r_d, c_u = c_d = rasterio.transform.rowcol(transform, lon, lat)
    if np.isnan(dem[r_u, c_u]):
        return None

    n = N_CELLS.get(tech, 67)
    elev_u, elev_d = [], []

    for _ in range(n):
        # -------- upstream leg (against the flow) --------------------
        code_u = int(fdir[r_u, c_u])
        if code_u == 0:
            break                                # source / pit / nodata
        r_u += _MOVES_R[opposite(code_u)]
        c_u += _MOVES_C[opposite(code_u)]
        if 0 <= r_u < dem.shape[0] and 0 <= c_u < dem.shape[1]:
            eu = dem[r_u, c_u]
            if not np.isnan(eu):
                elev_u.append(eu)

        # -------- downstream leg (with the flow) ---------------------
        code_d = int(fdir[r_d, c_d])
        if code_d == 0:
            break                                # reached sink / flat
        r_d += _MOVES_R[code_d]
        c_d += _MOVES_C[code_d]
        if 0 <= r_d < dem.shape[0] and 0 <= c_d < dem.shape[1]:
            ed = dem[r_d, c_d]
            if not np.isnan(ed):
                elev_d.append(ed)

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
    Returns (dem_array, upstream_xy, downstream_xy) and draws the walk.
    """
    dem, transform, fdir = dem_window(lon, lat, cells)
    r0, c0 = rasterio.transform.rowcol(transform, lon, lat)
    if np.isnan(dem[r0, c0]):
        raise ValueError("Centre pixel is NoData.")

    n = N_CELLS.get(tech, 67)
    ur = [r0]
    uc = [c0]
    dr = [r0]
    dc = [c0]
    r_u = r_d = r0
    c_u = c_d = c0

    for _ in range(n):
        code = int(fdir[r_d, c_d])
        if code == 0:
            break
        # upstream
        r_u += _MOVES_R[code]
        c_u += _MOVES_C[code]
        if 0 <= r_u < dem.shape[0] and 0 <= c_u < dem.shape[1]:
            ur.append(r_u)
            uc.append(c_u)
        # downstream (use fixed opposite)
        code_d = opposite(code)
        r_d += _MOVES_R[code_d]
        c_d += _MOVES_C[code_d]
        if 0 <= r_d < dem.shape[0] and 0 <= c_d < dem.shape[1]:
            dr.append(r_d)
            dc.append(c_d)

    # pixel → lon/lat
    u_xy = [
        rasterio.transform.xy(transform, r, c, offset="center") for r, c in zip(ur, uc)
    ]
    d_xy = [
        rasterio.transform.xy(transform, r, c, offset="center") for r, c in zip(dr, dc)
    ]

    if ax is None:
        fig, ax = plt.subplots(figsize=(5, 5))

    # compute extent manually
    extent = _extent_from_transform(transform, dem.shape[0], dem.shape[1])

    imshow = ax.imshow(dem, cmap="terrain", extent=extent, interpolation="none")
    # upstream (blue) & downstream (red) paths
    ax.plot(*zip(*u_xy), marker="o", ms=3, lw=1, color="blue", label="upstream")
    ax.plot(*zip(*d_xy), marker="o", ms=3, lw=1, color="red", label="downstream")
    ax.scatter(lon, lat, marker="x", color="k", zorder=10, label="plant")
    ax.legend(loc="lower right")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal")
    fig = ax.get_figure()
    fig.colorbar(imshow, ax=ax, label="Elevation (m)")

    return dem, u_xy, d_xy


# Convenience wrapper that pulls coordinates from a dataframe row
def show_head_walk(row, cells: int = 256):
    """
    Example:
        show_head_walk(hydro_df.loc[12345])
    """
    walk_path(
        lon=row.geometry.x, lat=row.geometry.y, tech=row["technology"], cells=cells
    )
