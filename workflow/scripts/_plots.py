"""Plot functions used in one or more rules."""

import math

import _schemas
import _utils
import geopandas as gpd
import numpy as np
import pandas as pd
from cmap import Colormap
from matplotlib import pyplot as plt
from matplotlib import ticker as mticker
from matplotlib.patches import Patch


def draw_empty(ax, title, message="No data available"):
    """Helper to render an empty-data placeholder."""
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=12, alpha=0.7)
    ax.set_title(title)
    ax.set_axis_off()


def plot_empty(title: str, output_path: str) -> None:
    fig, ax = plt.subplots()
    draw_empty(ax, "")
    fig.suptitle(title, fontsize=14)
    fig.savefig(output_path)


def plot_powerplant_capacity_buildup(df: pd.DataFrame, output_path: str, colormap: str):
    """Plot stacked bar charts of active powerplant capacity over time per country.

    Input should be a powerplant capacity file of a single category.
    """
    suptitle = "Active powerplant capacity by technology per country"

    if df.empty:
        plot_empty(suptitle, output_path)
        return

    # Year range (x-axis)
    start_year = df["start_year"].astype(int).min()
    end_year = df["end_year"].astype(int).max()
    years = list(range(start_year, end_year + 1))

    # Layout (per country in alphabetical order)
    countries = sorted(df["country_id"].unique())
    n_countries = len(countries)
    cols = 2
    rows = math.ceil(n_countries / cols)

    # Tech type color range
    tech_types = sorted(df["technology"].dropna().unique())
    cmap = Colormap(colormap).to_mpl()
    colors = [cmap(i) for i in np.linspace(0, 1, len(tech_types))]

    # Figure (always 2 columns, flexible rows)
    fig, axes = plt.subplots(
        rows,
        cols,
        figsize=(cols * 5, rows * 4),
        sharex=False,
        sharey=False,
        constrained_layout=True,
    )
    axes_flat = np.array(axes).ravel()

    # Plot per country
    for ax, country in zip(axes_flat, countries):
        country_df = df[df["country_id"] == country]
        if country_df.empty:
            draw_empty(ax, country, f"No data for {country}")
            continue

        cap_mw = pd.DataFrame(0.0, index=years, columns=tech_types)
        for year in years:
            active = country_df[
                (country_df["start_year"] <= year) & (year < country_df["end_year"])
            ]
            cap_mw.loc[year] = (
                active.groupby("technology")["output_capacity_mw"]
                .sum()
                .reindex(tech_types, fill_value=0)
            )

        cap_mw.plot(kind="bar", stacked=True, ax=ax, color=colors, legend=False, rot=45)
        ax.set_title(country)
        ax.set_ylabel("Capacity (MW)")
        ax.locator_params(axis="x", nbins=10)
        ax.minorticks_off()

    # Hide extra axes
    for ax in axes_flat[n_countries:]:
        ax.set_visible(False)

    # Add details
    handles, labels = axes_flat[0].get_legend_handles_labels()
    fig.legend(
        handles[::-1],
        labels[::-1],
        loc="center left",
        bbox_to_anchor=(1.0, 0.5),
        title="Technology",
        frameon=False,
    )
    fig.suptitle(suptitle, fontsize=14)

    fig.savefig(output_path, bbox_inches="tight")


def plot_capacity_adjustment(
    stats_file: str,
    unadjusted_file: str,
    adjusted_file: str,
    year: int,
    output_file: str,
    is_disagg: bool,
    *,
    country_col: str = "country_id",
    dis_tech_col: str = "technology",
    dis_cap_col: str = "output_capacity_mw",
    eia_cat_col: str = "category",
    eia_cap_col: str = "capacity_mw",
    bar_width: float = 0.5,
    row_height: float = 3.0,
    fig_width: float = 12.0,
    target_ticks: int = 12,
) -> None:
    """Plot adjustment per country."""
    suptitle = f"Unadjusted vs Adjusted vs EIA Capacity by Country - {year}"
    df_udj = pd.read_parquet(unadjusted_file)
    df_adj = pd.read_parquet(adjusted_file)
    df_eia = pd.read_parquet(stats_file)

    # Handle the no-data case
    if df_udj.empty and df_adj.empty:
        plot_empty(suptitle, output_file)
        return

    if is_disagg:
        df_udj = _utils.filter_years(df_udj, year, how="operating")
        df_adj = _utils.filter_years(df_adj, year, how="operating")

    category_dis = _utils.check_single_category(df_udj)
    category_adj = _utils.check_single_category(df_adj)
    if category_dis != category_adj:
        raise ValueError(
            f"Input datasets are not of the same category: {category_dis} vs {category_adj}"
        )
    df_eia = _utils.get_eia_stats_in_cat_yr(df_eia, year, category_dis)
    df_udj = df_udj[df_udj["country_id"].isin(df_eia["country_id"].unique())]

    # aggregate total capacities.
    agg_dis = (
        df_udj.groupby([country_col, dis_tech_col])[[dis_cap_col]].sum().reset_index()
    )
    agg_adj = (
        df_adj.groupby([country_col, dis_tech_col])[[dis_cap_col]].sum().reset_index()
    )
    agg_eia = (
        df_eia.groupby([country_col, eia_cat_col])[[eia_cap_col]].sum().reset_index()
    )
    countries = sorted(set(agg_dis[country_col]).union(agg_adj[country_col]))
    techs = sorted(set(agg_dis[dis_tech_col]).union(agg_adj[dis_tech_col]))
    cats = sorted(agg_eia[eia_cat_col].unique())

    # Avoid overlaps when techs / cats have the same name
    tech_slices = [("tech", t) for t in techs]
    cat_slices = [("cat", c) for c in cats]
    slices = tech_slices + cat_slices

    cmap = plt.get_cmap("tab20")
    positions = np.linspace(0, 1, len(slices))
    colours = {k: cmap(p) for k, p in zip(slices, positions)}

    tech_handles = [Patch(color=colours[("tech", t)], label=t) for t in techs[::-1]]
    cat_handles = [Patch(color=colours[("cat", c)], label=c) for c in cats[::-1]]

    # build lookup dicts
    dis = agg_dis.set_index([country_col, dis_tech_col])[dis_cap_col].to_dict()
    adj = agg_adj.set_index([country_col, dis_tech_col])[dis_cap_col].to_dict()
    eia = agg_eia.set_index([country_col, eia_cat_col])[eia_cap_col].to_dict()

    # figure layout (per country rows)
    nrows = len(countries)
    fig, axes = plt.subplots(
        nrows,
        3,  # tech-legend | bars | cat-legend
        figsize=(fig_width, row_height * nrows),
        gridspec_kw={"width_ratios": [1.2, 4, 1.2]},
        squeeze=False,
        tight_layout=True,
    )

    xpos = {"Unadjusted": 0, "Adjusted": 1, "EIA": 2}

    # loop per country
    for (ax_tech, ax_bar, ax_cat), country in zip(axes, countries):
        bottoms = {k: 0.0 for k in xpos}

        for grp, label in slices:
            colour = colours[(grp, label)]

            if grp == "tech":  # from df_dis / df_adj
                dis_val = dis.get((country, label), 0.0)
                adj_val = adj.get((country, label), 0.0)
                eia_val = 0.0
            else:  # grp == 'cat' (EIA dataframe)
                dis_val = 0.0
                adj_val = 0.0
                eia_val = eia.get((country, label), 0.0)

            ax_bar.bar(
                xpos["Unadjusted"],
                dis_val,
                bar_width,
                bottom=bottoms["Unadjusted"],
                color=colour,
            )
            ax_bar.bar(
                xpos["Adjusted"],
                adj_val,
                bar_width,
                bottom=bottoms["Adjusted"],
                color=colour,
            )
            ax_bar.bar(
                xpos["EIA"], eia_val, bar_width, bottom=bottoms["EIA"], color=colour
            )

            bottoms["Unadjusted"] += dis_val
            bottoms["Adjusted"] += adj_val
            bottoms["EIA"] += eia_val

        tot_dis, tot_adj, tot_eia = (
            bottoms["Unadjusted"],
            bottoms["Adjusted"],
            bottoms["EIA"],
        )

        # cosmetics
        ax_bar.axhline(tot_dis, ls=":", lw=0.8, color="grey")
        ax_bar.axhline(tot_adj, ls="--", lw=0.8, color="grey")

        ax_bar.set_xticks([xpos[k] for k in ("Unadjusted", "Adjusted", "EIA")])
        ax_bar.set_xticklabels(["Unadjusted", "Adjusted", "EIA statistics"])
        ax_bar.set_title(country, pad=4)
        ax_bar.set_ylabel("Capacity (MW)")
        ax_bar.yaxis.set_major_locator(mticker.MaxNLocator(nbins=target_ticks))
        ax_bar.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))

        # percentage axis
        if tot_adj:
            sec = ax_bar.secondary_yaxis(
                "right",
                functions=(
                    lambda mw, tot=tot_adj: mw / tot * 100,
                    lambda pct, tot=tot_adj: pct * tot / 100,
                ),
            )
            pct_max = max(100, tot_dis / tot_adj * 100, tot_eia / tot_adj * 100)
            sec.set_ylabel(r"% of adjusted total")
            sec.set_yticks(np.linspace(0, pct_max, target_ticks))
            sec.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:.0f}"))
            sec.set_ylim(0, pct_max)

        # legends
        for ax, handles, title in [
            (ax_tech, tech_handles, "Technology"),
            (ax_cat, cat_handles, "EIA category"),
        ]:
            ax.axis("off")
            ax.legend(
                handles,
                [h.get_label() for h in handles],
                ncol=1,
                frameon=False,
                title=title,
                loc="center",
            )
    fig.suptitle(suptitle, y=0.999)

    fig.savefig(output_file, bbox_inches="tight")


def plot_capacity_aggregation(
    aggregated_file: str, shapes_file: str, output_file: str, category: str
):
    """Plot aggregated capacity per region."""
    shapes = _schemas.ShapeSchema.validate(gpd.read_parquet(shapes_file))
    agg = _schemas.AggregatedPlantSchema.validate(pd.read_parquet(aggregated_file))

    title = f"Aggregated {category} capacity"

    if agg.empty:
        plot_empty(title, output_file)
    else:
        cap_by_shape = agg.groupby("shape_id")["output_capacity_mw"].sum()

        shapes = shapes.set_index("shape_id")
        shapes["output_capacity_mw"] = cap_by_shape.replace(0, np.nan)

        fig, ax = plt.subplots(figsize=(6, 6), dpi=300, rasterized=True)

        ax = shapes.plot(
            ax=ax,
            column="output_capacity_mw",
            cmap="magma",
            edgecolor="grey",
            linewidth=0.5,
            legend=True,
            legend_kwds={"label": "Capacity ($MW$)"},
            missing_kwds={"color": "lightgrey", "alpha": 0.2},
        )
        ax.set_title(title + f" in year {agg.attrs['year']}")
        ax.set_xlabel("Longitude ($deg$)")
        ax.set_ylabel("Latitude ($deg$)")
        fig.savefig(output_file, bbox_inches="tight")
