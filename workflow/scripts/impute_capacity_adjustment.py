"""Adjustment of disaggregated powerplant capacity to national statistics."""

import _plots
import _schemas
import _utils
import click
import geopandas as gpd
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import ticker as mticker
from matplotlib.patches import Patch

EIA_CAT_MAPPING = {
    "bioenergy": "biomass and waste",
    "fossil": "fossil fuels",
    "geothermal": "geothermal",
    "hydropower": ["hydropower", "pumped storage"],
    "nuclear": "nuclear",
    "solar": "solar",
    "wind": "wind",
}


def _get_stats_in_cat_yr(stats_file: str, year: int, category: str) -> pd.DataFrame:
    """Get EIA statistics for a given year and category."""
    stats = pd.read_parquet(stats_file)
    stats = stats[stats["year"] == year]
    stats = stats[stats["category"].isin(_utils.listify(EIA_CAT_MAPPING[category]))]
    return stats


@click.group()
def cli():
    """CLI for capacity adjustment imputations."""
    pass


@cli.command()
@click.argument("disaggregated_file", type=click.Path(dir_okay=False))
@click.argument("stats_file", type=click.Path(dir_okay=False))
@click.option("-y", "--year", type=int, required=True)
@click.option("-o", "--output_file", type=click.Path(dir_okay=False), required=True)
def adjust(disaggregated_file: str, stats_file: str, year: int, output_file: str):
    """Adjust powerplant capacity so it fits national statistics for the given year.

    Powerplant location/distribution will be kept equal.
    """
    plants = gpd.read_parquet(disaggregated_file)
    if not plants.empty:
        category = _utils.check_single_category(plants)
        stats = _get_stats_in_cat_yr(stats_file, year, category)

        # Adjustsment of operating facilities
        operating_plants = _utils.filter_years(plants, year, how="operating")
        expected_tot_cap = stats.groupby(["country_id"])["capacity_mw"].sum()
        adjusted_cap_mw = (
            operating_plants["output_capacity_mw"]
            / operating_plants.groupby("country_id")["output_capacity_mw"].transform(
                "sum"
            )
        ) * operating_plants["country_id"].map(expected_tot_cap)

        # Update plants with adjusted capacity without touching planned projects
        future_plants = _utils.filter_years(plants, year, how="future")
        future_plants.loc[adjusted_cap_mw.index, "output_capacity_mw"] = adjusted_cap_mw
        future_plants = future_plants.reset_index(drop=True)
    else:
        future_plants = plants

    _schemas.PlantSchema.validate(future_plants).to_parquet(output_file)


@cli.command()
@click.argument("disaggregated_file", type=click.Path(dir_okay=False))
@click.argument("stats_file", type=click.Path(dir_okay=False))
@click.argument("adjusted_file", type=click.Path(dir_okay=False))
@click.option("-y", "--year", type=int, required=True)
@click.option("-o", "--output_file", type=click.Path(dir_okay=False), required=True)
def plot(
    disaggregated_file: str,
    stats_file: str,
    adjusted_file: str,
    year: int,
    output_file: str,
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
    title = f"Disaggregated vs Adjusted vs EIA Capacity by Country - {year}"
    df_dis = pd.read_parquet(disaggregated_file)
    df_adj = pd.read_parquet(adjusted_file)

    # Handle the no-data case
    if df_dis.empty and df_adj.empty:
        _plots.plot_empty(title, output_file)
        return

    df_dis = _utils.filter_years(df_dis, year, how="operating")
    df_adj = _utils.filter_years(df_adj, year, how="operating")

    category_dis = _utils.check_single_category(df_dis)
    category_adj = _utils.check_single_category(df_adj)
    if category_dis != category_adj:
        raise ValueError(
            f"Input datasets are not of the same category: {category_dis} vs {category_adj}"
        )

    df_eia = _get_stats_in_cat_yr(stats_file, year, category_dis)

    # aggregate total capacities.
    agg_dis = (
        df_dis.groupby([country_col, dis_tech_col])[[dis_cap_col]].sum().reset_index()
    )
    agg_adj = (
        df_adj.groupby([country_col, dis_tech_col])[[dis_cap_col]].sum().reset_index()
    )
    agg_eia = (
        df_eia.groupby([country_col, eia_cat_col])[[eia_cap_col]].sum().reset_index()
    )

    countries = sorted(agg_dis[country_col].unique())
    techs = sorted(agg_dis[dis_tech_col].unique())
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

    xpos = {"Disaggregated": 0, "Adjusted": 1, "EIA": 2}

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
                xpos["Disaggregated"],
                dis_val,
                bar_width,
                bottom=bottoms["Disaggregated"],
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

            bottoms["Disaggregated"] += dis_val
            bottoms["Adjusted"] += adj_val
            bottoms["EIA"] += eia_val

        tot_dis, tot_adj, tot_eia = (
            bottoms["Disaggregated"],
            bottoms["Adjusted"],
            bottoms["EIA"],
        )

        # cosmetics
        ax_bar.axhline(tot_dis, ls=":", lw=0.8, color="grey")
        ax_bar.axhline(tot_adj, ls="--", lw=0.8, color="grey")

        ax_bar.set_xticks([xpos[k] for k in ("Disaggregated", "Adjusted", "EIA")])
        ax_bar.set_xticklabels(["Disaggregated", "Adjusted", "EIA statistics"])
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
    fig.suptitle(title, y=0.995)

    fig.savefig(output_file)


if __name__ == "__main__":
    cli()
