"""Rules for the solar special case.

Rooftop PV is generally incomplete in national statistics.
This special case is kept separate for clarity.

To fill solar capacity, we follow these steps:

1. Obtain powerplant capacity of large projects (utility PV, CSP).
2. Assume rooftop PV = national solar statistics - large projects.
3. Use a proxy to disaggregate then aggregate assumed rooftop PV capacity per shape.
4. Combine aggregated large pv projects and rooftop PV capacity.
"""


rule proxy_rooftop_pv:
    input:
        shapes="<shapes>",
        proxy="<proxy_rooftop_pv>",
        agg_unadj=workflow.pathvars.apply("<aggregated_capacity>").format(
            shapes="{shapes}",
            adjustment="unadjusted",
            category="large_solar",
        ),
        stats=rules.prepare_statistics.output.categories,
    output:
        proxy="<resources>/automatic/shapes/{shapes}/proxies/rooftop_pv.tif",
        plot=report(
            "<resources>/automatic/shapes/{shapes}/proxies/rooftop_pv.pdf",
            caption="../report/proxy_rooftop_pv.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
    log:
        "<logs>/{shapes}/proxy_rooftop_pv.log",
    conda:
        "../envs/powerplants.yaml"
    params:
        category="solar",
        year=config["imputation"]["adjustment_year"],
    message:
        "Generating proxy for rooftop capacity {wildcards.shapes}."
    script:
        "../scripts/proxy.py"


rule impute_adjustment_solar:
    input:
        large_solar=workflow.pathvars.apply("<aggregated_capacity>").format(
            shapes="{shapes}",
            adjustment="unadjusted",
            category="large_solar",
        ),
        proxy=rules.proxy_rooftop_pv.output.proxy,
        shapes="<shapes>",
        stats=rules.prepare_statistics.output.categories,
    output:
        aggregated=workflow.pathvars.apply("<aggregated_capacity>").format(
            shapes="{shapes}",
            adjustment="adjusted",
            category="solar",
        ),
        plot_map=report(
            "<results>/{shapes}/aggregated/adjusted/solar_map.pdf",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
        plot_stats=report(
            "<results>/{shapes}/aggregated/adjusted/solar_stats.pdf",
            caption="../report/impute_capacity_adjustment.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
    log:
        "<logs>/{shapes}/adjusted/solar/aggregate_solar_capacity.log",
    conda:
        "../envs/powerplants.yaml"
    params:
        category="solar",
        technology=config["category"]["solar"]["technology_mapping"]["rooftop_pv"],
        year=config["imputation"]["adjustment_year"],
    message:
        "Aggregating capacity for {wildcards.shapes}-adjusted-solar."
    script:
        "../scripts/impute_adjustment_solar.py"

