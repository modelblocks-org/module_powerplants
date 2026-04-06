"""Rules for the solar special case.

Rooftop PV is generally incomplete in national statistics.
This special case is kept separate for clarity.

To fill solar capacity, we follow these steps:

1. Obtain powerplant capacity of large projects (utility PV, CSP).
2. Assume rooftop PV = national solar statistics - large projects.
3. Use a proxy to disaggregate then aggregate assumed rooftop PV capacity per shape.
4. Combine aggregated large pv projects and rooftop PV capacity.
5. Adjust to national statistics.
"""


rule proxy_rooftop_pv:
    message:
        "Generating proxy for rooftop capacity {wildcards.shapes}."
    params:
        category="solar",
        year=config["imputation"]["adjustment_year"],
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
    script:
        "../scripts/proxy.py"


rule aggregate_solar_capacity:
    message:
        "Aggregating capacity for {wildcards.shapes}-unadjusted-solar."
    params:
        technology=config["category"]["solar"]["technology_mapping"]["rooftop_pv"],
        category="solar"
    input:
        large_solar=workflow.pathvars.apply("<aggregated_capacity>").format(
            shapes="{shapes}",
            adjustment="unadjusted",
            category="large_solar",
        ),
        proxy=rules.proxy_rooftop_pv.output.proxy,
        shapes="<shapes>",
    output:
        aggregated=workflow.pathvars.apply("<aggregated_capacity>").format(
            shapes="{shapes}",
            adjustment="unadjusted",
            category="solar",
        ),
        plot=report(
            "<results>/{shapes}/aggregated/unadjusted/solar.pdf",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
    log:
        "<logs>/{shapes}/unadjusted/solar/aggregate_solar_capacity.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/aggregate_capacity.py"


rule impute_capacity_adjustment_solar:
    message:
        "Adjusting aggregated capacity of {wildcards.shapes}-solar to {params.year} statistics."
    params:
        year=config["imputation"]["adjustment_year"],
    input:
        unadjusted=workflow.pathvars.apply("<aggregated_capacity>").format(
            shapes="{shapes}",
            adjustment="unadjusted",
            category="solar",
        ),
        shapes="<shapes>",
        stats=rules.prepare_statistics.output.categories,
    output:
        adjusted=workflow.pathvars.apply("<aggregated_capacity>").format(
            shapes="{shapes}",
            adjustment="adjusted",
            category="solar",
        ),
        adj_plot=report(
            "<results>/{shapes}/aggregated/adjusted/solar_adjustment.pdf",
            caption="../report/impute_capacity_adjustment.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
        map_plot=report(
            "<results>/{shapes}/aggregated/adjusted/solar_map.pdf",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
    log:
        "<logs>/{shapes}/adjusted/solar/impute_capacity_adjustment_solar.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/impute_capacity_adjustment_solar.py"
