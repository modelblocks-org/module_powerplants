"""Rules for the solar special case.

Rooftop PV is generally incomplete in national statistics.
This special case is kept separate for clarity.

To fill solar capacity, we follow these steps:

1. Obtain disaggregated capacity of large projects (utility PV, CSP).
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
        year=config["imputation"]["adjustment_yr"],
    input:
        borders="resources/user/borders/{shapes}.parquet",
        proxy="resources/user/proxies/rooftop_pv/{shapes}.tif",
        agg_unadj="results/{shapes}/aggregated/unadjusted/large_solar.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        proxy="results/{shapes}/aggregated/proxies/rooftop_pv.tif",
        plot=report(
            "results/{shapes}/aggregated/proxies/rooftop_pv.pdf",
            caption="../report/proxy_rooftop_pv.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
    log:
        "logs/proxy_rooftop_pv_{shapes}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/proxy.py"


rule aggregate_solar_capacity:
    message:
        "Aggregating capacity for {wildcards.shapes}-unadjusted-{wildcards.category}."
    params:
        technology=config["category"]["solar"]["technology_mapping"]["rooftop_pv"],
    input:
        large_solar="results/{shapes}/aggregated/unadjusted/large_solar.parquet",
        proxy="results/{shapes}/aggregated/proxies/rooftop_pv.tif",
        shapes="resources/user/shapes/{shapes}.parquet",
    output:
        aggregated="results/{shapes}/aggregated/unadjusted/{category}.parquet",
        plot=report(
            "results/{shapes}/aggregated/unadjusted/{category}.pdf",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    wildcard_constraints:
        category="solar",
    log:
        "logs/aggregate_capacity_{shapes}_unadjusted_{category}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/aggregate_capacity.py"


rule impute_capacity_adjustment_solar:
    message:
        "Adjusting aggregated capacity of {wildcards.shapes}-solar to {params.year} statistics."
    params:
        year=config["imputation"]["adjustment_yr"],
    input:
        unadjusted="results/{shapes}/aggregated/unadjusted/solar.parquet",
        shapes="resources/user/shapes/{shapes}.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        adjusted="results/{shapes}/aggregated/adjusted/solar.parquet",
        adj_plot=report(
            "results/{shapes}/aggregated/adjusted/solar_adj.pdf",
            caption="../report/impute_capacity_adjustment.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
        map_plot=report(
            "results/{shapes}/aggregated/adjusted/solar_map.pdf",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
    log:
        "logs/impute_capacity_adjusted_solar_{shapes}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/impute_capacity_adjustment_solar.py"
