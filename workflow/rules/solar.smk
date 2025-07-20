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
        script=workflow.source_path("../scripts/proxy.py"),
        borders="resources/user/borders/{shapes}.parquet",
        proxy="resources/user/proxies/rooftop_pv/{shapes}.tif",
        agg_unadj="results/{shapes}/aggregated/unadjusted/large_solar.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        proxy="results/{shapes}/aggregated/proxies/rooftop_pv.tif",
        plot=report(
            "results/{shapes}/aggregated/proxies/rooftop_pv.png",
            caption="../report/proxy_rooftop_pv.rst",
            category="Powerplants module",
            subcategory="solar",
        ),
    log:
        "logs/proxy_rooftop_pv_{shapes}.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        """
        python {input.script:q} capacity {input.borders:q} {input.proxy:q} {input.agg_unadj:q} {input.stats:q} \
            -o {output.proxy:q} -y {params.year} -c "{params.category}" 2> {log:q}
        python {input.script:q} plot {output.proxy:q} {input.borders:q} \
            -o {output.plot:q} 2> {log:q}
        """


rule aggregate_solar_capacity:
    message:
        "Aggregating capacity for {wildcards.shapes}-{wildcards.adjustment}-{wildcards.adjustment}."
    params:
        category="solar",
        technology=config["category"]["solar"]["technology_mapping"]["rooftop_pv"],
    input:
        script=workflow.source_path("../scripts/aggregate.py"),
        large_solar="results/{shapes}/aggregated/{adjustment}/large_solar.parquet",
        proxy="results/{shapes}/aggregated/proxies/rooftop_pv.tif",
        shapes="resources/user/shapes/{shapes}.parquet",
    output:
        aggregated="results/{shapes}/aggregated/{adjustment}/{category}.parquet",
        plot=report(
            "results/{shapes}/aggregated/{adjustment}/{category}.png",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="{category}"
        )
    wildcard_constraints:
        adjustment="unadjusted",
        category="solar",
    log:
        "logs/aggregate_capacity_{shapes}_{adjustment}_{category}.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        """
        python {input.script:q} capacity-solar {input.large_solar:q} {input.proxy:q} {input.shapes:q} \
            -o {output.aggregated:q} -c "{params.category}" -t "{params.technology}" 2> {log:q}
        python {input.script:q} plot {output.aggregated:q} {input.shapes:q} \
            -c "{params.category}" -o {output.plot:q} 2> {log:q}
        """


rule impute_aggregated_capacity_adjustment_solar:
    message:
        "Adjusting aggregated capacity of {wildcards.shapes}-solar to {params.year} statistics."
    params:
        year=config["imputation"]["adjustment_yr"],
    input:
        script_impute=workflow.source_path("../scripts/impute_capacity_adjustment.py"),
        script_aggregate=workflow.source_path("../scripts/aggregate.py"),
        unadjusted="results/{shapes}/aggregated/unadjusted/{category}.parquet",
        shapes="resources/user/shapes/{shapes}.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        adjusted="results/{shapes}/aggregated/{adjustment}/{category}.parquet",
        pdf=report(
            "results/{shapes}/aggregated/{adjustment}/{category}.pdf",
            caption="../report/impute_disaggregated_capacity_adjustment.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
        png=report(
            "results/{shapes}/aggregated/{adjustment}/{category}.png",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="{category}"
        ),
    wildcard_constraints:
        adjustment="adjusted",
        category="solar",
    log:
        "logs/impute_capacity_{adjustment}_{category}_{shapes}.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        """
        python {input.script_impute:q} adjust-aggregated {input.stats:q} {input.unadjusted:q} \
            -y {params.year} -o {output.adjusted:q} 2> {log:q}
        python {input.script_impute:q} plot {input.stats:q} {input.unadjusted:q} {output.adjusted:q} \
            -y {params.year} -o {output.pdf:q} --aggregated 2> {log:q}
        python {input.script_aggregate:q} plot {output.adjusted:q} {input.shapes:q} \
            -c "solar" -o {output.png:q} 2> {log:q}
        """
