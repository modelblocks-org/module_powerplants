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


rule proxy_rooftop_solar:
    message:
        "Generating proxy for rooftop capacity {wildcards.shapes}."
    params:
        category = "solar",
        year = config["imputation"]["adjustment_yr"],
    input:
        script=workflow.source_path("../scripts/proxy.py"),
        borders="resources/user/borders/{shapes}.parquet",
        proxy="resources/user/proxies/rooftop_pv/{shapes}.tif",
        agg_unadj="results/{shapes}/aggregated/unadjusted/large_solar.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        proxy="results/{shapes}/aggregated/proxies/rooftop_pv.tif",
        plot="results/{shapes}/aggregated/proxies/rooftop_pv.png"
    log:
        "logs/proxy_rooftop_solar_{shapes}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python {input.script:q} capacity {input.borders:q} {input.proxy:q} {input.agg_unadj:q} {input.stats:q} \
            -o {output.proxy:q} -y {params.year} -c "{params.category}" 2> {log:q}
        python {input.script:q} plot {output.proxy:q} {input.borders:q} \
            -o {output.plot:q} 2> {log:q}
        """

rule aggregate_large_solar_capacity:
    message:
        "Aggregating unadjusted large solar capacity for {wildcards.shapes}."
    params:
        year = config["imputation"]["adjustment_yr"]
    input:
        script=workflow.source_path("../scripts/aggregate.py"),
        powerplants="results/{shapes}/disaggregated/unadjusted/large_solar.parquet",
        shapes="resources/user/shapes/{shapes}.parquet"
    output:
        aggregated="results/{shapes}/aggregated/unadjusted/large_solar.parquet",
        plot="results/{shapes}/aggregated/unadjusted/large_solar.png"
    log:
        "logs/aggregate_large_solar_capacity_{shapes}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python {input.script:q} capacity {input.powerplants:q} {input.shapes:q} \
            -y {params.year} -o {output.aggregated:q} 2> {log:q}
        python {input.script:q} plot {output.aggregated:q} {input.shapes:q} \
            -o {output.plot:q} -c "large solar" 2> {log:q}
        """


rule aggregate_solar_capacity:
    message:
        "Aggregating capacity for {wildcards.shapes}-unadjusted-solar."
    params:
        category="solar",
        technology=config["category"]["solar"]["technology_mapping"]["rooftop_pv"]
    input:
        script=workflow.source_path("../scripts/aggregate.py"),
        large_solar="results/{shapes}/aggregated/unadjusted/large_solar.parquet",
        proxy="results/{shapes}/aggregated/proxies/rooftop_pv.tif",
        shapes="resources/user/shapes/{shapes}.parquet",
    output:
        aggregated="results/{shapes}/aggregated/unadjusted/solar.parquet",
        plot="results/{shapes}/aggregated/unadjusted/solar.png"
    log:
        "logs/aggregate_solar_capacity_{shapes}.log",
    conda:
        "../envs/shapes.yaml",
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
        unadjusted="results/{shapes}/aggregated/unadjusted/solar.parquet",
        shapes="resources/user/shapes/{shapes}.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet"
    output:
        adjusted="results/{shapes}/aggregated/adjusted/solar.parquet",
        pdf="results/{shapes}/aggregated/adjusted/solar.pdf",
        png="results/{shapes}/aggregated/adjusted/solar.png",
    log:
        "logs/impute_capacity_adjustment_solar_{shapes}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python {input.script_impute:q} adjust-aggregated {input.stats:q} {input.unadjusted:q} \
            -y {params.year} -o {output.adjusted:q} 2> {log:q}
        python {input.script_impute:q} plot {input.stats:q} {input.unadjusted:q} {output.adjusted:q} \
            -y {params.year} -o {output.pdf:q} --aggregated 2> {log:q}
        python {input.script_aggregate:q} plot {output.adjusted:q} {input.shapes:q} \
            -c "solar" -o {output.png:q} 2> {log:q}
        """
