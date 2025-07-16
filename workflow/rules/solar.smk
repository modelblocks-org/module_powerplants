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
        python {input.script} capacity {input.borders} {input.proxy} {input.agg_unadj} {input.stats} \
            -o {output.proxy} -y {params.year} -c {params.category} 2> {log}
        python {input.script} plot {output.proxy} {input.borders} -o {output.plot} 2> {log}
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
        python {input.script} capacity {input.powerplants} {input.shapes} -y {params.year} -o {output.aggregated} 2> {log}
        python {input.script} plot {output.aggregated} {input.shapes} -o {output.plot} -c "large solar" 2> {log}
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
        python {input.script} capacity-solar {input.large_solar} {input.proxy} {input.shapes} -o {output.aggregated} -c "{params.category}" -t "{params.technology}" 2> {log}
        python {input.script} plot {output.aggregated} {input.shapes} -c "{params.category}" -o {output.plot} 2> {log}
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
        python "{input.script_impute}" adjust-aggregated "{input.stats}" "{input.unadjusted}" -y {params.year} -o "{output.adjusted}" 2> "{log}"
        python "{input.script_impute}" plot "{input.stats}" "{input.unadjusted}" "{output.adjusted}" -y {params.year} -o "{output.pdf}" --aggregated 2> "{log}"
        python "{input.script_aggregate}" plot "{output.adjusted}" "{input.shapes}" -c "solar" -o "{output.png}" 2> "{log}"
        """
