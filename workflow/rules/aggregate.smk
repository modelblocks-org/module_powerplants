"""Aggregation to provided shapes."""


rule aggregate_capacity:
    message:
        "Aggregating capacity for {wildcards.shapes}-{wildcards.adjustment}-{wildcards.category}."
    params:
        year=config["imputation"]["adjustment_yr"],
    input:
        script=workflow.source_path("../scripts/aggregate.py"),
        powerplants="results/{shapes}/disaggregated/{adjustment}/{category}.parquet",
        shapes="resources/user/shapes/{shapes}.parquet",
    output:
        aggregated="results/{shapes}/aggregated/{adjustment}/{category}.parquet",
        plot="results/{shapes}/aggregated/{adjustment}/{category}.png",
    wildcard_constraints:
        category="|".join(IMPUTED_CAT - IMPUTED_CAT_SPECIAL),
    log:
        "logs/aggregate_capacity_{shapes}_{adjustment}_{category}.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        """
        python {input.script:q} capacity {input.powerplants:q} {input.shapes:q} \
            -y {params.year} -o {output.aggregated:q} 2> {log:q}
        python {input.script:q} plot {output.aggregated:q} {input.shapes:q} \
            -c "{wildcards.category}" -o {output.plot:q} 2> {log:q}
        """
