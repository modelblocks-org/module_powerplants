"""Aggregation to provided shapes."""


rule aggregate_capacity:
    message:
        "Aggregating capacity for {wildcards.shapes}-{wildcards.adjustment}-{wildcards.category}."
    input:
        script=workflow.source_path("../scripts/aggregate.py"),
        powerplants="results/{shapes}/disaggregated/{adjustment}/{category}.parquet",
        shapes="resources/user/shapes/{shapes}.parquet"
    output:
        aggregated="results/{shapes}/aggregated/{adjustment}/{category}.parquet",
        plot="results/{shapes}/aggregated/{adjustment}/{category}.png"
    wildcard_constraints:
        category = "|".join(['bioenergy', 'fossil', 'geothermal', 'hydropower', 'nuclear', 'wind'])
    log:
        "logs/aggregate_capacity_{shapes}_{adjustment}_{category}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python {input.script} capacity {input.powerplants} {input.shapes} -o {output.aggregated} 2> {log}
        python {input.script} plot {output.aggregated} {input.shapes} -o {output.plot} 2> {log}
        """
