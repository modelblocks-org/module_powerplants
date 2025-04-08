rule get_eia_capacity_statistics:
    message:
        "Get EIA annual country capacity statistics."
    params:
        disaggregate=lambda wc: True if wc.aggregation == "disaggregated" else False
    input:
        shapes="resources/user/shapes.parquet",
        eia_bulk="resources/automatic/eia/INTL.txt",
    output:
        annual_stats="resources/automatic/eia/{aggregation}_capacity.parquet",
    log:
        "logs/get_eia_capacity_statistics_{aggregation}.log",
    wildcard_constraints:
        aggregation="|".join(["total", "disaggregated"])
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/get_eia_capacity_statistics.py"
