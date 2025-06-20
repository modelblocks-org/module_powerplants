rule get_eia_capacity_statistics:
    message:
        "Get EIA annual country capacity statistics."
    input:
        shapes="resources/user/shapes.parquet",
        eia_bulk="resources/automatic/eia/INTL.txt",
    output:
        total="resources/automatic/eia/total_capacity.parquet",
        disaggregated="resources/automatic/eia/disaggregated_capacity.parquet"
    log:
        "logs/get_eia_capacity_statistics.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/get_eia_capacity_statistics.py"
