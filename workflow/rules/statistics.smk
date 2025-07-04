rule process_eia_statistics:
    message:
        "Get EIA annual country capacity statistics."
    input:
        shapes="resources/user/shapes.parquet",
        eia_bulk="resources/automatic/eia/INTL.txt",
    output:
        total="results/statistics/total_capacity.parquet",
        categories="results/statistics/category_capacity.parquet",
        plot="results/statistics/category_capacity.pdf"
    log:
        "logs/process_eia_statistics.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/process_eia_statistics.py"
