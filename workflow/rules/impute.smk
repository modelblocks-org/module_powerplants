rule impute:
    message:
        "Impute missing values to each technology dataset."
    params:
        lifetimes=config["imputation"]["lifetime_yr"],
        delay=config["imputation"]["operating_to_retired_delay_yr"],
        script=workflow.source_path("../scripts/impute.py"),
    input:
        prepared_path="resources/automatic/prepared/{dataset}.parquet",
        shapes_path="resources/user/shapes.parquet"
    output:
        output_path="results/per_plant/capacities/{dataset}.parquet",
    log:
        "logs/impute_{dataset}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python "{params.script}" {input} "{params.lifetimes}" "{params.delay}" {output} 2> {log}
        """
