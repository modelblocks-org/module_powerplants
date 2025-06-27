rule prepare_glohydrores:
    message:
        "GloHydroRes: process and clean hydropower plants."
    params:
        lifetime= 80
    input:
        input_path="resources/automatic/glohydrores/data.csv",
    output:
        output_path="resources/prepared/hydropower.parquet",
    log:
        "logs/prepare_glohydrores.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        "python {workflow.basedir}/scripts/prepare_glohydrores.py {input} {output} --lifetime {params.lifetime}"
