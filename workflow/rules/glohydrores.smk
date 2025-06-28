rule prepare_glohydrores:
    message:
        "GloHydroRes: process and clean hydropower plants."
    params:
        lifetime= config["lifetime"]["hydropower"],
        script=workflow.source_path("../scripts/prepare_glohydrores.py")
    input:
        input_path="resources/automatic/glohydrores/data.csv",
    output:
        output_path="resources/automatic/prepared/hydropower_plants.parquet",
    log:
        "logs/prepare_glohydrores.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        "python {params.script} {input} {output} --lifetime {params.lifetime}"
