rule prepare_wemi:
    message:
        "Wind Energy Market Intelligence: process and clean wind plants."
    params:
        lifetime= config["lifetime"]["wind"],
        script=workflow.source_path("../scripts/prepare_wemi.py")
    input:
        input_path="resources/user/wemi.xls",
    output:
        output_path="resources/automatic/prepared/wind.parquet",
    log:
        "logs/prepare_wemi.log",
    conda:
        "../envs/shapes.yaml"
    wildcard_constraints:
        ext = "xls|xlsx"
    shell:
        "python {params.script} {input} {output} --lifetime {params.lifetime}"
