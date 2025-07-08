"""Rules to used to download automatic resource files."""


rule download_eia:
    message:
        "Download the EIA International energy statistics in bulk."
    params:
        url=internal["resources"]["automatic"]["eia_bulk"],
        zip_file_path="INTL.txt"
    input:
        script=workflow.source_path("../scripts/unzip_webfile.py"),
    output:
        path="resources/automatic/downloads/EIA-INTL.txt",
    log:
        "logs/download_eia.log",
    conda:
        "../envs/download.yaml"
    shell:
        'python {input.script} {params.url} {params.zip_file_path} {output.path}'


rule download_tz_sam:
    message:
        "Download the Transition Zero - Solar Asset Mapper dataset."
    params:
        url=internal["resources"]["automatic"]["TZ-SAM"],
        zip_file_path="tz-sam-runs_2025-Q1_outputs_external_analysis_polygons.gpkg"
    input:
        script=workflow.source_path("../scripts/unzip_webfile.py"),
    output:
        path="resources/automatic/downloads/TZ-SAM.gpkg"
    log:
        "logs/download_tz_sam.log"
    conda:
        "../envs/download.yaml"
    shell:
        'python {input.script} {params.url} {params.zip_file_path} {output.path}'


rule download_glohydrores:
    message:
        "Download the GloHydroRes dataset."
    params:
        url=internal["resources"]["automatic"]["GloHydroRes"]
    output:
        path="resources/automatic/downloads/GloHydroRes.csv"
    log:
        "logs/download_glohydrores.log"
    conda:
        "../envs/shell.yaml"
    shell:
        'curl -sSLo {output.path} "{params.url}"'


rule download_gem:
    message:
        "Download the GEM {wildcards.dataset} dataset."
    params:
        url=lambda wc: internal["resources"]["automatic"]["GEM"][wc.dataset]
    output:
        path="resources/automatic/downloads/GEM_{dataset}.xlsx"
    log:
        "logs/download_gem_{dataset}.log"
    conda:
        "../envs/shell.yaml"
    shell:
        'curl -sSLo {output.path} "{params.url}"'
