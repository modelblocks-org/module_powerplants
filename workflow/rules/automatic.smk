"""Rules to used to download automatic resource files."""


rule download_eia:
    message:
        "Download the EIA International energy statistics in bulk."
    params:
        script=workflow.source_path("../scripts/_url_unzip_single_file.py"),
        url=internal["resources"]["automatic"]["eia_bulk"],
        zip_file_path="INTL.txt"
    output:
        path=temp("resources/automatic/eia/INTL.txt"),
    log:
        "logs/download_eia.log",
    conda:
        "../envs/download.yaml"
    shell:
        'python {params.script} {params.url} {params.zip_file_path} {output.path}'


rule download_tz_sam:
    message:
        "Download the Transition Zero - Solar Asset Mapper dataset."
    params:
        script=workflow.source_path("../scripts/_url_unzip_single_file.py"),
        url=internal["resources"]["automatic"]["tz-sam"],
        zip_file_path="tz-sam-runs_2025-Q1_outputs_external_analysis_polygons.gpkg"
    output:
        path="resources/automatic/tz/sam.gpkg"
    log:
        "logs/download_tz_sam.log"
    conda:
        "../envs/download.yaml"
    shell:
        'python {params.script} {params.url} {params.zip_file_path} {output.path}'


rule download_gem_integrated:
    message:
        "Download the Global Integrated Power Tracker dataset."
    params:
        url=internal["resources"]["automatic"]["gem"]["integrated"]
    output:
        path="resources/automatic/gem/integrated.xlsx",
    log:
        "logs/download_gem_integrated.log",
    conda:
        "../envs/shell.yaml"
    shell:
        'curl -sSLo {output.path} "{params.url}"'

rule download_gem_solar:
    message:
        "Download the Global Solar Power Tracker dataset."
    params:
        url=internal["resources"]["automatic"]["gem"]["solar"]
    output:
        path="resources/automatic/gem/solar.xlsx",
    log:
        "logs/download_gem_solar.log",
    conda:
        "../envs/shell.yaml"
    shell:
        'curl -sSLo {output.path} "{params.url}"'

rule download_glohydrores:
    message:
        "Download the GloHydroRes dataset."
    params:
        url=internal["resources"]["automatic"]["glohydrores"]
    output:
        path="resources/automatic/glohydrores/data.csv"
    log:
        "logs/glohydrores_download.log"
    conda:
        "../envs/shell.yaml"
    shell:
        'curl -sSLo {output.path} "{params.url}"'
