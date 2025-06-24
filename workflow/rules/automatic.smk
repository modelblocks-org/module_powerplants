"""Rules to used to download automatic resource files."""


rule download_eia:
    message:
        "Download the EIA International energy statistics in bulk."
    params:
        url=internal["resources"]["automatic"]["eia_bulk"],
    output:
        path=temp("resources/automatic/eia/INTL.txt"),
    log:
        "logs/download_eia.log",
    conda:
        "../envs/download.yaml"
    script:
        "../scripts/download_eia.py"

rule download_gem:
    message:
        "Download the Global Integrated Power Tracker dataset."
    params:
        url=internal["resources"]["automatic"]["gem"]
    output:
        path="resources/automatic/gem/integrated.xlsx",
    log:
        "logs/download_gem.log",
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
