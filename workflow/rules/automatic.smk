"""Rules to used to download automatic resource files."""


rule eia_bulk_download:
    message:
        "Download the EIA International energy statistics in bulk."
    params:
        url=internal["resources"]["automatic"]["eia_bulk"],
    output:
        path=temp("resources/automatic/eia/INTL.txt"),
    log:
        "logs/eia_bulk_download.log",
    conda:
        "../envs/download.yaml"
    script:
        "../scripts/eia_bulk_download.py"

rule gem_gipt_download:
    message:
        "Download the Global Integrated Power Tracker dataset."
    params:
        url=internal["resources"]["automatic"]["gem"]["integrated"]
    output:
        path="resources/automatic/gem/integrated.xlsx",
    log:
        "logs/gem_bulk_download.log",
    conda:
        "../envs/shell.yaml"
    shell:
        'curl -sSLo {output.path} "{params.url}"'
