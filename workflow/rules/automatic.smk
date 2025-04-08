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
