"""Rules to used to download automatic resource files."""


rule download_eia:
    message:
        "Download the EIA International energy statistics in bulk."
    params:
        url=internal["resources"]["automatic"]["EIA"],
    output:
        path="resources/automatic/downloads/EIA-INTL.txt",
    log:
        "logs/download_eia.log",
    conda:
        "../envs/shell.yaml"
    shell:
        """
        curl -sSLo {output.path:q} {params.url:q}
        """


rule download_tz_sam:
    message:
        "Download the Transition Zero - Solar Asset Mapper dataset."
    params:
        url=internal["resources"]["automatic"]["TZ-SAM"],
    output:
        path="resources/automatic/downloads/TZ-SAM.gpkg",
    log:
        "logs/download_tz_sam.log",
    conda:
        "../envs/shell.yaml"
    shell:
        """
        curl -sSLo {output.path:q} {params.url:q}
        """


rule download_glohydrores:
    message:
        "Download the GloHydroRes dataset."
    params:
        url=internal["resources"]["automatic"]["GloHydroRes"],
    output:
        path="resources/automatic/downloads/GloHydroRes.csv",
    log:
        "logs/download_glohydrores.log",
    conda:
        "../envs/shell.yaml"
    shell:
        """
        curl -sSLo {output.path:q} {params.url:q}
        """


rule download_gem:
    message:
        "Download the GEM {wildcards.dataset} dataset."
    params:
        url=lambda wc: internal["resources"]["automatic"]["GEM"][wc.dataset],
    output:
        path="resources/automatic/downloads/GEM_{dataset}.xlsx",
    log:
        "logs/download_gem_{dataset}.log",
    conda:
        "../envs/shell.yaml"
    shell:
        """
        curl -sSLo {output.path:q} {params.url:q}
        """
