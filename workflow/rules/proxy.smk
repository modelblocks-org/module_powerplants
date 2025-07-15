"""Rules focusing on proxies used for aggregation."""


rule proxy_rooftop_solar:
    message:
        "Generating proxy for rooftop capacity {wildcards.shapes}."
    params:
        category = "solar",
        year = config["imputation"]["adjustment_yr"],
    input:
        script=workflow.source_path("../scripts/proxy.py"),
        borders="resources/user/borders/{shapes}.parquet",
        proxy="resources/user/proxies/rooftop_pv/{shapes}.tif",
        agg_unadj="results/{shapes}/aggregated/unadjusted/large_solar.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        proxy="results/{shapes}/aggregated/proxies/rooftop_pv.tif",
        plot="results/{shapes}/aggregated/proxies/rooftop_pv.png"
    log:
        "logs/proxy_rooftop_solar_{shapes}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python {input.script} capacity {input.borders} {input.proxy} {input.agg_unadj} {input.stats} \
            -o {output.proxy} -y {params.year} -c {params.category} 2> {log}
        python {input.script} plot {output.proxy} {input.borders} -o {output.plot} 2> {log}
        """

