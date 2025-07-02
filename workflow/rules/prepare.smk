rule prepare_utility_pv:
    message:
        """
        Preparing utility PV data using:
        - Transition Zero Solar Asset Mapper
        - Global Energy Monitor's Solar Power Tracker.
        """
    params:
        dc_ac_ratio=config["solar"]["pv"]["dc_ac_ratio"],
        script=workflow.source_path("../scripts/prepare_utility_pv.py"),
    input:
        tz_sam_path="resources/automatic/tz/sam.gpkg",
        gem_gspt_path="resources/automatic/gem/gspt.xlsx",
    output:
        output_path="resources/automatic/prepared/utility_pv.parquet"
    log:
        "logs/prepare_utility_pv.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {params.script} {input} {output} --dc_ac_ratio {params.dc_ac_ratio}"
