rule prepare_hydropower:
    message:
        "Preparing hydropower powerplants using the GloHydroRes dataset."
    params:
        lifetime= config["lifetime"]["hydropower"],
        script=workflow.source_path("../scripts/prepare_hydropower.py")
    input:
        input_path="resources/automatic/glohydrores/data.csv",
    output:
        output_path="resources/automatic/prepared/hydropower.parquet",
    log:
        "logs/prepare_hydropower.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        "python {params.script} {input} {output} --lifetime {params.lifetime}"



rule prepare_utility_pv:
    message:
        "Preparing utility PV powerplants using the Solar Asset Mapper (TZ-SAM) "
        "and the Global Solar Power Tracker (GEM-GSPT) datasets."
    params:
        dc_ac_ratio=config["solar"]["utility_pv"]["dc_ac_ratio"],
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


rule prepare_concentrated_solar:
    message:
        "Preparing concentrated solar powerplants using the Global Solar Power Tracker (GEM-GSPT) dataset."
    params:
        dc_ac_ratio=config["solar"]["utility_pv"]["dc_ac_ratio"],
        script=workflow.source_path("../scripts/prepare_csp.py")
    input:
        gem_gspt_path="resources/automatic/gem/gspt.xlsx",
    output:
        output_path="resources/automatic/prepared/concentrated_solar.parquet",
    log:
        "logs/prepare_concentrated_solar.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {params.script} {input} {output} --dc_ac_ratio {params.dc_ac_ratio}"


rule prepare_wind_wemi:
    message:
        "Preparing wind powerplants using the Wind Energy Market Intelligence dataset."
    params:
        lifetime= config["lifetime"]["wind"],
        script=workflow.source_path("../scripts/prepare_wind_wemi.py")
    input:
        input_path="resources/user/wemi.xls",
    output:
        output_path="resources/automatic/prepared/wind.parquet",
    log:
        "logs/prepare_wind_wemi.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        "python {params.script} {input} {output} --lifetime {params.lifetime}"


rule prepare_firm_non_combustion:
    message:
        "Preparing {wildcards.category} powerplants using the Global Energy Monitor dataset."
    params:
        technology_mapping = lambda wc: config["categories"]["technology_mapping"][wc.category]
    input:
        gem_raw="resources/automatic/gem/integrated.xlsx",
    output:
        powerplant_capacity="resources/automatic/prepared/{category}.parquet",
    wildcard_constraints:
        category="|".join(["nuclear", "geothermal"])
    log:
        "logs/prepare_firm_non_combustion_{category}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_gem.py"

rule prepare_firm_combustion:
    message:
        "Preparing {wildcards.category} combustion powerplants using the Global Energy Monitor dataset."
    params:
        default_fuel = lambda wc: config["categories"]["default_fuel"][wc.category],
        technology_mapping = lambda wc: config["categories"]["technology_mapping"][wc.category]
    input:
        gem_raw="resources/automatic/gem/integrated.xlsx",
    output:
        powerplant_capacity="resources/automatic/prepared/{category}.parquet",
        powerplant_fuels="resources/automatic/prepared/{category}_fuels.parquet"
    wildcard_constraints:
        category="|".join(["bioenergy", "coal", "oil_gas"])
    log:
        "logs/prepare_gem_combustion_{category}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_gem.py"
