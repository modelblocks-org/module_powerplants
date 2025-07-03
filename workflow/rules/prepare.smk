rule prepare_hydropower:
    message:
        "Preparing hydropower powerplants using the GloHydroRes dataset."
    params:
        lifetime= config["lifetime"]["hydropower"],
        script=workflow.source_path("../scripts/prepare_hydropower.py")
    input:
        input_path="resources/automatic/downloads/GloHydroRes.csv",
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
        tz_sam_path="resources/automatic/downloads/TZ-SAM.gpkg",
        gem_gspt_path="resources/automatic/downloads/GEM_GSPT.xlsx",
    output:
        output_path="resources/automatic/prepared/utility_pv.parquet"
    log:
        "logs/prepare_utility_pv.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {params.script} {input} {output} --dc_ac_ratio {params.dc_ac_ratio}"


rule prepare_csp:
    message:
        "Preparing concentrated solar powerplants using the Global Solar Power Tracker (GEM-GSPT) dataset."
    params:
        dc_ac_ratio=config["solar"]["utility_pv"]["dc_ac_ratio"],
        script=workflow.source_path("../scripts/prepare_csp.py")
    input:
        gem_gspt_path="resources/automatic/downloads/GEM_GSPT.xlsx",
    output:
        output_path="resources/automatic/prepared/csp.parquet",
    log:
        "logs/prepare_csp.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {params.script} {input} {output} --dc_ac_ratio {params.dc_ac_ratio}"


rule prepare_wind_gem:
    message:
        "Preparing wind powerplants using the Global Wind Power Tracker (GEM-GWPT) dataset."
    params:
        script=workflow.source_path("../scripts/prepare_wind_gwpt.py")
    input:
        gem_gspt_path="resources/automatic/downloads/GEM_GWPT.xlsx",
    output:
        output_path="resources/automatic/prepared/wind_gem.parquet",
    log:
        "logs/prepare_wind_gem.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {params.script} {input} {output}"


rule prepare_wind_wemi:
    message:
        "Preparing wind powerplants using the Wind Energy Market Intelligence dataset."
    params:
        lifetime= config["lifetime"]["wind"],
        script=workflow.source_path("../scripts/prepare_wind_wemi.py")
    input:
        input_path="resources/user/WEMI.xls",
    output:
        output_path="resources/automatic/prepared/wind_wemi.parquet",
    log:
        "logs/prepare_wind_wemi.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        "python {params.script} {input} {output} --lifetime {params.lifetime}"


rule prepare_coal:
    message:
        "Preparing coal powerplants using the Global Coal Power Tracker (GCPT) dataset."
    params:
        technology_mapping= config["coal"]["technology_mapping"],
        fuel_mapping = internal["fuel_mapping"] | config["coal"]["fuel_mapping"],
    input:
        gem_gcpt="resources/automatic/downloads/GEM_GCPT.xlsx",
    output:
        plants="resources/automatic/prepared/coal.parquet",
        fuels="results/per_plant/fuels/coal.parquet"
    log:
        "logs/prepare_coal.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_coal.py"


rule prepare_bioenergy:
    message:
        "Preparing bioenergy powerplants using the Global Bioenergy Power Tracker (GBPT) dataset."
    params:
        technology_mapping= config["bioenergy"]["technology_mapping"],
        fuel_mapping = internal["fuel_mapping"] | config["bioenergy"]["fuel_mapping"],
    input:
        gem_gcpt="resources/automatic/downloads/GEM_GBPT.xlsx",
    output:
        plants="resources/automatic/prepared/bioenergy.parquet",
        fuels="results/per_plant/fuels/bioenergy.parquet"
    log:
        "logs/prepare_bioenergy.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_bioenergy.py"

rule prepare_oil_gas:
    message:
        "Preparing oil and gas powerplants using the Global Oil and Gas Power Tracker (GOGPT) dataset."
    params:
        technology_mapping= config["oil_gas"]["technology_mapping"],
        fuel_mapping = internal["fuel_mapping"] | config["oil_gas"]["fuel_mapping"],
    input:
        gem_gcpt="resources/automatic/downloads/GEM_GOGPT.xlsx",
    output:
        plants="resources/automatic/prepared/oil_gas.parquet",
        fuels="results/per_plant/fuels/oil_gas.parquet"
    log:
        "logs/prepare_oil_gas.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_oil_gas.py"



# rule prepare_firm_non_combustion:
#     message:
#         "Preparing {wildcards.category} powerplants using the Global Energy Monitor dataset."
#     params:
#         technology_mapping = lambda wc: config["categories"]["technology_mapping"][wc.category]
#     input:
#         gem_raw="resources/automatic/gem/integrated.xlsx",
#     output:
#         powerplant_capacity="resources/automatic/prepared/{category}.parquet",
#     wildcard_constraints:
#         category="|".join(["nuclear", "geothermal"])
#     log:
#         "logs/prepare_firm_non_combustion_{category}.log",
#     conda:
#         "../envs/shapes.yaml"
#     script:
#         "../scripts/prepare_gem.py"

# rule prepare_firm_combustion:
#     message:
#         "Preparing {wildcards.category} combustion powerplants using the Global Energy Monitor dataset."
#     params:
#         default_fuel = lambda wc: config["categories"]["default_fuel"][wc.category],
#         technology_mapping = lambda wc: config["categories"]["technology_mapping"][wc.category]
#     input:
#         gem_raw="resources/automatic/gem/integrated.xlsx",
#     output:
#         powerplant_capacity="resources/automatic/prepared/{category}.parquet",
#         powerplant_fuels="resources/automatic/prepared/{category}_fuels.parquet"
#     wildcard_constraints:
#         category="|".join(["bioenergy", "coal", "oil_gas"])
#     log:
#         "logs/prepare_gem_combustion_{category}.log",
#     conda:
#         "../envs/shapes.yaml"
#     script:
#         "../scripts/prepare_gem.py"
