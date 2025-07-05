rule prepare_hydropower:
    message:
        "Preparing hydropower powerplants using the GloHydroRes dataset."
    params:
        technology_mapping=config["hydropower"]["technology_mapping"]
    input:
        glohydrores_path="resources/automatic/downloads/GloHydroRes.csv",
    output:
        output_path="resources/automatic/prepared/hydropower.parquet",
    log:
        "logs/prepare_hydropower.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_hydropower.py"


rule prepare_utility_pv:
    message:
        "Preparing utility PV powerplants using the TZ-SAM and GEM-GSPT datasets."
    params:
        dc_ac_ratio=config["solar"]["utility_pv"]["dc_ac_ratio"],
    input:
        script=workflow.source_path("../scripts/prepare_utility_pv.py"),
        tz_sam="resources/automatic/downloads/TZ-SAM.gpkg",
        gem_gspt="resources/automatic/downloads/GEM_GSPT.xlsx",
    output:
        output_path="resources/automatic/prepared/utility_pv.parquet"
    log:
        "logs/prepare_utility_pv.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {input.script} {input.tz_sam} {input.gem_gspt} {output} --dc_ac_ratio {params.dc_ac_ratio}"


rule prepare_csp:
    message:
        "Preparing concentrated solar powerplants using the Global Solar Power Tracker (GEM-GSPT) dataset."
    params:
        dc_ac_ratio=config["solar"]["utility_pv"]["dc_ac_ratio"],
    input:
        script=workflow.source_path("../scripts/prepare_csp.py"),
        gem_gspt="resources/automatic/downloads/GEM_GSPT.xlsx",
    output:
        output_path="resources/automatic/prepared/csp.parquet",
    log:
        "logs/prepare_csp.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {input.script} {input.gem_gspt} {output} --dc_ac_ratio {params.dc_ac_ratio}"

if config["wind"]["source"] == "gem":

    rule prepare_wind_gem:
        message:
            "Preparing wind powerplants using the Global Wind Power Tracker (GEM-GWPT) dataset."
        input:
            script=workflow.source_path("../scripts/prepare_wind_gwpt.py"),
            gem_gwpt="resources/automatic/downloads/GEM_GWPT.xlsx",
        output:
            output_path="resources/automatic/prepared/wind.parquet",
        log:
            "logs/prepare_wind_gem.log",
        conda:
            "../envs/shapes.yaml",
        shell:
            "python {input.script} {input.gem_gwpt} {output}"

elif config["wind"]["source"] == "wemi":

    rule prepare_wind_wemi:
        message:
            "Preparing wind powerplants using the Wind Energy Market Intelligence dataset."
        input:
            script=workflow.source_path("../scripts/prepare_wind_wemi.py"),
            wemi="resources/user/WEMI.xls",
        output:
            output_path="resources/automatic/prepared/wind.parquet",
        log:
            "logs/prepare_wind_wemi.log",
        conda:
            "../envs/shapes.yaml"
        shell:
            "python {input.script} {input.wemi} {output}"

else:
    raise ValueError(f"Incorrect wind source configuration value '{config['wind']['source']}'")


rule prepare_coal:
    message:
        "Preparing coal powerplants using the Global Coal Power Tracker (GCPT) dataset."
    params:
        technology_mapping= config["coal"]["technology_mapping"],
        fuel_mapping = internal["fuel_mapping"] | config["fuel_mapping"],
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
        fuel_mapping = internal["fuel_mapping"] | config["fuel_mapping"],
    input:
        gem_gbpt="resources/automatic/downloads/GEM_GBPT.xlsx",
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
        fuel_mapping = internal["fuel_mapping"] | config["fuel_mapping"],
    input:
        gem_gogpt="resources/automatic/downloads/GEM_GOGPT.xlsx",
    output:
        plants="resources/automatic/prepared/oil_gas.parquet",
        fuels="results/per_plant/fuels/oil_gas.parquet"
    log:
        "logs/prepare_oil_gas.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_oil_gas.py"


rule prepare_nuclear:
    message:
        "Preparing nuclear powerplants using the Global Nuclear Power Tracker (GNPT) dataset."
    params:
        technology_mapping= config["nuclear"]["technology_mapping"],
    input:
        gem_gnpt="resources/automatic/downloads/GEM_GNPT.xlsx",
    output:
        plants="resources/automatic/prepared/nuclear.parquet",
    log:
        "logs/prepare_nuclear.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_nuclear.py"


rule prepare_geothermal:
    message:
        "Preparing geothermal powerplants using the Global Geothermal Power Tracker (GGPT) dataset."
    params:
        technology_mapping= config["geothermal"]["technology_mapping"],
    input:
        gem_ggpt="resources/automatic/downloads/GEM_GGPT.xlsx",
    output:
        plants="resources/automatic/prepared/geothermal.parquet",
    log:
        "logs/prepare_geothermal.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_geothermal.py"

