"""Rules in this file focus on parsing and cleaning data into shared schemas."""

rule prepare_hydropower:
    message:
        "Preparing hydropower powerplants using the GloHydroRes dataset."
    params:
        technology_mapping=config["category"]["hydropower"]["technology_mapping"]
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

# TODO: technology_mapping?
rule prepare_solar_utility_pv:
    message:
        "Preparing utility PV powerplants using the TZ-SAM and GEM-GSPT datasets."
    params:
        dc_ac_ratio=config["category"]["solar"]["dc_ac_ratio"]["utility_pv"],
    input:
        script=workflow.source_path("../scripts/prepare_solar_utility_pv.py"),
        tz_sam="resources/automatic/downloads/TZ-SAM.gpkg",
        gem_gspt="resources/automatic/downloads/GEM_GSPT.xlsx",
    output:
        output_path="resources/automatic/prepared/solar_utility_pv.parquet"
    log:
        "logs/prepare_solar_utility_pv.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {input.script} {input.tz_sam} {input.gem_gspt} {output} --dc_ac_ratio {params.dc_ac_ratio}"

# TODO: technology mapping?
rule prepare_solar_csp:
    message:
        "Preparing concentrated solar powerplants using the Global Solar Power Tracker (GEM-GSPT) dataset."
    params:
        dc_ac_ratio=config["category"]["solar"]["dc_ac_ratio"]["csp"],
    input:
        script=workflow.source_path("../scripts/prepare_solar_csp.py"),
        gem_gspt="resources/automatic/downloads/GEM_GSPT.xlsx",
    output:
        output_path="resources/automatic/prepared/solar_csp.parquet",
    log:
        "logs/prepare_solar_csp.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        "python {input.script} {input.gem_gspt} {output} --dc_ac_ratio {params.dc_ac_ratio}"


# TODO: technology mapping?
if config["category"]["wind"]["source"] == "gem":

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

elif config["category"]["wind"]["source"] == "wemi":

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


rule prepare_fossil_coal:
    message:
        "Preparing coal powerplants using the Global Coal Power Tracker (GCPT) dataset."
    params:
        technology_mapping= config["category"]["fossil"]["technology_mapping"]["coal"],
        fuel_mapping = internal["fuel_mapping"] | config["fuel_mapping"],
    input:
        gem_gcpt="resources/automatic/downloads/GEM_GCPT.xlsx",
    output:
        plants="resources/automatic/prepared/fossil_coal.parquet",
        fuels="results/disaggregated/fuels/fossil_coal.parquet"
    log:
        "logs/prepare_fossil_coal.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_fossil_coal.py"


rule prepare_bioenergy:
    message:
        "Preparing bioenergy powerplants using the Global Bioenergy Power Tracker (GBPT) dataset."
    params:
        technology_mapping= config["category"]["bioenergy"]["technology_mapping"],
        fuel_mapping = internal["fuel_mapping"] | config["fuel_mapping"],
    input:
        gem_gbpt="resources/automatic/downloads/GEM_GBPT.xlsx",
    output:
        plants="resources/automatic/prepared/bioenergy.parquet",
        fuels="results/disaggregated/fuels/bioenergy.parquet"
    log:
        "logs/prepare_bioenergy.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_bioenergy.py"

rule prepare_fossil_oil_gas:
    message:
        "Preparing oil and gas powerplants using the Global Oil and Gas Power Tracker (GOGPT) dataset."
    params:
        technology_mapping= config["category"]["fossil"]["technology_mapping"]["oil_gas"],
        fuel_mapping = internal["fuel_mapping"] | config["fuel_mapping"],
    input:
        gem_gogpt="resources/automatic/downloads/GEM_GOGPT.xlsx",
    output:
        plants="resources/automatic/prepared/fossil_oil_gas.parquet",
        fuels="results/disaggregated/fuels/fossil_oil_gas.parquet"
    log:
        "logs/prepare_fossil_oil_gas.log"
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_fossil_oil_gas.py"


rule prepare_nuclear:
    message:
        "Preparing nuclear powerplants using the Global Nuclear Power Tracker (GNPT) dataset."
    params:
        technology_mapping= config["category"]["nuclear"]["technology_mapping"],
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
        technology_mapping= config["category"]["geothermal"]["technology_mapping"],
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

rule prepare_statistics:
    message:
        "Get EIA annual country capacity statistics."
    input:
        script = workflow.source_path("../scripts/prepare_statistics.py"),
        shapes="resources/user/shapes.parquet",
        eia_bulk="resources/automatic/eia/INTL.txt",
    output:
        total="results/statistics/total_capacity.parquet",
        categories="results/statistics/category_capacity.parquet",
        plot="results/statistics/category_capacity.pdf"
    log:
        "logs/prepare_statistics.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        """
        python "{input.script}" prepare "{input.shapes}" "{input.eia_bulk}" "{output.total}" "{output.categories}" 2> {log}
        python "{input.script}" plot "{output.total}" "{output.categories}" "{output.plot}" 2> {log}
        """
