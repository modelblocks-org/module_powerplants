"""Rules in this file focus on parsing and cleaning data into shared schemas."""

PREPARED_FUEL_CAT = ("bioenergy", "fossil_coal", "fossil_oil_gas")
PREPARED_PLANT_CAT = (
    "bioenergy",
    "fossil_coal",
    "fossil_oil_gas",
    "geothermal",
    "hydropower",
    "nuclear",
    "large_solar",
    "wind",
)


rule prepare_shapes:
    message:
        "Preparing intermediate shapefile versions to speed up processing."
    params:
        crs=config["projected_crs"],
    input:
        shapes="<shapes>",
    output:
        dissolved="<resources>/automatic/shapes/{shapes}/dissolved.parquet",
        dissolved_plt=report(
            "<resources>/automatic/shapes/{shapes}/dissolved.png",
            caption="../report/prepare_shapes.rst",
            category="Powerplants module",
            subcategory="preparation"
        )
    log:
        "<logs>/{shapes}/prepare_shapes.log"
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/prepare_shapes.py"


rule prepare_hydropower:
    message:
        "Preparing hydropower powerplants using the GloHydroRes dataset."
    params:
        technology_mapping=config["category"]["hydropower"]["technology_mapping"],
    input:
        glohydrores_path="<resources>/automatic/downloads/GloHydroRes.csv",
    output:
        output_path="<resources>/automatic/prepared/hydropower.parquet",
    log:
        "<logs>/prepare_hydropower.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/prepare_hydropower.py"


rule prepare_large_solar:
    message:
        "Preparing utility PV powerplants using the TZ-SAM and GEM-GSPT datasets."
    params:
        dc_ac_ratio=config["category"]["solar"]["dc_ac_ratio"]["utility_pv"],
        utility_pv_name=config["category"]["solar"]["technology_mapping"]["utility_pv"],
        csp_name=config["category"]["solar"]["technology_mapping"]["csp"],
    input:
        tz_sam="<resources>/automatic/downloads/TZ-SAM.gpkg",
        gem_gspt="<resources>/automatic/downloads/GEM_GSPT.xlsx",
    output:
        large_solar="<resources>/automatic/prepared/large_solar.parquet",
    log:
        "<logs>/prepare_large_solar.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/prepare_large_solar.py"


if config["category"]["wind"]["source"] == "gem":

    rule prepare_wind_gem:
        message:
            "Preparing wind powerplants using the Global Wind Power Tracker (GEM-GWPT) dataset."
        params:
            tech_map=config["category"]["wind"]["technology_mapping"],
        input:
            script=workflow.source_path("../scripts/prepare_wind_gwpt.py"),
            gem_gwpt="<resources>/automatic/downloads/GEM_GWPT.xlsx",
        output:
            path="<resources>/automatic/prepared/wind.parquet",
        log:
            "<logs>/prepare_wind_gem.log",
        conda:
            "../envs/powerplants.yaml"
        shell:
            """
            python {input.script:q} {input.gem_gwpt:q} \
                -t "{params.tech_map}" -o {output:q} 2> {log:q}
            """

elif config["category"]["wind"]["source"] == "wemi":

    rule prepare_wind_wemi:
        message:
            "Preparing wind powerplants using the Wind Energy Market Intelligence dataset."
        params:
            tech_map=config["category"]["wind"]["technology_mapping"],
        input:
            script=workflow.source_path("../scripts/prepare_wind_wemi.py"),
            wemi="<resources>/user/WEMI.xls",
        output:
            path="<resources>/automatic/prepared/wind.parquet",
        log:
            "<logs>/prepare_wind_wemi.log",
        conda:
            "../envs/powerplants.yaml"
        shell:
            """
            python {input.script:q} {input.wemi:q} \
                -t "{params.tech_map}" -o {output.path:q} 2> {log:q}
            """

else:
    raise ValueError(
        f"Incorrect wind source configuration value '{config['wind']['source']}'"
    )


rule prepare_bioenergy:
    message:
        "Preparing bioenergy powerplants using the Global Bioenergy Power Tracker (GBPT) dataset."
    params:
        technology_mapping=config["category"]["bioenergy"]["technology_mapping"],
        fuel_mapping=internal["fuel_mapping"] | config["fuel_mapping"],
    input:
        gem_gbpt="<resources>/automatic/downloads/GEM_GBPT.xlsx",
    output:
        plants=temp("<resources>/automatic/temp/plants_bioenergy.parquet"),
        fuels=temp("<resources>/automatic/temp/fuels_bioenergy.parquet"),
    log:
        "<logs>/prepare_bioenergy.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/prepare_bioenergy.py"


rule prepare_fossil_oil_gas:
    message:
        "Preparing oil and gas powerplants using the Global Oil and Gas Power Tracker (GOGPT) dataset."
    params:
        technology_mapping=config["category"]["fossil"]["technology_mapping"],
        fuel_mapping=internal["fuel_mapping"] | config["fuel_mapping"],
    input:
        gem_gcpt="<resources>/automatic/downloads/GEM_GCPT.xlsx",
        gem_gogpt="<resources>/automatic/downloads/GEM_GOGPT.xlsx",
    output:
        og_plants=temp("<resources>/automatic/temp/plants_fossil_oil_gas.parquet"),
        og_fuels=temp("<resources>/automatic/temp/fuels_fossil_oil_gas.parquet"),
        coal_plants=temp("<resources>/automatic/temp/plants_fossil_coal.parquet"),
        coal_fuels=temp("<resources>/automatic/temp/fuels_fossil_coal.parquet"),
    log:
        "<logs>/prepare_fossil_oil_gas.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/prepare_fossil_oil_gas.py"


rule prepare_nuclear:
    message:
        "Preparing nuclear powerplants using the Global Nuclear Power Tracker (GNPT) dataset."
    params:
        technology_mapping=config["category"]["nuclear"]["technology_mapping"],
    input:
        gem_gnpt="<resources>/automatic/downloads/GEM_GNPT.xlsx",
    output:
        plants="<resources>/automatic/prepared/nuclear.parquet",
    log:
        "<logs>/prepare_nuclear.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/prepare_nuclear.py"


rule prepare_geothermal:
    message:
        "Preparing geothermal powerplants using the Global Geothermal Power Tracker (GGPT) dataset."
    params:
        technology_mapping=config["category"]["geothermal"]["technology_mapping"],
    input:
        gem_ggpt="<resources>/automatic/downloads/GEM_GGPT.xlsx",
    output:
        plants="<resources>/automatic/prepared/geothermal.parquet",
    log:
        "<logs>/prepare_geothermal.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/prepare_geothermal.py"


rule prepare_statistics:
    message:
        "Get EIA annual country capacity statistics."
    input:
        shapes="<shapes>",
        eia_bulk="<resources>/automatic/downloads/EIA-INTL.txt",
    output:
        total="<results>/{shapes}/statistics/total_capacity.parquet",
        categories="<results>/{shapes}/statistics/category_capacity.parquet",
        plot="<results>/{shapes}/statistics/category_capacity.pdf",
    log:
        "<logs>/prepare_statistics_{shapes}.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/prepare_statistics.py"


rule prepare_fuels:
    message:
        "Get a harmonised dataset of fuel class combinations."
    input:
        script=workflow.source_path("../scripts/prepare_fuels.py"),
        fuel_classes=expand(
            "<resources>/automatic/temp/fuels_{cat}.parquet", cat=PREPARED_FUEL_CAT
        ),
    output:
        "<results>/fuel_classes.parquet",
    log:
        "<logs>/prepare_fuels.log",
    conda:
        "../envs/powerplants.yaml"
    shell:
        """
        python {input.script:q} prepare {input.fuel_classes:q} -o {output:q} 2> {log:q}
        """


rule prepare_remapped_fuel_categories:
    message:
        "Remap fuel classes of combustion plants to harmonised ones."
    input:
        script=workflow.source_path("../scripts/prepare_fuels.py"),
        plants="<resources>/automatic/temp/plants_{category}.parquet",
        old="<resources>/automatic/temp/fuels_{category}.parquet",
        new="<results>/fuel_classes.parquet",
    output:
        "<resources>/automatic/prepared/{category}.parquet",
    log:
        "<logs>/prepare_remapped_fuel_categories_{category}.log",
    wildcard_constraints:
        category="|".join(PREPARED_FUEL_CAT),
    conda:
        "../envs/powerplants.yaml"
    shell:
        """
        python {input.script:q} remap {input.plants:q} {input.old:q} {input.new:q} \
            -o {output:q} 2> {log:q}
        """
