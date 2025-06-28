rule prepare_gem:
    message:
        "Global Energy Monitor: obtain combustion, nuclear and geothermal powerplants."
    params:
        fuel_mapping=internal["gem"]["fuel_mapping"],
        default_fuel=config["categories"]["default_fuel"],
        technology_mapping=config["categories"]["technology_mapping"]
    input:
        gem_raw="resources/automatic/gem/integrated.xlsx",
    output:
        combustion_plants="resources/automatic/prepared/combustion.parquet",
        combustion_plant_fuels="resources/automatic/prepared/combustion_fuels.parquet",
        nuclear_plants="resources/automatic/prepared/nuclear.parquet",
        geothermal_plants="resources/automatic/prepared/geothermal.parquet"
    log:
        "logs/prepare_gem.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_gem.py"
