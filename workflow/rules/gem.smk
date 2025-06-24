rule get_gem_powerplants:
    message:
        "Global Energy Monitor: obtain combustion, nuclear and geothermal powerplants."
    params:
        fuel_mapping=config["fuel_mapping"],
        default_fuel=config["categories"]["default_fuel"],
        technology_mapping=config["categories"]["technology_mapping"]
    input:
        gem_raw="resources/automatic/gem/integrated.xlsx",
    output:
        combustion_plants="resources/automatic/gem/combustion_plants.parquet",
        combustion_plant_fuels="resources/automatic/gem/combustion_plant_fuels.parquet",
        nuclear_plants="resources/automatic/gem/nuclear_plants.parquet",
        geothermal_plants="resources/automatic/gem/geothermal_plants.parquet"
    log:
        "logs/get_gem_powerplants.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/get_gem_powerplants.py"
