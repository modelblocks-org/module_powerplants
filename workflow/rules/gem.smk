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
        combustion_plants="results/disaggregated/combustion_plants.parquet",
        combustion_plant_fuels="results/disaggregated/combustion_plant_fuels.parquet",
    log:
        "logs/get_gem_powerplants.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/get_gem_powerplants.py"
