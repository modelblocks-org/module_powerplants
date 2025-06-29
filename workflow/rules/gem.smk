rule prepare_gem_non_combustion:
    message:
        "Global Energy Monitor: obtain powerplants - {wildcards.category}."
    params:
        technology_mapping = lambda wc: config["categories"]["technology_mapping"][wc.category]
    input:
        gem_raw="resources/automatic/gem/integrated.xlsx",
    output:
        powerplant_capacity="resources/automatic/prepared/{category}.parquet",
    wildcard_constraints:
        category="|".join(["nuclear", "geothermal"])
    log:
        "logs/prepare_gem_non_combustion_{category}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/prepare_gem.py"

rule prepare_gem_combustion:
    message:
        "Global Energy Monitor: obtain combustion powerplants - {wildcards.category}."
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
