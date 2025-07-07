
lifetime_set = set(config["imputation"]["lifetime_yr"].keys())
mismatch = lifetime_set ^ set(config["imputation"]["operating_to_retired_delay_yr"])
if mismatch:
    raise ValueError(f"Imputed lifetimes and retirement delay mismatch: {mismatch}.")

tech_map_set = set()
for cat in ["bioenergy", "geothermal", "hydropower", "nuclear", "solar", "wind"]:
    tech_map_set |= set(config["category"][cat]["technology_mapping"].values())
for fossil_cat in ["coal", "oil_gas"]:
    tech_map_set |= set(config["category"]["fossil"]["technology_mapping"][fossil_cat].values())
mismatch = lifetime_set ^ tech_map_set
if mismatch:
    raise ValueError(f"Technology mapping does not match lifetime technologies for {mismatch}")



def get_technology_mapping(filename: str):
    if "solar" in filename:
        mapping = config["category"]["solar"]["technology_mapping"]
    elif "fossil" in filename:
        tech = filename.removeprefix("fossil_").removesuffix(".parquet")
        mapping = config["category"]["fossil"]["technology_mapping"][tech]
    else:
        mapping = config["category"][filename]["technology_mapping"]
    return mapping

rule impute:
    message:
        "Impute missing values to each technology dataset."
    params:
        lifetimes=config["imputation"]["lifetime_yr"],
        delay=config["imputation"]["operating_to_retired_delay_yr"],
        tech_map=lambda wc: get_technology_mapping(wc.dataset)
    input:
        script=workflow.source_path("../scripts/impute.py"),
        prepared="resources/automatic/prepared/{dataset}.parquet",
        shapes="resources/user/shapes.parquet"
    output:
        imputed="results/disaggregated/capacity/{dataset}.parquet",
        plot="results/disaggregated/capacity/{dataset}.pdf"
    wildcard_constraints:
        dataset = "|".join(['bioenergy', 'fossil_coal', 'fossil_oil_gas', 'geothermal', 'hydropower', 'nuclear', 'solar_csp', 'solar_utility_pv', 'wind'])
    log:
        "logs/impute_{dataset}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python "{input.script}" main "{input.prepared}" "{input.shapes}" "{params.tech_map}" "{params.lifetimes}" "{params.delay}" "{output.imputed}" 2> "{log}"
        python "{input.script}" plot "{output.imputed}" "{output.plot}" 2> "{log}"
        """
