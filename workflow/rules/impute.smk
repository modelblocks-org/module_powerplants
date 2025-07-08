"""Rules related to imputation and user-set modifications."""

# Ensure user-set technology mapping is correct
# Check lifetime technology names
lifetime_set = set(config["imputation"]["lifetime_yr"].keys())
mismatch = lifetime_set ^ set(config["imputation"]["retirement_delay_yr"])
if mismatch:
    raise ValueError(
        f"Technologies in lifetimes and retirement delays mismatch: {mismatch}."
    )
# Check category technology names
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
        mapping = dict()
        for tech in ["coal", "oil_gas"]:
            mapping |= config["category"]["fossil"]["technology_mapping"][tech]
    else:
        mapping = config["category"][filename]["technology_mapping"]
    return mapping


def get_files_to_combine(shapes, category):
    to_combine = []
    if category == "solar":
        to_combine += [
            f"resources/automatic/{shapes}/imputed/solar_utility_pv.parquet",
            f"resources/automatic/{shapes}/imputed/solar_csp.parquet",
        ]
    elif category == "fossil":
        to_combine += [
            f"resources/automatic/{shapes}/imputed/fossil_coal.parquet",
            f"resources/automatic/{shapes}/imputed/fossil_oil_gas.parquet",
        ]
    else:
        to_combine.append(f"resources/automatic/{shapes}/imputed/{category}.parquet")

    user_path = f"resources/user/{shapes}/imputed_{category}.parquet"
    if exists(user_path):
        to_combine.append(user_path)
    return to_combine


rule impute_years:
    message:
        "Imputing missing years values for {wildcards.shapes}-{wildcards.dataset} dataset."
    params:
        imputation=config["imputation"],
        tech_map=lambda wc: get_technology_mapping(wc.dataset)
    input:
        script=workflow.source_path("../scripts/impute_years.py"),
        prepared="resources/automatic/prepared/{dataset}.parquet",
        shapes="resources/user/shapes/{shapes}.parquet"
    output:
        imputed="resources/automatic/{shapes}/imputed/{dataset}.parquet",
        plot="resources/automatic/{shapes}/imputed/{dataset}.pdf"
    wildcard_constraints:
        dataset = "|".join(['bioenergy', 'fossil_coal', 'fossil_oil_gas', 'geothermal', 'hydropower', 'nuclear', 'solar_csp', 'solar_utility_pv', 'wind'])
    log:
        "logs/impute_years_{shapes}_{dataset}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python "{input.script}" impute "{input.prepared}" "{input.shapes}" "{params.imputation}" "{params.tech_map}" "{output.imputed}" 2> "{log}"
        python "{input.script}" plot "{output.imputed}" "{output.plot}" 2> "{log}"
        """

rule impute_combined:
    message:
        "Combine and impute user-configured inclusions and exclusions for {wildcards.shapes}-{wildcards.category}."
    params:
        tech_map=lambda wc: get_technology_mapping(f"{wc.category}"),
        excluded=lambda wc: config["category"][wc.category].get("excluded_ids", [])
    input:
        script=workflow.source_path("../scripts/impute_combined.py"),
        to_combine=  lambda wc: get_files_to_combine(wc.shapes, wc.category)
    output:
        combined="results/{shapes}/disaggregated/capacity/{category}.parquet",
        plot="results/{shapes}/disaggregated/capacity/{category}.pdf",
        explore="results/{shapes}/disaggregated/capacity/{category}.html"
    wildcard_constraints:
        category = "|".join(['bioenergy', 'fossil', 'geothermal', 'hydropower', 'nuclear', 'solar', 'wind'])
    log:
        "logs/impute_combined_{shapes}_{category}.log",
    conda:
        "../envs/shapes.yaml",
    script:
        "../scripts/impute_combined.py"
