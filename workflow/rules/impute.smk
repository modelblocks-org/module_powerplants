"""Rules related to imputation and user-set modifications."""

IMPUTED_CAT = {'bioenergy', 'fossil', 'geothermal', 'hydropower', 'nuclear', 'large_solar', 'wind'}
IMPUTED_CAT_SPECIAL = {"large_solar"}


def get_config_category(category):
    if category == "large_solar":
        config_cat = "solar"
    else:
        config_cat = category
    return config_cat


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
    if category == "large_solar":
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
        projected_crs=config["projected_crs"],
        tech_map=lambda wc: get_technology_mapping(wc.dataset),
    input:
        script=workflow.source_path("../scripts/impute_years.py"),
        prepared="resources/automatic/prepared/{dataset}.parquet",
        shapes="resources/user/shapes/{shapes}.parquet"
    output:
        imputed="resources/automatic/{shapes}/imputed/{dataset}.parquet",
        plot="resources/automatic/{shapes}/imputed/{dataset}.pdf"
    wildcard_constraints:
        dataset = "|".join(PREPARED_CAT)
    log:
        "logs/impute_years_{shapes}_{dataset}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python "{input.script}" impute "{input.prepared}" "{input.shapes}" "{params.imputation}" "{params.tech_map}" "{output.imputed}" -c "{params.projected_crs}" 2> "{log}"
        python "{input.script}" plot "{output.imputed}" "{output.plot}" 2> "{log}"
        """

rule impute_category_combination:
    message:
        "Combine sub-categories and impute user-configured inclusions and exclusions for {wildcards.shapes}-{wildcards.category}."
    params:
        tech_map=lambda wc: get_technology_mapping(f"{wc.category}"),
        excluded=lambda wc: config["category"][get_config_category(wc.category)].get("excluded_ids", [])
    input:
        to_combine=  lambda wc: get_files_to_combine(wc.shapes, wc.category)
    output:
        combined="results/{shapes}/disaggregated/unadjusted/{category}.parquet",
        plot="results/{shapes}/disaggregated/unadjusted/{category}.pdf",
        explore="results/{shapes}/disaggregated/unadjusted/{category}.html"
    wildcard_constraints:
        category = "|".join(IMPUTED_CAT)
    log:
        "logs/impute_category_combination_{shapes}_{category}.log",
    conda:
        "../envs/shapes.yaml",
    script:
        "../scripts/impute_category_combination.py"


rule impute_disaggregated_capacity_adjustment:
    message:
        "Adjusting disaggregated capacity of {wildcards.shapes}-{wildcards.category} to {params.year} statistics."
    params:
        year=config["imputation"]["adjustment_yr"],
    input:
        script=workflow.source_path("../scripts/impute_capacity_adjustment.py"),
        unadjusted="results/{shapes}/disaggregated/unadjusted/{category}.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet"
    output:
        adjusted="results/{shapes}/disaggregated/adjusted/{category}.parquet",
        plot="results/{shapes}/disaggregated/adjusted/{category}.pdf",
    wildcard_constraints:
        category = "|".join(IMPUTED_CAT - IMPUTED_CAT_SPECIAL)
    log:
        "logs/impute_disaggregated_capacity_adjustment_{shapes}_{category}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python "{input.script}" adjust-disaggregated "{input.stats}" "{input.unadjusted}" -y {params.year} -o "{output.adjusted}" 2> "{log}"
        python "{input.script}" plot "{input.stats}" "{input.unadjusted}" "{output.adjusted}" -y {params.year} -o "{output.plot}" --disaggregated 2> "{log}"
        """
