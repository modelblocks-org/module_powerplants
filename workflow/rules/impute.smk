"""Rules related to internal and user-provided imputations."""

IMPUTED_CAT = {
    "bioenergy",
    "fossil",
    "geothermal",
    "hydropower",
    "nuclear",
    "large_solar",
    "wind",
}
IMPUTED_CAT_SPECIAL = {"large_solar"}  # unique case due to missing data


rule impute_years:
    message:
        "National-level imputation of missing years for all powerplants in {wildcards.shapes}-{wildcards.dataset} dataset."
    params:
        imputation=config["imputation"],
        projected_crs=config["projected_crs"],
        tech_map=lambda wc: get_technology_mapping(wc.dataset),
    input:
        prepared="resources/automatic/prepared/{dataset}.parquet",
        borders="resources/user/borders/{shapes}.parquet",
    output:
        imputed="resources/automatic/{shapes}/imputed/{dataset}.parquet",
        plot="resources/automatic/{shapes}/imputed/{dataset}.pdf",
    wildcard_constraints:
        dataset="|".join(PREPARED_CAT),
    log:
        "logs/impute_years_{shapes}_{dataset}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/impute_years.py"


rule impute_category_combination:
    message:
        "National-level imputation of user-configured inclusions and exclusions for {wildcards.shapes}-{wildcards.category}."
    params:
        tech_map=lambda wc: get_technology_mapping(f"{wc.category}"),
        excluded=lambda wc: get_excluded_powerplant_ids(f"{wc.category}"),
    input:
        to_combine=lambda wc: get_files_to_combine(wc.shapes, wc.category),
    output:
        combined="resources/automatic/{shapes}/unadjusted/{category}.parquet",
        plot=report(
            "resources/automatic/{shapes}/unadjusted/{category}.pdf",
            caption="../report/impute_category_combination_histogram.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
        explore=report(
            "resources/automatic/{shapes}/unadjusted/{category}.html",
            caption="../report/impute_category_combination_map.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    wildcard_constraints:
        category="|".join(IMPUTED_CAT),
    log:
        "logs/impute_category_combination_{shapes}_{category}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/impute_category_combination.py"


rule impute_capacity_adjustment:
    message:
        "National-level adjustment of powerplant capacity in {wildcards.shapes}-{wildcards.category} to {params.year} statistics."
    params:
        year=config["imputation"]["adjustment_yr"],
    input:
        unadjusted="resources/automatic/{shapes}/unadjusted/{category}.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        adjusted="resources/automatic/{shapes}/adjusted/{category}.parquet",
        plot=report(
            "resources/automatic/{shapes}/adjusted/{category}.pdf",
            caption="../report/impute_capacity_adjustment.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    wildcard_constraints:
        category="|".join(IMPUTED_CAT - IMPUTED_CAT_SPECIAL),
    log:
        "logs/impute_capacity_adjustment_{shapes}_{category}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/impute_capacity_adjustment.py"
