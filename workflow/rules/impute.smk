"""Rules related to internal and user-provided imputations."""


rule impute_years:
    input:
        prepared="<resources>/automatic/prepared/{category}.parquet",
        dissolved_shapes=rules.prepare_shapes.output.dissolved,
    output:
        imputed="<resources>/automatic/shapes/{shapes}/imputed/{category}.parquet",
        plot="<resources>/automatic/shapes/{shapes}/imputed/{category}.pdf",
    log:
        "<logs>/{shapes}/{category}/impute_years.log",
    wildcard_constraints:
        dataset="|".join(IMPUTED_CAT),
    conda:
        "../envs/powerplants.yaml"
    params:
        imputation=config["imputation"],
        projected_crs=config["projected_crs"],
        tech_map=lambda wc: get_technology_mapping(wc.category),
    message:
        "National-level imputation of missing years for all powerplants in {wildcards.shapes}-{wildcards.category} dataset."
    script:
        "../scripts/impute_years.py"


rule impute_category_combination:
    input:
        internal=rules.impute_years.output.imputed,
        user=branch(
            exists("<imputed_powerplants>"),
            then=["<imputed_powerplants>"],
            otherwise=[],
        ),
    output:
        combined=workflow.pathvars.apply("<powerplants>").format(
            shapes="{shapes}",
            adjustment="unadjusted",
            category="{category}",
        ),
        plot=report(
            "<results>/{shapes}/powerplants/unadjusted/{category}.pdf",
            caption="../report/impute_category_combination_histogram.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
        explore=report(
            "<results>/{shapes}/powerplants/unadjusted/{category}.html",
            caption="../report/impute_category_combination_map.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    log:
        "<logs>/{shapes}/{category}/impute_category_combination.log",
    wildcard_constraints:
        category="|".join(IMPUTED_CAT),
    conda:
        "../envs/powerplants.yaml"
    params:
        tech_map=lambda wc: get_technology_mapping(f"{wc.category}"),
        excluded=lambda wc: get_excluded_powerplant_ids(f"{wc.category}"),
    message:
        "National-level imputation of user-configured inclusions and exclusions for {wildcards.shapes}-{wildcards.category}."
    script:
        "../scripts/impute_category_combination.py"


rule impute_capacity_adjustment:
    input:
        unadjusted=rules.impute_category_combination.output.combined,
        stats=rules.prepare_statistics.output.categories,
    output:
        adjusted=workflow.pathvars.apply("<powerplants>").format(
            shapes="{shapes}",
            adjustment="adjusted",
            category="{category}",
        ),
        plot=report(
            "<results>/{shapes}/powerplants/adjusted/{category}.pdf",
            caption="../report/impute_capacity_adjustment.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    log:
        "<logs>/{shapes}/{category}/impute_capacity_adjustment.log",
    wildcard_constraints:
        category="|".join(IMPUTED_CAT - IMPUTED_CAT_WITHOUT_ADJUSTMENT),
    conda:
        "../envs/powerplants.yaml"
    message:
        "National-level adjustment of powerplant capacity in {wildcards.shapes}-{wildcards.category} to EIA statistics."
    script:
        "../scripts/impute_capacity_adjustment.py"
