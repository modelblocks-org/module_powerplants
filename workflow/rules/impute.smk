"""Rules related to imputation and user-set modifications."""

IMPUTED_CAT = {
    "bioenergy",
    "fossil",
    "geothermal",
    "hydropower",
    "nuclear",
    "large_solar",
    "wind",
}
IMPUTED_CAT_SPECIAL = {"large_solar"}





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
        shapes="resources/user/shapes/{shapes}.parquet",
    output:
        imputed="resources/automatic/{shapes}/imputed/{dataset}.parquet",
        plot="resources/automatic/{shapes}/imputed/{dataset}.pdf",
    wildcard_constraints:
        dataset="|".join(PREPARED_CAT),
    log:
        "logs/impute_years_{shapes}_{dataset}.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        """
        python {input.script:q} impute {input.prepared:q} {input.shapes:q} \
            -i "{params.imputation}" -t "{params.tech_map}" -c "{params.projected_crs}" \
            -o "{output.imputed}" 2> {log:q}
        python {input.script:q} plot {output.imputed:q} -o {output.plot:q} 2> {log:q}
        """


rule impute_category_combination:
    message:
        "Combine sub-categories and impute user-configured inclusions and exclusions for {wildcards.shapes}-{wildcards.category}."
    params:
        tech_map=lambda wc: get_technology_mapping(f"{wc.category}"),
        excluded=lambda wc: get_excluded_powerplant_ids(f"{wc.category}"),
    input:
        to_combine=lambda wc: get_files_to_combine(wc.shapes, wc.category),
    output:
        combined="results/{shapes}/disaggregated/unadjusted/{category}.parquet",
        plot=report(
            "results/{shapes}/disaggregated/unadjusted/{category}.pdf",
            caption="../report/impute_category_combination_histogram.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
        explore=report(
            "results/{shapes}/disaggregated/unadjusted/{category}.html",
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


rule impute_disaggregated_capacity_adjustment:
    message:
        "Adjusting disaggregated capacity of {wildcards.shapes}-{wildcards.category} to {params.year} statistics."
    params:
        year=config["imputation"]["adjustment_yr"],
    input:
        script=workflow.source_path("../scripts/impute_capacity_adjustment.py"),
        unadjusted="results/{shapes}/disaggregated/unadjusted/{category}.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        adjusted="results/{shapes}/disaggregated/adjusted/{category}.parquet",
        plot=report(
            "results/{shapes}/disaggregated/adjusted/{category}.pdf",
            caption="../report/impute_disaggregated_capacity_adjustment.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    wildcard_constraints:
        category="|".join(IMPUTED_CAT - IMPUTED_CAT_SPECIAL),
    log:
        "logs/impute_disaggregated_capacity_adjustment_{shapes}_{category}.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        """
        python {input.script:q} adjust-disaggregated {input.stats:q} {input.unadjusted:q} \
            -y {params.year} -o {output.adjusted:q} 2> {log:q}
        python {input.script:q} plot {input.stats:q} {input.unadjusted:q} {output.adjusted:q} \
            -y {params.year} -o {output.plot:q} --disaggregated 2> {log:q}
        """
