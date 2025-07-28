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
        script=workflow.source_path("../scripts/impute_years.py"),
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
    shell:
        """
        python {input.script:q} impute {input.prepared:q} {input.borders:q} \
            -i "{params.imputation}" -t "{params.tech_map}" -c "{params.projected_crs}" \
            -o "{output.imputed}" 2> {log:q}
        python {input.script:q} plot {output.imputed:q} -o {output.plot:q} 2> {log:q}
        """


rule impute_national_category_combination:
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
            caption="../report/impute_national_category_combination_histogram.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
        explore=report(
            "resources/automatic/{shapes}/unadjusted/{category}.html",
            caption="../report/impute_national_category_combination_map.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    wildcard_constraints:
        category="|".join(IMPUTED_CAT),
    log:
        "logs/impute_national_category_combination_{shapes}_{category}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/impute_category_combination.py"


rule impute_national_capacity_adjustment:
    message:
        "National-level adjustement of powerplant capacity in {wildcards.shapes}-{wildcards.category} to {params.year} statistics."
    params:
        year=config["imputation"]["adjustment_yr"],
    input:
        script=workflow.source_path("../scripts/impute_capacity_adjustment.py"),
        unadjusted="resources/automatic/{shapes}/unadjusted/{category}.parquet",
        stats="results/{shapes}/statistics/category_capacity.parquet",
    output:
        adjusted="resources/automatic/{shapes}/adjusted/{category}.parquet",
        plot=report(
            "resources/automatic/{shapes}/adjusted/{category}.pdf",
            caption="../report/impute_national_capacity_adjustment.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    wildcard_constraints:
        category="|".join(IMPUTED_CAT - IMPUTED_CAT_SPECIAL),
    log:
        "logs/impute_national_capacity_adjustment_{shapes}_{category}.log",
    conda:
        "../envs/shapes.yaml"
    shell:
        """
        python {input.script:q} adjust-disaggregated {input.stats:q} {input.unadjusted:q} \
            -y {params.year} -o {output.adjusted:q} 2> {log:q}
        python {input.script:q} plot {input.stats:q} {input.unadjusted:q} {output.adjusted:q} \
            -y {params.year} -o {output.plot:q} --disaggregated 2> {log:q}
        """
