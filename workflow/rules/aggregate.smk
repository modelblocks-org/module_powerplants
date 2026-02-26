"""Aggregation to user requested shapes."""

ADJUSTMENTS = ("adjusted", "unadjusted")


rule aggregate_capacity:
    message:
        "Aggregating capacity for {wildcards.shapes}-{wildcards.adjustment}-{wildcards.category}."
    params:
        year=config["imputation"]["adjustment_year"],
    input:
        powerplants="results/{shapes}/disaggregated/{adjustment}/{category}.parquet",
        shapes="resources/user/{shapes}/shapes.parquet",
    output:
        aggregated="results/{shapes}/aggregated/{adjustment}/{category}.parquet",
        plot=report(
            "results/{shapes}/aggregated/{adjustment}/{category}.pdf",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="{category}",
        )
    wildcard_constraints:
        adjustment="|".join(ADJUSTMENTS),
        category="|".join(IMPUTED_CAT),
    log:
        "logs/aggregate_capacity_{shapes}_{adjustment}_{category}.log",
    conda:
        "../envs/shapes.yaml"
    script:
        "../scripts/aggregate_capacity.py"
