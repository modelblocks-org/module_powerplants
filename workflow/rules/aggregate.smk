"""Aggregation to user requested shapes."""

ADJUSTMENTS = ("adjusted", "unadjusted")


rule aggregate_capacity:
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
        ),
    log:
        "logs/aggregate_capacity_{shapes}_{adjustment}_{category}.log",
    wildcard_constraints:
        adjustment="|".join(ADJUSTMENTS),
        category="|".join(IMPUTED_CAT),
    conda:
        "../envs/shapes.yaml"
    params:
        year=config["imputation"]["adjustment_year"],
    message:
        "Aggregating capacity for {wildcards.shapes}-{wildcards.adjustment}-{wildcards.category}."
    script:
        "../scripts/aggregate_capacity.py"
