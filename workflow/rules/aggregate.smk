"""Aggregation to user requested shapes."""


rule aggregate_capacity:
    input:
        powerplants="<powerplants>",
        shapes="<shapes>",
    output:
        aggregated="<aggregated_capacity>",
        plot=report(
            "<results>/{shapes}/aggregated/{adjustment}/{category}.pdf",
            caption="../report/aggregate_capacity.rst",
            category="Powerplants module",
            subcategory="{category}",
        ),
    log:
        "<logs>/{shapes}/{adjustment}/{category}/aggregate_capacity.log",
    wildcard_constraints:
        adjustment="|".join(ADJUSTMENTS),
        category="|".join(IMPUTED_CAT),
    conda:
        "../envs/powerplants.yaml"
    params:
        year=config["imputation"]["adjustment_year"],
        category=lambda wc: wc.category,
    message:
        "Aggregating capacity for {wildcards.shapes}-{wildcards.adjustment}-{wildcards.category}."
    script:
        "../scripts/aggregate_capacity.py"
