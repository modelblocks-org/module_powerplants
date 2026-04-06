"""Aggregation to user requested shapes."""


rule aggregate_capacity:
    message:
        "Aggregating capacity for {wildcards.shapes}-{wildcards.adjustment}-{wildcards.category}."
    params:
        year=config["imputation"]["adjustment_year"],
        category=lambda wc: wc.category
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
    wildcard_constraints:
        adjustment="|".join(ADJUSTMENTS),
        category="|".join(IMPUTED_CAT),
    log:
        "<logs>/{shapes}/{adjustment}/{category}/aggregate_capacity.log",
    conda:
        "../envs/powerplants.yaml"
    script:
        "../scripts/aggregate_capacity.py"
