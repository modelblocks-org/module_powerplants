rule impute:
    message:
        "Impute missing values to each technology dataset."
    params:
        lifetimes=config["imputation"]["lifetime_yr"],
        delay=config["imputation"]["operating_to_retired_delay_yr"],
    input:
        script=workflow.source_path("../scripts/impute.py"),
        prepared="resources/automatic/prepared/{dataset}.parquet",
        shapes="resources/user/shapes.parquet"
    output:
        imputed="results/disaggregated/capacity/{dataset}.parquet",
        plot="results/disaggregated/capacity/{dataset}.pdf"
    wildcard_constraints:
        dataset = "|".join(['bioenergy', 'fossil_coal', 'fossil_oil_gas', 'geothermal', 'hydropower', 'nuclear', 'solar_csp', 'solar_utility_pv', 'wind'])
    log:
        "logs/impute_{dataset}.log",
    conda:
        "../envs/shapes.yaml",
    shell:
        """
        python "{input.script}" main "{input.prepared}" "{input.shapes}" "{params.lifetimes}" "{params.delay}" {output.imputed} 2> {log}
        python "{input.script}" plot "{output.imputed}" "{output.plot}" 2> "{log}"
        """
