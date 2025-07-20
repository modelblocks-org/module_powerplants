Adjustment of active {{ snakemake.wildcards.category }} capacity of each country in {{ snakemake.wildcards.shapes }} to EIA statistics in {{ snakemake.params.year }}.

.. math::

    ActiveCapAdj_{ {{ snakemake.wildcards.category }} , {p}} = EIACap_{ {{ snakemake.wildcards.category }} , {{ snakemake.params.year }} } * \frac{ ActiveCap_{ {{ snakemake.wildcards.category }} ,{p}} }{\sum_{p} ActiveCap_{ {{ snakemake.wildcards.category }} ,{p}}}
