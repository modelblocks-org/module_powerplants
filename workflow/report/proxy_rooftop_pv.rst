Proxy of rooftop PV capacity for countries in {{ snakemake.wildcards.shapes }}.
It is assumed that 'missing' solar capacity represents rooftop PV capacity.

.. math::

    RooftopPVCap_{country, RefYear} = EIACap_{solar, RefYear} - LargeSolarCap_{country, RefYear}

If negative, rooftop PV capacity is assumed to be zero.
