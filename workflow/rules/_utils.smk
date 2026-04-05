"""Collection of auxiliary functions for this module."""


def additional_config_validation():
    """Ensures technology mapping and lifetime-related names match."""
    lifetime_set = set(config["imputation"]["lifetime_years"].keys())
    mismatch = lifetime_set ^ set(config["imputation"]["retirement_delay_years"])
    if mismatch:
        raise ValueError(
            f"Technologies in lifetimes and retirement delays mismatch: {mismatch}."
        )
    # Check category technology names
    tech_map_set = set()
    for cat in ["bioenergy", "geothermal", "hydropower", "nuclear", "solar", "wind"]:
        tech_map_set |= set(config["category"][cat]["technology_mapping"].values())
    for fossil_cat in ["coal", "oil_gas"]:
        tech_map_set |= set(
            config["category"]["fossil"]["technology_mapping"][fossil_cat].values()
        )
    mismatch = lifetime_set ^ tech_map_set
    if mismatch:
        raise ValueError(
            f"Technology mapping does not match lifetime technologies for {mismatch}"
        )


def get_excluded_powerplant_ids(category):
    """Handle cases where the naming in /<results>/.../disaggregated and configuration files mismatch.

    These are categories that include technologies poorly tracked at individual level.
    Proxying processes are necessary, and /disaggregated/ uses a different name to deter
    improper handling.
    """
    if category == "large_solar":
        config_cat = "solar"
    else:
        config_cat = category
    return config["category"][config_cat].get("excluded_ids", [])


def get_technology_mapping(filename: str):
    if "solar" in filename:
        mapping = config["category"]["solar"]["technology_mapping"]
    elif "fossil" in filename:
        tech = filename.removeprefix("fossil_").removesuffix(".parquet")
        mapping = dict()
        for tech in ["coal", "oil_gas"]:
            mapping |= config["category"]["fossil"]["technology_mapping"][tech]
    else:
        mapping = config["category"][filename]["technology_mapping"]
    return mapping


def get_files_to_combine(shapes, category):
    """Produce a list of subcategory files to combine.

    Will also append imputed data files if present.
    """
    to_combine = []
    if category == "large_solar":
        to_combine += [
            f"<resources>/automatic/shapes/{shapes}/imputed/solar_utility_pv.parquet",
            f"<resources>/automatic/shapes/{shapes}/imputed/solar_csp.parquet",
        ]
    elif category == "fossil":
        to_combine += [
            f"<resources>/automatic/shapes/{shapes}/imputed/fossil_coal.parquet",
            f"<resources>/automatic/shapes/{shapes}/imputed/fossil_oil_gas.parquet",
        ]
    else:
        to_combine.append(f"<resources>/automatic/shapes/{shapes}/imputed/{category}.parquet")

    user_path = f"<resources>/user/{shapes}/impute/{category}.parquet"
    if exists(user_path):
        to_combine.append(user_path)
    return to_combine
