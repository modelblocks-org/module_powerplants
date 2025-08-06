"""A collection of heavier tests to run locally.

Useful for debugging or testing new features.

There are three degrees of testing:
- lightweight: MNE
- medium: MEX
- heavy: europe

!!!!!!!!!!!!!!!!!!!!!!!!IMPORTANT!!!!!!!!!!!!!!!!!!!!!!!!
- You normally want to run these tests one case at a time. E.g.:

    `pytest tests/local_test.py::test_name[param1-param2-param3]`

- Do not run this on Github's CI!
- The heavy case (europe) needs ~24 GB of memory (RAM + Swap).
"""

import subprocess
from pathlib import Path
from typing import Literal

import pytest

TEST_CATEGORIES = {
    "bioenergy",
    "fossil",
    "geothermal",
    "hydropower",
    "nuclear",
    "solar",
    "wind",
}

type Aggregation = Literal["aggregated", "disaggregated"]
type Adjustment = Literal["adjusted", "unadjusted"]


def build_request_all(
    case: str, categories: set[str], level: Aggregation, adjustment: Adjustment
):
    """Construct a request for the given categories."""
    return " ".join(
        [f"results/{case}/{level}/{adjustment}/{cat}.parquet" for cat in categories]
    )


@pytest.mark.parametrize("adjustment", ["unadjusted", "adjusted"])
@pytest.mark.parametrize("aggregation", ["aggregated"])
@pytest.mark.parametrize("case", ["MEX", "MNE", "europe"])
def test_full_run(
    user_path: Path, case: str, aggregation: Aggregation, adjustment: Adjustment
):
    """Test a full request of categories a given setup can give.

    NNN-aggregated-adjusted is often the most holistic case.
    """
    cats = TEST_CATEGORIES
    if aggregation == "disaggregated":
        # solar has no disaggregated case due to the lack of point-source rooftop PV.
        cats = cats - {"solar"}

    request = build_request_all(case, TEST_CATEGORIES, "aggregated", adjustment)

    assert subprocess.run(
        f"snakemake --use-conda --cores 4 --forceall {request}",
        shell=True,
        check=True,
        cwd=user_path.parent.parent,
    )
    assert subprocess.run(
        f"snakemake --use-conda --cores 4 {request} --report results/{case}/report.html",
        shell=True,
        check=True,
        cwd=user_path.parent.parent,
    )
    assert subprocess.run(
        f"snakemake --use-conda --cores 4 {request} --rulegraph | dot -Tpng > results/{case}/rulegraph.png",
        shell=True,
        check=True,
        cwd=user_path.parent.parent,
    )
