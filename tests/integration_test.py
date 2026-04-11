"""Set of standard Modelblocks tests.

PLEASE ENSURE THIS SET OF MINIMAL TESTS WORKS BEFORE PUBLISHING YOUR MODULE.
Contents may be updated in future template updates.
"""

import shutil
import subprocess
from pathlib import Path

import pytest
from clio_tools.data_module import ModuleInterface


@pytest.fixture(scope="module")
def module_path():
    """Parent directory of the project."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="module")
def integration_path(user_path: Path, module_path: Path):
    """Ensures the minimal integration test is ready."""
    integration_dir = Path(module_path / "tests/integration")
    if integration_dir.exists():
        # clean everything
        shutil.rmtree(integration_dir / "resources", ignore_errors=True)
        shutil.rmtree(integration_dir / "results/", ignore_errors=True)
    user_integ_dir = integration_dir / "resources/inputs/"
    files_to_copy = ["MNE/proxies/rooftop_pv.tif", "MNE/shapes.parquet"]
    for file in files_to_copy:
        destination_file = Path(user_integ_dir / file)
        destination_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(user_path / file, destination_file)
    return integration_dir


def test_interface_file(module_path):
    """The interfacing file should be correct."""
    assert ModuleInterface.from_yaml(module_path / "INTERFACE.yaml")


@pytest.mark.parametrize(
    "file",
    [
        "AUTHORS",
        "CITATION.cff",
        "INTERFACE.yaml",
        "LICENSE",
        "README.md",
        "config/config.yaml",
        "workflow/internal/config.schema.yaml",
        "tests/integration/Snakefile",
    ],
)
def test_standard_file_existance(module_path, file):
    """Check that a minimal set of files used for documentation are present."""
    assert Path(module_path / file).exists()


def test_snakemake_all_failure(module_path):
    """The snakemake 'all' rule should return an error by default."""
    process = subprocess.run(
        "snakemake --cores 1", shell=True, cwd=module_path, capture_output=True
    )
    assert "INVALID (missing locally)" in str(process.stderr)


def test_snakemake_integration_testing(integration_path):
    """Run a light-weight test simulating someone using this module."""
    assert subprocess.run(
        "snakemake --use-conda --cores 1", shell=True, check=True, cwd=integration_path
    )
