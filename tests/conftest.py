"""Shared test fixtures."""

import shutil
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

import pytest

TEST_FILES = "https://www.dropbox.com/scl/fi/2i36vghcjr837yn1moaos/test_resources_user.zip?rlkey=sgostxujaqxfo74pigei72rfv&st=ljhm3soc&dl=1"


@pytest.fixture(scope="session")
def user_path() -> Path:
    """Download and unzip test files."""
    user_dir = Path("resources/user/")
    # If test suite has been downloaded, assume everything is OK.
    # Otherwise, cleanup and re-download.
    if not Path(user_dir / "test_suite.zip").exists():
        shutil.rmtree(user_dir, ignore_errors=True)
        breakpoint()
        Path(user_dir).mkdir(parents=True, exist_ok=True)
        test_zip = Path(user_dir / "test_suite.zip")
        urlretrieve(TEST_FILES, test_zip)
        with zipfile.ZipFile(test_zip, "r") as zfile:
            zfile.extractall(user_dir)
    return user_dir
