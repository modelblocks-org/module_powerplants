"""EIA international statistics download."""
import io
import sys
import zipfile
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")


def eia_bulk_download(url: str, output_path: str):
    """Download and save EIA's international statistics dataset."""
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip:
        with zip.open("INTL.txt") as source_file:
            with open(output_path, "wb") as target_file:
                target_file.write(source_file.read())


if __name__ == "__main__":
    eia_bulk_download(
        url=snakemake.params.url,
        output_path=snakemake.output.path
    )
