"""Online zip file extractor."""

import io
import zipfile
from contextlib import closing
from pathlib import Path

import click
import requests
from requests.exceptions import RequestException

CHUNK_SIZE = 1 << 16  # ~64 KiB


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("url")
@click.argument("zip_file_path")
@click.argument("output_path", type=click.Path(path_type=Path))
def download(url: str, zip_file_path: str, output_path: Path) -> None:
    """Temporarily download a ZIP file and extract one file."""
    try:
        with closing(requests.get(url, stream=True, timeout=30)) as response:
            response.raise_for_status()

            # Stream the ZIP into memory to avoid issues if size > RAM.
            buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:  # filter out keep-alives
                    buffer.write(chunk)

            buffer.seek(0)
            with zipfile.ZipFile(buffer) as z:
                if zip_file_path not in z.namelist():
                    raise click.ClickException(f"Error: '{zip_file_path}' not found in archive.")

                output_path.parent.mkdir(parents=True, exist_ok=True)
                with z.open(zip_file_path) as src, output_path.open("wb") as dst:
                    for chunk in iter(lambda: src.read(CHUNK_SIZE), b""):
                        dst.write(chunk)

    except (RequestException, zipfile.BadZipFile) as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    download()
