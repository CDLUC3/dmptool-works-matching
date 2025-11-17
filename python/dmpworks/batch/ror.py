import gzip
import logging
import pathlib
import shutil
import zipfile
from typing import Optional

import pooch
from cyclopts import App

from dmpworks.batch.tasks import download_source_task, transform_parquets_task
from dmpworks.transform.ror import transform_ror
from dmpworks.transform.utils_file import setup_multiprocessing_logging

log = logging.getLogger(__name__)

DATASET = "ror"
app = App(name="ror", help="ROR AWS Batch pipeline.")


def extract_ror(file_path: pathlib.Path) -> pathlib.Path:
    with zipfile.ZipFile(file_path, 'r') as file:
        # Find ROR v2 JSON file in ZIP file
        log.info(f"Files in archive: {file_path}")
        json_file_name = None
        for name in file.namelist():
            log.info(name)
            if name.lower().endswith("schema_v2.json"):
                log.info(f"Found ROR v2 JSON file: {name}")
                json_file_name = name
                break

        if json_file_name is None:
            msg = f"Could not find ROR V2 JSON file: {name}"
            log.error(msg)
            raise FileNotFoundError(msg)

        # Extract it
        json_path = file_path.parent / json_file_name
        file.extract(member=json_file_name, path=file_path.parent)

    return json_path


def gzip_file(in_file: pathlib.Path, out_file: pathlib.Path):
    with open(in_file, "rb") as f_in:
        with gzip.open(out_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


@app.command(name="download")
def download_cmd(bucket_name: str, run_id: str, download_url: str, hash: Optional[str] = None):
    """Download ROR from the Zenodo and upload it to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        download_url: the Zenodo download URL for a specific ROR ID, e.g. https://zenodo.org/records/15731450/files/v1.67-2025-06-24-ror-data.zip?download=1.
        hash: the MD5 sum of the file.
    """

    setup_multiprocessing_logging(logging.INFO)

    with download_source_task(bucket_name, DATASET, run_id) as ctx:
        # Download file
        zip_path = pooch.retrieve(
            url=download_url,
            known_hash=hash,
            path=ctx.download_dir,
            progressbar=True,
        )
        zip_path = pathlib.Path(zip_path)

        # Extract the ROR v2 JSON file
        json_path = extract_ror(zip_path)

        # Gzip it
        gzip_path = json_path.with_name(json_path.name + ".gz")
        gzip_file(json_path, gzip_path)

        # Cleanup files we no longer need
        zip_path.unlink(missing_ok=True)
        json_path.unlink(missing_ok=True)


@app.command(name="transform")
def transform_cmd(bucket_name: str, run_id: str, file_name: str):
    """Download ROR from the DMP Tool S3 bucket, transform it to
    Parquet format, and upload the results to same bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        file_name: the name of the gzipped ROR V2 JSON file.
    """

    setup_multiprocessing_logging(logging.INFO)

    with transform_parquets_task(bucket_name, DATASET, run_id) as ctx:
        json_file = ctx.download_dir / file_name
        if not json_file.is_file():
            msg = f"Could not find file: {json_file}"
            log.error(msg)
            raise FileNotFoundError(msg)

        transform_ror(
            json_file=json_file,
            out_dir=ctx.transform_dir,
        )


if __name__ == "__main__":
    app()
