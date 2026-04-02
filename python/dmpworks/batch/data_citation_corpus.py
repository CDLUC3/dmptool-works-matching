import logging
import pathlib

import pooch

from dmpworks.batch.tasks import download_source_task
from dmpworks.utils import extract_zip_to_gzip

log = logging.getLogger(__name__)

DATASET = "data-citation-corpus"


def download(
    *,
    bucket_name: str,
    run_id: str,
    download_url: str,
    file_hash: str | None = None,
):
    """Download Data Citation Corpus JSON zip from Zenodo, extract and gzip JSON files, upload to S3.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        download_url: the Zenodo download URL for the DCC JSON zip file.
        file_hash: optional expected MD5 checksum (md5:... format).
    """
    with download_source_task(bucket_name, DATASET, run_id) as ctx:
        # Download file
        zip_path = pooch.retrieve(
            url=download_url,
            known_hash=file_hash,
            path=ctx.download_dir,
            progressbar=True,
        )
        zip_path = pathlib.Path(zip_path)

        # Extract JSON files and gzip each one
        file_paths = extract_zip_to_gzip(zip_path)
        if len(file_paths) == 0:
            raise ValueError(f"No JSON files found in archive: {zip_path}")

        # Remove zip
        zip_path.unlink(missing_ok=True)
