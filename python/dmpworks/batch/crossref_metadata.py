import logging
import pathlib
from typing import Optional

from cyclopts import App

from dmpworks.batch.tasks import download_source_task, transform_parquets_task
from dmpworks.transform.cli import CrossrefMetadataConfig
from dmpworks.transform.crossref_metadata import transform_crossref_metadata
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmpworks.utils import copy_dict, run_process

log = logging.getLogger(__name__)

DATASET = "crossref_metadata"
app = App(name="crossref-metadata", help="Crossref Metadata AWS Batch pipeline.")


@app.command(name="download")
def download_cmd(bucket_name: str, task_id: str, file_name: str):
    """Download Crossref Metadata from the Crossref Metadata requestor pays S3
    bucket and upload it to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        task_id: a unique task ID.
        file_name: the name of the Crossref Metadata Public Datafile,
        e.g. April_2025_Public_Data_File_from_Crossref.tar.
    """

    setup_multiprocessing_logging(logging.INFO)

    with download_source_task(bucket_name, DATASET, task_id) as ctx:
        # Download archive
        run_process(
            [
                "s5cmd",
                "--request-payer",
                "requester",
                "cp",
                f"s3://api-snapshots-reqpays-crossref/{file_name}",
                f"{ctx.download_dir}/",
            ],
        )

        # Extract archive and cleanup
        # Untar it here because it is much faster to upload and download many
        # smaller files, rather than one large file.
        archive_path: pathlib.Path = ctx.download_dir / file_name
        run_process(
            ["tar", "-xvf", str(archive_path), "-C", str(ctx.download_dir), "--strip-components", "1"],
        )

        # Cleanup
        archive_path.unlink(missing_ok=True)


@app.command(name="transform")
def transform_cmd(
    bucket_name: str,
    task_id: str,
    *,
    config: Optional[CrossrefMetadataConfig] = None,
):
    """Download Crossref Metadata from the DMP Tool S3 bucket, transform it to
    Parquet format, and upload the results to same bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        task_id: a unique task ID.
        config: optional configuration parameters.
    """

    config = CrossrefMetadataConfig() if config is None else config
    setup_multiprocessing_logging(logging.INFO)

    with transform_parquets_task(bucket_name, DATASET, task_id) as ctx:
        transform_crossref_metadata(
            in_dir=ctx.download_dir,
            out_dir=ctx.transform_dir,
            **copy_dict(vars(config), ["log_level"]),
        )


if __name__ == "__main__":
    app()
