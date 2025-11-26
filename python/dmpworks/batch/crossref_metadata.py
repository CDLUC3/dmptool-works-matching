import logging
import pathlib
from typing import Optional

from cyclopts import App

from dmpworks.batch.tasks import dataset_subset_task, download_source_task, transform_parquets_task
from dmpworks.cli_utils import DatasetSubset
from dmpworks.transform.cli import CrossrefMetadataConfig
from dmpworks.transform.crossref_metadata import transform_crossref_metadata
from dmpworks.transform.dataset_subset import create_dataset_subset
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmpworks.utils import copy_dict, run_process

log = logging.getLogger(__name__)

DATASET = "crossref_metadata"
app = App(name="crossref-metadata", help="Crossref Metadata AWS Batch pipeline.")


@app.command(name="download")
def download_cmd(bucket_name: str, run_id: str, file_name: str):
    """Download Crossref Metadata from the Crossref Metadata requestor pays S3
    bucket and upload it to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        file_name: the name of the Crossref Metadata Public Datafile,
        e.g. April_2025_Public_Data_File_from_Crossref.tar.
    """

    setup_multiprocessing_logging(logging.INFO)

    with download_source_task(bucket_name, DATASET, run_id) as ctx:
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


@app.command(name="dataset-subset")
def dataset_subset_cmd(
    bucket_name: str,
    run_id: str,
    dataset_subset: DatasetSubset = None,
):
    """Create a subset of Crossref Metadata.

    Args:
        bucket_name: the name of the S3 bucket for JOB I/O.
        run_id: a unique ID to represent this run of the job.
        dataset_subset: settings for creating the subset of institutions
    """

    setup_multiprocessing_logging(logging.INFO)

    with dataset_subset_task(
        bucket_name=bucket_name,
        dataset=DATASET,
        run_id=run_id,
        dataset_subset=dataset_subset,
    ) as ctx:
        create_dataset_subset(
            dataset="crossref-metadata",
            in_dir=ctx.download_dir,
            out_dir=ctx.subset_dir,
            institutions=ctx.institutions,
            dois=ctx.dois,
        )


@app.command(name="transform")
def transform_cmd(
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    *,
    config: Optional[CrossrefMetadataConfig] = None,
):
    """Download Crossref Metadata from the DMP Tool S3 bucket, transform it to
    Parquet format, and upload the results to same bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: optional configuration parameters.
    """

    config = CrossrefMetadataConfig() if config is None else config
    setup_multiprocessing_logging(logging.INFO)

    with transform_parquets_task(bucket_name, DATASET, run_id, use_subset=use_subset) as ctx:
        transform_crossref_metadata(
            in_dir=ctx.download_dir,
            out_dir=ctx.transform_dir,
            **copy_dict(vars(config), ["log_level"]),
        )


if __name__ == "__main__":
    app()
