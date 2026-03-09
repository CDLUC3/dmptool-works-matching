import logging
from typing import TYPE_CHECKING

from dmpworks.batch.tasks import dataset_subset_task, download_source_task, transform_parquets_task
from dmpworks.batch.utils import s3_uri
from dmpworks.cli_utils import CrossrefMetadataTransformConfig, DatasetSubsetAWS
from dmpworks.transform.crossref_metadata import transform_crossref_metadata
from dmpworks.transform.dataset_subset import create_dataset_subset
from dmpworks.utils import run_process

if TYPE_CHECKING:
    import pathlib

log = logging.getLogger(__name__)

DATASET = "crossref_metadata"


def download(*, bucket_name: str, run_id: str, file_name: str, crossref_bucket_name: str):
    """Download Crossref Metadata from the Crossref Metadata requestor pays S3 bucket and upload to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: Unique ID to represent this run of the job.
        file_name: Name of the Crossref Metadata Public Datafile, e.g. April_2025_Public_Data_File_from_Crossref.tar.
        crossref_bucket_name: Name of the Crossref AWS S3 bucket.
    """
    with download_source_task(bucket_name, DATASET, run_id) as ctx:
        # Download archive
        run_process(
            [
                "s5cmd",
                "--request-payer",
                "requester",
                "cp",
                s3_uri(crossref_bucket_name, file_name),
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


def dataset_subset(
    *,
    bucket_name: str,
    run_id: str,
    ds_config: DatasetSubsetAWS = None,
):
    """Create a subset of Crossref Metadata.

    Args:
        bucket_name: the name of the S3 bucket for JOB I/O.
        run_id: a unique ID to represent this run of the job.
        ds_config: settings for creating the subset of works
    """
    with dataset_subset_task(
        bucket_name=bucket_name,
        dataset=DATASET,
        run_id=run_id,
        dataset_subset=ds_config,
    ) as ctx:
        create_dataset_subset(
            dataset="crossref-metadata",
            in_dir=ctx.download_dir,
            out_dir=ctx.subset_dir,
            institutions=ctx.institutions,
            dois=ctx.dois,
        )


def transform(
    *,
    bucket_name: str,
    run_id: str,
    config: CrossrefMetadataTransformConfig,
    use_subset: bool = False,
    log_level: int = logging.INFO,
):
    """Download Crossref Metadata from DMP Tool S3 bucket, transform to Parquet, and upload the result.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        config: configuration parameters.
        use_subset: whether to use a subset of the dataset or the full dataset.
        log_level: Python log level.
    """
    with transform_parquets_task(bucket_name, DATASET, run_id, use_subset=use_subset) as ctx:
        transform_crossref_metadata(
            in_dir=ctx.download_dir,
            out_dir=ctx.transform_dir,
            **vars(config),
            log_level=log_level,
        )
