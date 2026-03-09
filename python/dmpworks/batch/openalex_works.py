import logging

from dmpworks.batch.tasks import dataset_subset_task, download_source_task, transform_parquets_task
from dmpworks.batch.utils import s3_uri
from dmpworks.cli_utils import DatasetSubsetAWS, OpenAlexWorksTransformConfig
from dmpworks.transform.dataset_subset import create_dataset_subset
from dmpworks.transform.openalex_works import transform_openalex_works
from dmpworks.utils import run_process

log = logging.getLogger(__name__)

DATASET = "openalex_works"


def download(*, bucket_name: str, run_id: str, openalex_bucket_name: str):
    """Download OpenAlex Works from the OpenAlex S3 bucket and upload to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        openalex_bucket_name: the name of the OpenAlex AWS S3 bucket.
    """
    with download_source_task(bucket_name, DATASET, run_id) as ctx:
        run_process(
            [
                "s5cmd",
                "--no-sign-request",
                "cp",
                s3_uri(openalex_bucket_name, "data/works/*"),
                f"{ctx.download_dir}/",
            ],
        )


def dataset_subset(
    *,
    bucket_name: str,
    run_id: str,
    ds_config: DatasetSubsetAWS = None,
):
    """Create a subset of OpenAlex Works.

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
            dataset="openalex-works",
            in_dir=ctx.download_dir,
            out_dir=ctx.subset_dir,
            institutions=ctx.institutions,
            dois=ctx.dois,
        )


def transform(
    *,
    bucket_name: str,
    run_id: str,
    config: OpenAlexWorksTransformConfig,
    use_subset: bool = False,
    log_level: int = logging.INFO,
):
    """Download OpenAlex Works from DMP Tool S3 bucket, transform to Parquet, and upload the result.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        config: configuration parameters.
        use_subset: whether to use a subset of the dataset or the full dataset.
        log_level: Python log level.
    """
    with transform_parquets_task(bucket_name, DATASET, run_id, use_subset=use_subset) as ctx:
        transform_openalex_works(
            in_dir=ctx.download_dir,
            out_dir=ctx.transform_dir,
            **vars(config),
            log_level=log_level,
        )
