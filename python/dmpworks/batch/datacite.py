import logging
import os
from typing import Optional

from cyclopts import App

from dmpworks.batch.tasks import dataset_subset_task, download_source_task, transform_parquets_task
from dmpworks.batch.utils import s3_uri
from dmpworks.cli_utils import DatasetSubset
from dmpworks.transform.cli import DataCiteConfig
from dmpworks.transform.datacite import transform_datacite
from dmpworks.transform.dataset_subset import create_dataset_subset
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmpworks.utils import copy_dict, fetch_datacite_aws_credentials, run_process

log = logging.getLogger(__name__)

DATASET = "datacite"
app = App(name="datacite", help="DataCite AWS Batch pipeline.")


@app.command(name="download")
def download_cmd(bucket_name: str, run_id: str, datacite_bucket_name: str):
    """Download DataCite from the DataCite S3 bucket and upload it to
    the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        datacite_bucket_name: the name of the DataCite AWS S3 bucket.
    """

    setup_multiprocessing_logging(logging.INFO)

    # Fetch DataCite AWS credentials
    access_key_id, secret_access_key, session_token = fetch_datacite_aws_credentials()
    env = os.environ.copy()
    env.update(
        {
            "AWS_ACCESS_KEY_ID": access_key_id,
            "AWS_SECRET_ACCESS_KEY": secret_access_key,
            "AWS_SESSION_TOKEN": session_token,
        }
    )

    # Download release
    with download_source_task(bucket_name, DATASET, run_id) as ctx:
        run_process(
            [
                "s5cmd",
                "cp",
                s3_uri(datacite_bucket_name, "dois/*"),
                f"{ctx.download_dir}/",
            ],
            env=env,
        )


@app.command(name="dataset-subset")
def dataset_subset_cmd(
    bucket_name: str,
    run_id: str,
    dataset_subset: DatasetSubset,
):
    """Create a subset of DataCite.

    Args:
        bucket_name: the name of the S3 bucket for JOB I/O.
        run_id: a unique ID to represent this run of the job.
        dataset_subset: settings for creating the subset of works
    """

    setup_multiprocessing_logging(logging.INFO)

    with dataset_subset_task(
        bucket_name=bucket_name,
        dataset=DATASET,
        run_id=run_id,
        dataset_subset=dataset_subset,
    ) as ctx:
        create_dataset_subset(
            dataset="datacite",
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
    config: Optional[DataCiteConfig] = None,
):
    """Download DataCite from the DMP Tool S3 bucket, transform it to
    Parquet format, and upload the results to same bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: optional configuration parameters.
    """

    config = DataCiteConfig() if config is None else config
    setup_multiprocessing_logging(logging.INFO)

    with transform_parquets_task(bucket_name, DATASET, run_id, use_subset=use_subset) as ctx:
        transform_datacite(
            in_dir=ctx.download_dir,
            out_dir=ctx.transform_dir,
            **copy_dict(vars(config), ["log_level"]),
        )


if __name__ == "__main__":
    app()
