import logging
from typing import Optional

from cyclopts import App

from dmpworks.batch.tasks import download_source_task, transform_parquets_task
from dmpworks.batch.utils import associate_elastic_ip, get_ec2_instance_info
from dmpworks.transform.cli import DataCiteConfig
from dmpworks.transform.datacite import transform_datacite
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmpworks.utils import copy_dict, run_process

log = logging.getLogger(__name__)

DATASET = "datacite"
app = App(name="datacite", help="DataCite AWS Batch pipeline.")


@app.command(name="download")
def download_cmd(bucket_name: str, run_id: str, allocation_id: str):
    """Download DataCite from the DataCite S3 bucket and upload it to
    the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        allocation_id: the Elastic IP allocation ID.
    """

    setup_multiprocessing_logging(logging.INFO)

    # Assign Elastic IP address to instance
    instance_id, region = get_ec2_instance_info()
    associate_elastic_ip(
        instance_id=instance_id,
        allocation_id=allocation_id,
        region_name=region,
    )

    # Download release
    with download_source_task(bucket_name, DATASET, run_id) as ctx:
        run_process(
            [
                "s5cmd",
                "--no-sign-request",
                "cp",
                "s3://datafile-beta/dois/*",
                f"{ctx.download_dir}/",
            ],
        )


@app.command(name="transform")
def transform_cmd(
    bucket_name: str,
    run_id: str,
    *,
    config: Optional[DataCiteConfig] = None,
):
    """Download DataCite from the DMP Tool S3 bucket, transform it to
    Parquet format, and upload the results to same bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        config: optional configuration parameters.
    """

    config = DataCiteConfig() if config is None else config
    setup_multiprocessing_logging(logging.INFO)

    with transform_parquets_task(bucket_name, DATASET, run_id) as ctx:
        transform_datacite(
            in_dir=ctx.download_dir,
            out_dir=ctx.transform_dir,
            **copy_dict(vars(config), ["log_level"]),
        )


if __name__ == "__main__":
    app()
