import logging
from typing import Optional

from cyclopts import App

from dmpworks.batch.tasks import dataset_subset_task, download_source_task, transform_parquets_task
from dmpworks.cli_utils import DatasetSubsetInstitution, Institutions
from dmpworks.transform.cli import OpenAlexWorksConfig
from dmpworks.transform.dataset_subset import create_dataset_subset
from dmpworks.transform.openalex_works import transform_openalex_works
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmpworks.utils import copy_dict, run_process

log = logging.getLogger(__name__)

DATASET = "openalex_works"
app = App(name="openalex-works", help="OpenAlex Works AWS Batch pipeline.")


@app.command(name="download")
def download_cmd(bucket_name: str, run_id: str):
    """Download OpenAlex Works from the OpenAlex S3 bucket and upload it to
    the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
    """

    setup_multiprocessing_logging(logging.INFO)

    with download_source_task(bucket_name, DATASET, run_id) as ctx:
        run_process(
            [
                "s5cmd",
                "--no-sign-request",
                "cp",
                "s3://openalex/data/works/*",
                f"{ctx.download_dir}/",
            ],
        )


@app.command(name="dataset-subset")
def dataset_subset(
    bucket_name: str,
    run_id: str,
    institutions: Institutions = None,
):
    """Create a subset of DataCite.

    Args:
        bucket_name: the name of the S3 bucket for JOB I/O.
        run_id: a unique ID to represent this run of the job.
        institutions: a list of the institutions to include.
    """

    setup_multiprocessing_logging(logging.INFO)

    with dataset_subset_task(bucket_name, DATASET, run_id) as ctx:
        create_dataset_subset(
            dataset="openalex-works",
            in_dir=ctx.download_dir,
            out_dir=ctx.subset_dir,
            institutions=DatasetSubsetInstitution.parse(institutions),
        )


@app.command(name="transform")
def transform_cmd(
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    *,
    config: Optional[OpenAlexWorksConfig] = None,
):
    """Download OpenAlex Works from the DMP Tool S3 bucket, transform it to
    Parquet format, and upload the results to same bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: optional configuration parameters.
    """

    config = OpenAlexWorksConfig() if config is None else config
    setup_multiprocessing_logging(logging.INFO)

    with transform_parquets_task(bucket_name, DATASET, run_id, use_subset=use_subset) as ctx:
        transform_openalex_works(
            in_dir=ctx.download_dir,
            out_dir=ctx.transform_dir,
            **copy_dict(vars(config), ["log_level"]),
        )


if __name__ == "__main__":
    app()
