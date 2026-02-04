import logging
from typing import Optional

from cyclopts import App

from dmpworks.batch.tasks import transform_parquets_task
from dmpworks.transform.cli import DMPsConfig
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmpworks.utils import copy_dict
from dmpworks.transform.dmps import transform_dmps

log = logging.getLogger(__name__)

DATASET = "dmps"
app = App(name="dmps", help="DMPs AWS Batch pipeline.")


@app.command(name="transform")
def transform_cmd(
    bucket_name: str,
    run_id: str,
    *,
    config: Optional[DMPsConfig] = None,
):
    """Transform the DMP data file into Parquet format, and upload the results
    to same bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        config: optional configuration parameters.
    """

    config = DMPsConfig() if config is None else config
    setup_multiprocessing_logging(logging.INFO)

    with transform_parquets_task(bucket_name, DATASET, run_id) as ctx:
        transform_dmps(
            in_dir=ctx.download_dir,
            out_dir=ctx.transform_dir,
            **copy_dict(vars(config), ["log_level"]),
        )


if __name__ == "__main__":
    app()
