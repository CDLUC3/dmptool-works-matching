import logging
import os
import pathlib
from dataclasses import dataclass

from cyclopts import App

from dmpworks.batch.utils import download_files_from_s3, local_path, s3_uri, upload_files_to_s3
from dmpworks.cli_utils import DateString, LogLevel
from dmpworks.sql.commands import run_plan
from dmpworks.transform.utils_file import setup_multiprocessing_logging

log = logging.getLogger(__name__)

DATASET = "sqlmesh"
app = App(name="sqlmesh", help="SQLMesh AWS Batch pipeline.")


@dataclass
class ReleaseDates:
    openalex_works: DateString
    openalex_funders: DateString
    datacite: DateString
    crossref_metadata: DateString
    ror: DateString


@app.command(name="plan")
def plan(
    bucket_name: str,
    run_id: str,
    release_dates: ReleaseDates,
    log_level: LogLevel = "INFO",
):
    """

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        release_dates: the release dates of each dataset.
        log_level: Python log level.
    """

    setup_multiprocessing_logging(logging.getLevelName(log_level))

    # Download Parquet files for each dataset from S3.
    datasets = [
        ("openalex_works", release_dates.openalex_works),
        ("openalex_funders", release_dates.openalex_funders),
        ("crossref_metadata", release_dates.crossref_metadata),
        ("datacite", release_dates.datacite),
        ("ror", release_dates.ror),
    ]
    for dataset, release_date in datasets:
        transform_dir = local_path(dataset, release_date, "transform")
        target_uri = s3_uri(bucket_name, dataset, release_date, "transform/*")
        download_files_from_s3(target_uri, transform_dir)

    # Configure SQL Mesh environment
    sqlmesh_data_dir = pathlib.Path("/data") / "sqlmesh" / run_id
    duckdb_dir = sqlmesh_data_dir / "duckdb" / "db.db"
    export_dir = sqlmesh_data_dir / "export"
    duckdb_dir.parent.mkdir(parents=True, exist_ok=True)
    export_dir.mkdir(parents=True, exist_ok=True)
    os.environ["SQLMESH__GATEWAYS__DUCKDB__CONNECTION__DATABASE"] = str(duckdb_dir)
    for dataset, release_date in datasets:
        parquet_path = local_path(dataset, release_date, "transform") / "parquets"
        os.environ[f"SQLMESH__VARIABLES__{dataset.upper()}_PATH"] = str(parquet_path)
    os.environ["SQLMESH__VARIABLES__EXPORT_PATH"] = str(export_dir)

    # Run SQL Mesh
    run_plan()

    # Upload exported Parquet files
    sql_mesh_s3_uri = f"s3://{bucket_name}/sqlmesh/{run_id}/"
    upload_files_to_s3(sqlmesh_data_dir, sql_mesh_s3_uri, "*")


if __name__ == "__main__":
    app()
