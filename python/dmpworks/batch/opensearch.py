import logging
import shutil
from typing import Annotated, Optional

from cyclopts import App, Parameter

from dmpworks.batch.dataset_subset import load_dois, load_institutions
from dmpworks.batch.utils import (
    download_file_from_s3,
    download_files_from_s3,
    local_path,
    s3_uri,
    upload_file_to_s3,
)
from dmpworks.cli_utils import DMPSubset, LogLevel
from dmpworks.dmsp.related_works import merge_related_works
from dmpworks.opensearch.cli import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.opensearch.dmp_works import dmp_works_search
from dmpworks.opensearch.enrich_dmps import enrich_dmps
from dmpworks.opensearch.index import create_index
from dmpworks.opensearch.sync_dmps import sync_dmps
from dmpworks.opensearch.sync_works import sync_works
from dmpworks.opensearch.utils import Date, make_opensearch_client
from dmpworks.transform.utils_file import setup_multiprocessing_logging

log = logging.getLogger(__name__)

SQLMESH_DIR = "sqlmesh"
DMPS_SOURCE_DIR = "dmps"

WORKS_MAPPING_FILE = "works-mapping.json"
DMPS_MAPPING_FILE = "dmps-mapping.json"

MATCHES_FILE_NAME = "matches.jsonl"
DMP_WORKS_SEARCH_PATH = "dmp-works-search"

DATASET = "opensearch"
app = App(name="opensearch", help="OpenSearch AWS Batch pipeline.")


@app.command(name="sync-works")
def sync_works_cmd(
    bucket_name: str,
    run_id: str,
    index_name: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Sync exported works in Parquet format with OpenSearch.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        index_name: the OpenSearch index name.
        client_config: the OpenSearch client config settings.
        sync_config: the OpenSearch sync config settings.
        log_level: Python log level.
    """

    client_config = OpenSearchClientConfig() if client_config is None else client_config
    sync_config = OpenSearchSyncConfig() if sync_config is None else sync_config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)

    export_dir = local_path(SQLMESH_DIR, run_id, "export")
    try:
        # Download Parquet files from S3
        source_uri = s3_uri(bucket_name, SQLMESH_DIR, run_id, "export/*")
        download_files_from_s3(source_uri, export_dir)

        # Create index (if it doesn't exist already)
        client = make_opensearch_client(client_config)
        create_index(client, index_name, WORKS_MAPPING_FILE)

        # Run process
        sync_works(index_name, export_dir, client_config, sync_config, level)
    finally:
        shutil.rmtree(export_dir, ignore_errors=True)


@app.command(name="sync-dmps")
def sync_dmps_cmd(
    bucket_name: str,
    run_id: str,
    index_name: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Sync dmps in Parquet format with OpenSearch.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        index_name: the OpenSearch index name.
        client_config: the OpenSearch client config settings.
        sync_config: the OpenSearch sync config settings.
        log_level: Python log level.
    """

    client_config = OpenSearchClientConfig() if client_config is None else client_config
    sync_config = OpenSearchSyncConfig() if sync_config is None else sync_config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)

    export_dir = local_path(DMPS_SOURCE_DIR, run_id)
    try:
        # Download Parquet files from S3
        source_uri = s3_uri(bucket_name, DMPS_SOURCE_DIR, f"{run_id}/*")
        download_files_from_s3(source_uri, export_dir)

        # Create index (if it doesn't exist already)
        client = make_opensearch_client(client_config)
        create_index(client, index_name, DMPS_MAPPING_FILE)

        # Sync dmps
        sync_dmps(index_name, export_dir, client_config, sync_config, level)
    finally:
        shutil.rmtree(export_dir)


@app.command(name="enrich-dmps")
def enrich_dmps_cmd(
    index_name: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Enrich dmps in the OpenSearch DMPs index.

    Args:
        index_name: the OpenSearch index name.
        client_config: the OpenSearch client config settings.
        log_level: Python log level.
    """

    client_config = OpenSearchClientConfig() if client_config is None else client_config
    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    enrich_dmps(index_name, client_config)


@app.command(name="dmp-works-search")
def dmp_works_search_cmd(
    bucket_name: str,
    run_id: str,
    dmps_index_name: str,
    works_index_name: str,
    scroll_time: str = "60m",
    batch_size: int = 250,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    parallel_search: bool = False,
    include_named_queries_score: bool = True,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
    client_config: Optional[OpenSearchClientConfig] = None,
    dmp_subset: DMPSubset = None,
    start_date: Date = None,
    end_date: Date = None,
    log_level: LogLevel = "INFO",
):
    """DMP Works Search.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        dmps_index_name: the name of the DMP index in OpenSearch.
        works_index_name: the name of the works index in OpenSearch.
        scroll_time: the length of time the OpenSearch scroll used to iterate
        through DMPs will stay active. Set it to a value greater than the length
        of this process.
        batch_size: the number of searches run in parallel when include_scores=False.
        max_results: the maximum number of matches per DMP.
        project_end_buffer_years: the number of years to add to the end of the
        project end date when searching for works.
        parallel_search: whether to run parallel search or not.
        include_named_queries_score: whether to include scores for subqueries.
        max_concurrent_searches: the maximum number of concurrent searches.
        max_concurrent_shard_requests: the maximum number of shards searched per node.
        client_config: OpenSearch client settings.
        dmp_subset: settings for including a subset of DMPs.
        start_date: return DMPs with project start dates on or after this date.
        end_date: return DMPs with project start dates on before this date.
        log_level: Python log level.
    """

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    out_file = local_path(DMP_WORKS_SEARCH_PATH, run_id, MATCHES_FILE_NAME)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    meta_dir = local_path(DMP_WORKS_SEARCH_PATH, run_id, "meta")
    meta_dir.mkdir(parents=True, exist_ok=True)

    # Load subset
    use_subset = dmp_subset is not None and dmp_subset.enable
    logging.info(f"use_subset: {use_subset}")

    # Download institutions
    institutions = None
    if use_subset and dmp_subset.institutions_s3_path is not None:
        institutions_uri = s3_uri(bucket_name, dmp_subset.institutions_s3_path)
        institutions_path = meta_dir / "institutions.json"
        download_file_from_s3(institutions_uri, institutions_path)
        institutions = load_institutions(institutions_path)
        logging.info(f"institutions: {institutions}")

    # Download DOIs
    dois = None
    if use_subset and dmp_subset.dois_s3_path is not None:
        dois_uri = s3_uri(bucket_name, dmp_subset.dois_s3_path)
        dois_path = meta_dir / "dois.json"
        download_file_from_s3(dois_uri, dois_path)
        dois = load_dois(dois_path)
        logging.info(f"dois: {dois}")

    try:
        dmp_works_search(
            dmps_index_name,
            works_index_name,
            out_file,
            client_config,
            scroll_time=scroll_time,
            batch_size=batch_size,
            max_results=max_results,
            project_end_buffer_years=project_end_buffer_years,
            parallel_search=parallel_search,
            include_named_queries_score=include_named_queries_score,
            max_concurrent_searches=max_concurrent_searches,
            max_concurrent_shard_requests=max_concurrent_shard_requests,
            institutions=institutions,
            dois=dois,
            start_date=start_date,
            end_date=end_date,
        )

        # Upload to s3
        target_uri = s3_uri(bucket_name, DMP_WORKS_SEARCH_PATH, run_id, MATCHES_FILE_NAME)
        upload_file_to_s3(out_file, target_uri)
    finally:
        out_file.unlink(missing_ok=True)


@app.command(name="merge-related-works")
def merge_related_works_cmd(
    bucket_name: str,
    run_id: str,
    mysql_host: Annotated[
        str,
        Parameter(
            env_var="MYSQL_HOST",
            help="MySQL hostname",
        ),
    ],
    mysql_tcp_port: Annotated[
        int,
        Parameter(
            env_var="MYSQL_TCP_PORT",
            help="MySQL port",
        ),
    ],
    mysql_user: Annotated[
        str,
        Parameter(
            env_var="MYSQL_USER",
            help="MySQL user name",
        ),
    ],
    mysql_database: Annotated[
        str,
        Parameter(
            env_var="MYSQL_DATABASE",
            help="MySQL database name",
        ),
    ],
    mysql_pwd: Annotated[
        str,
        Parameter(
            env_var="MYSQL_PWD",
            help="MySQL password",
        ),
    ],
    batch_size: int = 1000,
    log_level: LogLevel = "INFO",
):
    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)

    matches_path = local_path(DMP_WORKS_SEARCH_PATH, run_id, MATCHES_FILE_NAME)
    try:
        # Download data from s3
        source_uri = s3_uri(bucket_name, DMP_WORKS_SEARCH_PATH, run_id, MATCHES_FILE_NAME)
        download_file_from_s3(source_uri, matches_path)

        # Upsert data
        merge_related_works(
            matches_path,
            mysql_host,
            mysql_tcp_port,
            mysql_user,
            mysql_database,
            mysql_pwd,
            batch_size=batch_size,
        )
    finally:
        matches_path.unlink(missing_ok=True)


if __name__ == "__main__":
    app()
