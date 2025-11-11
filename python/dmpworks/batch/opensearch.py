import logging
from typing import Optional

from cyclopts import App

from dmpworks.batch.utils import (
    data_path,
    download_file_from_s3,
    download_files_from_s3,
    local_path,
    s3_uri,
    upload_file_to_s3,
)
from dmpworks.cli_utils import DateString, LogLevel
from dmpworks.dmsp.related_works import merge_related_works
from dmpworks.opensearch.cli import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.opensearch.dmp_works import dmp_works_search
from dmpworks.opensearch.enrich_dmps import enrich_dmps
from dmpworks.opensearch.index import create_index
from dmpworks.opensearch.sync_dmps import sync_dmps
from dmpworks.opensearch.sync_works import sync_works
from dmpworks.opensearch.utils import Date, make_opensearch_client
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmsp.related_works import MergeRelatedWorksConfig

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
    export_date: DateString,
    index_name: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Sync exported works in Parquet format with OpenSearch.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        export_date: a unique task ID.
        index_name: the OpenSearch index name.
        client_config: the OpenSearch client config settings.
        sync_config: the OpenSearch sync config settings.
        log_level: Python log level.
    """

    client_config = OpenSearchClientConfig() if client_config is None else client_config
    sync_config = OpenSearchSyncConfig() if sync_config is None else sync_config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)

    # Download Parquet files from S3
    export_dir = local_path(SQLMESH_DIR, export_date, "export")
    source_uri = s3_uri(bucket_name, SQLMESH_DIR, export_date, "export")
    download_files_from_s3(f"{source_uri}*", export_dir)

    # Create index (if it doesn't exist already)
    client = make_opensearch_client(client_config)
    create_index(client, index_name, WORKS_MAPPING_FILE)

    # Run process
    sync_works(index_name, export_dir, client_config, sync_config, level)


@app.command(name="sync-dmps")
def sync_dmps_cmd(
    bucket_name: str,
    export_date: DateString,
    index_name: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Sync dmps in Parquet format with OpenSearch.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        export_date: a unique task ID.
        index_name: the OpenSearch index name.
        client_config: the OpenSearch client config settings.
        sync_config: the OpenSearch sync config settings.
        log_level: Python log level.
    """

    client_config = OpenSearchClientConfig() if client_config is None else client_config
    sync_config = OpenSearchSyncConfig() if sync_config is None else sync_config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)

    # Download Parquet files from S3
    export_dir = local_path(DMPS_SOURCE_DIR, export_date)
    source_uri = s3_uri(bucket_name, DMPS_SOURCE_DIR, export_date)
    download_files_from_s3(f"{source_uri}*", export_dir)

    # Create index (if it doesn't exist already)
    client = make_opensearch_client(client_config)
    create_index(client, index_name, DMPS_MAPPING_FILE)

    # Sync dmps
    sync_dmps(index_name, export_dir, client_config, sync_config, level)


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
    dmp_index_name: str,
    works_index_name: str,
    bucket_name: str,
    export_date: DateString,
    scroll_time: str = "60m",
    batch_size: int = 250,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    parallel_search: bool = False,
    include_named_queries_score: bool = True,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
    client_config: Optional[OpenSearchClientConfig] = None,
    dmp_inst_name: Optional[str] = None,
    dmp_inst_ror: Optional[str] = None,
    start_date: Date = None,
    end_date: Date = None,
    log_level: LogLevel = "INFO",
):
    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    out_file = data_path() / MATCHES_FILE_NAME
    dmp_works_search(
        dmp_index_name,
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
        dmp_inst_name=dmp_inst_name,
        dmp_inst_ror=dmp_inst_ror,
        start_date=start_date,
        end_date=end_date,
    )

    # Upload to s3
    target_uri = s3_uri(bucket_name, DMP_WORKS_SEARCH_PATH, export_date, MATCHES_FILE_NAME)
    upload_file_to_s3(out_file, target_uri)


@app.command(name="merge-related-works")
def merge_related_works_cmd(
    bucket_name: str,
    export_date: DateString,
    config: MergeRelatedWorksConfig,
    log_level: LogLevel = "INFO",
):
    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)

    # Download data from s3
    source_uri = s3_uri(bucket_name, DMP_WORKS_SEARCH_PATH, export_date, MATCHES_FILE_NAME)
    matches_path = local_path(DMP_WORKS_SEARCH_PATH, export_date, MATCHES_FILE_NAME)
    download_file_from_s3(source_uri, matches_path)

    # Upsert data
    merge_related_works(
        config.matches_path,
        config.mysql_host,
        config.mysql_tcp_port,
        config.mysql_user,
        config.mysql_database,
        config.mysql_pwd,
        batch_size=config.batch_size,
    )


if __name__ == "__main__":
    app()
