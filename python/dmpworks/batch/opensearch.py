import logging
import shutil

import pendulum
import pymysql.cursors

from dmpworks.batch.utils import (
    download_file_from_s3,
    download_files_from_s3,
    local_path,
    s3_uri,
    upload_files_to_s3,
)
from dmpworks.cli_utils import DMPSubsetAWS, LogLevel, MySQLConfig, OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.dataset_subset import load_dois, load_institutions

log = logging.getLogger(__name__)

SQLMESH_DIR = "sqlmesh"
MATCHES_DIR = "matches"
DMP_WORKS_SEARCH_DIR = "dmp-works-search"
META_DIR = "meta"
DATASET = "opensearch"


def load_dmp_subset_from_s3(
    bucket_name: str,
    dmp_subset: DMPSubsetAWS | None,
    meta_dir,
):
    """Download and load DMP subset institutions and DOIs from S3.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        dmp_subset: DMP subset configuration.
        meta_dir: Local directory to download files into.

    Returns:
        tuple[list | None, list[str] | None]: Loaded institutions and DOIs, or None if not configured.
    """
    use_subset = dmp_subset is not None and dmp_subset.enable
    institutions = None
    dois = None

    if use_subset and dmp_subset.institutions_s3_path is not None:
        institutions_uri = s3_uri(bucket_name, dmp_subset.institutions_s3_path)
        institutions_path = meta_dir / "institutions.json"
        download_file_from_s3(institutions_uri, institutions_path)
        institutions = load_institutions(institutions_path)
        logging.info(f"institutions: {institutions}")

    if use_subset and dmp_subset.dois_s3_path is not None:
        dois_uri = s3_uri(bucket_name, dmp_subset.dois_s3_path)
        dois_path = meta_dir / "dois.json"
        download_file_from_s3(dois_uri, dois_path)
        dois = load_dois(dois_path)
        logging.info(f"dois: {dois}")

    return institutions, dois


def sync_works_cmd(
    *,
    bucket_name: str,
    run_id: str,
    index_name: str,
    client_config: OpenSearchClientConfig,
    sync_config: OpenSearchSyncConfig,
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
    from dmpworks.opensearch.sync_works import sync_works  # noqa: PLC0415

    level = logging.getLevelName(log_level)

    works_index_export = local_path(SQLMESH_DIR, run_id, "works_index_export")
    doi_state_export = local_path(SQLMESH_DIR, run_id, "doi_state_export")
    try:
        # Download Works Index Parquet files from S3
        works_index_source_uri = s3_uri(bucket_name, SQLMESH_DIR, run_id, "works_index_export/*")
        download_files_from_s3(works_index_source_uri, works_index_export)

        # Download DOI State Parquet files from S3
        doi_state_source_uri = s3_uri(bucket_name, SQLMESH_DIR, run_id, "doi_state_export/*")
        download_files_from_s3(doi_state_source_uri, doi_state_export)

        # Run process
        sync_works(
            index_name=index_name,
            works_index_export=works_index_export,
            doi_state_export=doi_state_export,
            run_id=run_id,
            client_config=client_config,
            sync_config=sync_config,
            log_level=level,
        )
    finally:
        shutil.rmtree(works_index_export, ignore_errors=True)
        shutil.rmtree(doi_state_export, ignore_errors=True)


def sync_dmps_cmd(
    *,
    bucket_name: str,
    index_name: str,
    client_config: OpenSearchClientConfig,
    mysql_config: MySQLConfig,
    dmp_subset: DMPSubsetAWS | None = None,
):
    """Sync DMPs from MySQL into the OpenSearch DMPs index, with optional subset filtering.

    Downloads institution and DOI subset files from S3 when configured, then syncs
    only the matching DMPs.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        index_name: the OpenSearch DMPs index name.
        client_config: the OpenSearch client config settings.
        mysql_config: MySQL connection configuration.
        dmp_subset: settings for including a subset of DMPs.
    """
    from dmpworks.opensearch.sync_dmps import sync_dmps  # noqa: PLC0415

    logging.getLogger("opensearch").setLevel(logging.WARNING)

    meta_dir = local_path(META_DIR)
    meta_dir.mkdir(parents=True, exist_ok=True)
    try:
        institutions, dois = load_dmp_subset_from_s3(bucket_name, dmp_subset, meta_dir)
        sync_dmps(
            index_name,
            mysql_config,
            opensearch_config=client_config,
            institutions=institutions,
            dois=dois,
        )
    finally:
        shutil.rmtree(meta_dir, ignore_errors=True)


def enrich_dmps_cmd(
    *,
    index_name: str,
    client_config: OpenSearchClientConfig,
    bucket_name: str | None = None,
    dmp_subset: DMPSubsetAWS | None = None,
):
    """Enrich dmps in the OpenSearch DMPs index.

    Args:
        index_name: the OpenSearch index name.
        client_config: the OpenSearch client config settings.
        bucket_name: DMP Tool S3 bucket name (required when dmp_subset is provided).
        dmp_subset: settings for including a subset of DMPs.
    """
    from dmpworks.opensearch.enrich_dmps import enrich_dmps  # noqa: PLC0415

    logging.getLogger("opensearch").setLevel(logging.WARNING)

    institutions = None
    dois = None
    meta_dir = None
    if dmp_subset is not None and dmp_subset.enable and bucket_name is not None:
        meta_dir = local_path(META_DIR)
        meta_dir.mkdir(parents=True, exist_ok=True)
        institutions, dois = load_dmp_subset_from_s3(bucket_name, dmp_subset, meta_dir)

    try:
        enrich_dmps(index_name, client_config, institutions=institutions, dois=dois)
    finally:
        if meta_dir is not None:
            shutil.rmtree(meta_dir, ignore_errors=True)


def dmp_works_search_cmd(
    *,
    bucket_name: str,
    run_id: str,
    dmps_index_name: str,
    works_index_name: str,
    scroll_time: str = "360m",
    batch_size: int = 250,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    parallel_search: bool = False,
    include_named_queries_score: bool = True,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
    client_config: OpenSearchClientConfig | None = None,
    dmp_subset: DMPSubsetAWS = None,
    start_date: pendulum.Date | None = None,
    end_date: pendulum.Date | None = None,
):
    """Run the DMP Works Search process to find matching works for DMPs.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        dmps_index_name: the name of the DMP index in OpenSearch.
        works_index_name: the name of the works index in OpenSearch.
        scroll_time: the length of time the OpenSearch scroll used to iterate
            through DMPs will stay active. Set it to a value greater than the
            length of this process.
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
    """
    from dmpworks.opensearch.dmp_works_search import dmp_works_search  # noqa: PLC0415

    logging.getLogger("opensearch").setLevel(logging.WARNING)

    out_dir = local_path(DMP_WORKS_SEARCH_DIR, run_id, MATCHES_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    meta_dir = local_path(DMP_WORKS_SEARCH_DIR, run_id, META_DIR)
    meta_dir.mkdir(parents=True, exist_ok=True)

    try:
        institutions, dois = load_dmp_subset_from_s3(bucket_name, dmp_subset, meta_dir)

        dmp_works_search(
            dmps_index_name,
            works_index_name,
            out_dir,
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

        # Upload all Parquet files to S3
        target_uri = s3_uri(bucket_name, DMP_WORKS_SEARCH_DIR, run_id, MATCHES_DIR)
        upload_files_to_s3(out_dir, target_uri, glob_pattern="*.parquet")
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)


def merge_related_works_cmd(
    *,
    bucket_name: str,
    run_id: str,
    mysql_config: MySQLConfig,
    batch_size: int = 1000,
):
    """Merge related works from S3 into the MySQL database.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        mysql_config: MySQL connection configuration.
        batch_size: Number of records to process in a batch.
    """
    from dmpworks.dmsp.related_works import merge_related_works  # noqa: PLC0415

    matches_dir = local_path(DMP_WORKS_SEARCH_DIR, run_id, MATCHES_DIR)
    matches_dir.mkdir(parents=True, exist_ok=True)
    try:
        # Download all Parquet files from S3
        source_uri = s3_uri(bucket_name, DMP_WORKS_SEARCH_DIR, run_id, MATCHES_DIR, "*.parquet")
        download_files_from_s3(source_uri, matches_dir)

        # Upsert data
        conn = pymysql.connect(
            host=mysql_config.mysql_host,
            port=mysql_config.mysql_tcp_port,
            user=mysql_config.mysql_user,
            password=mysql_config.mysql_pwd,
            database=mysql_config.mysql_database,
            cursorclass=pymysql.cursors.DictCursor,
        )
        merge_related_works(
            matches_dir,
            conn,
            batch_size=batch_size,
        )
    finally:
        shutil.rmtree(matches_dir, ignore_errors=True)
