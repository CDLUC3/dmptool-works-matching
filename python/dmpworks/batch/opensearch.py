import logging
from typing import Optional

from cyclopts import App

from dmpworks.batch.utils import download_from_s3, local_path, s3_uri
from dmpworks.cli_utils import DateString, LogLevel
from dmpworks.opensearch.cli import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.opensearch.sync_works import sync_works
from dmpworks.transform.utils_file import setup_multiprocessing_logging

log = logging.getLogger(__name__)

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
    """Sync exported works with OpenSearch.

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
    export_dir = local_path("sqlmesh", export_date, "export")
    source_uri = s3_uri(bucket_name, "sqlmesh", export_date, "export")
    download_from_s3(f"{source_uri}*", export_dir)

    # Run process
    sync_works(index_name, export_dir, client_config, sync_config, level)


if __name__ == "__main__":
    app()
