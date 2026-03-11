import logging
import pathlib

import boto3
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection, Transport
from opensearchpy.exceptions import OpenSearchException, TransportError
import pyarrow.dataset as ds

from dmpworks.cli_utils import OpenSearchClientConfig

log = logging.getLogger(__name__)


class DebugTransport(Transport):
    """Enables us to log the request body sent to OpenSearch that failed."""

    def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=(), headers=None):
        """Perform the request and log the body on error.

        Args:
            method: The HTTP method.
            url: The URL.
            params: The query parameters.
            body: The request body.
            timeout: The timeout.
            ignore: The status codes to ignore.
            headers: The headers.

        Returns:
            The response.

        Raises:
            TransportError: If the request fails.
        """
        try:
            return super().perform_request(method, url, params, body, timeout, ignore, headers)
        except TransportError:
            log.exception("Transport error, logging request...")
            log.exception(body)
            raise


def make_opensearch_client(config: OpenSearchClientConfig) -> OpenSearch:
    """Create an OpenSearch client based on the configuration.

    Args:
        config: The OpenSearch client configuration.

    Returns:
        OpenSearch: The OpenSearch client.
    """
    auth = None
    if config.auth_type == "aws":
        if not config.aws_region or not config.aws_service:
            raise ValueError("AWS authentication requires 'aws_region' and 'aws_service' to be defined.")
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, config.aws_region, config.aws_service)
    elif config.auth_type == "basic":
        if not config.username or not config.password:
            raise ValueError("Basic authentication requires 'username' and 'password' to be defined.")
        auth = (config.username, config.password)

    return OpenSearch(
        hosts=[{"host": config.host, "port": config.port}],
        http_auth=auth,
        http_compress=True,
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        connection_class=RequestsHttpConnection,
        transport_class=DebugTransport,
        pool_maxsize=config.pool_maxsize,
        timeout=config.timeout,
    )


def load_dataset(in_dir: pathlib.Path) -> ds.Dataset:
    """Load a parquet dataset from a directory.

    Args:
        in_dir: The directory containing the parquet files.

    Returns:
        ds.Dataset: The loaded dataset.
    """
    return ds.dataset(in_dir, format="parquet")


def count_records(in_dir: pathlib.Path) -> int:
    """Count the number of records in a parquet dataset.

    Args:
        in_dir: The directory containing the parquet files.

    Returns:
        int: The number of records.
    """
    log.info(f"Counting records: {in_dir}")
    dataset = load_dataset(in_dir)
    return dataset.count_rows()


def update_refresh_interval(*, client: OpenSearch, index: str, refresh_interval: str) -> None:
    """Update the refresh interval for an OpenSearch index.

    Args:
        client: The OpenSearch client.
        index: The name of the index.
        refresh_interval: The new refresh interval.

    Raises:
        OpenSearchException: If the update fails.
    """
    try:
        client.indices.put_settings(
            index=index,
            body={"index": {"refresh_interval": refresh_interval}},
        )

        log.info(f"Successfully updated refresh_interval for index '{index}' " f"to '{refresh_interval}'")

    except OpenSearchException:
        log.exception(f"Failed to update refresh_interval for index '{index}' " f"to '{refresh_interval}'")
        raise


def force_index_refresh(*, client: OpenSearch, index: str) -> None:
    """Force a refresh of an OpenSearch index.

    Args:
        client: The OpenSearch client.
        index: The name of the index.

    Raises:
        OpenSearchException: If the refresh fails.
    """
    try:
        response = client.indices.refresh(index=index)

        shards = response.get("_shards", {})
        failed = shards.get("failed", 0)

        if failed == 0:
            log.info(f"Successfully refreshed index '{index}'")
        else:
            log.warning(f"Refresh completed with shard failures for index '{index}': {shards}")

    except OpenSearchException:
        log.exception(f"Failed to refresh index '{index}'")
        raise
