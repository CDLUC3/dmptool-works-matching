from collections.abc import Sequence
from dataclasses import dataclass
import logging
import pathlib
from typing import Annotated, Literal

import boto3
from cyclopts import Parameter, Token
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection, Transport
from opensearchpy.exceptions import OpenSearchException, TransportError
import pendulum
import pyarrow.dataset as ds

log = logging.getLogger(__name__)

MAX_PROCESSES = 2
CHUNK_SIZE = 1000
MAX_CHUNK_BYTES = 100 * 1024 * 1024
MAX_RETRIES = 10
INITIAL_BACKOFF = 2
MAX_BACKOFF = 600
MAX_ERROR_SAMPLES = 50


def parse_date(type_, tokens: Sequence[Token]) -> pendulum.Date:  # noqa: ARG001
    """Parse a date string from command line arguments.

    Args:
        type_: The type of the parameter.
        tokens: The list of tokens from the command line.

    Returns:
        pendulum.Date: The parsed date.

    Raises:
        ValueError: If the date string is invalid.
    """
    value = tokens[0].value
    try:
        return pendulum.from_format(value, "YYYY-MM-DD").date()
    except Exception as e:
        raise ValueError(f"Not a valid date: '{value}'. Expected format: YYYY-MM-DD") from e


Mode = Literal["local", "aws"]
Date = Annotated[pendulum.Date | None, Parameter(converter=parse_date)]
QueryBuilder = Literal["build_dmp_works_search_baseline_query", "build_dmp_works_search_candidate_query"]


@dataclass
class OpenSearchClientConfig:
    """Configuration for the OpenSearch client.

    Attributes:
        mode: OpenSearch connection mode.
        host: OpenSearch hostname or IP address.
        port: OpenSearch HTTP port.
        region: AWS region (required when mode=aws).
        service: AWS service name for SigV4 signing (usually `es`).
    """

    mode: Annotated[
        Mode,
        Parameter(
            help=(
                "OpenSearch connection mode. "
                "`local` uses an unauthenticated local client; "
                "`aws` uses AWS SigV4-signed requests."
            ),
        ),
    ] = "local"

    host: Annotated[
        str,
        Parameter(
            help="OpenSearch hostname or IP address.",
        ),
    ] = "localhost"

    port: Annotated[
        int,
        Parameter(
            help="OpenSearch HTTP port.",
        ),
    ] = 9200

    region: Annotated[
        str | None,
        Parameter(
            help="AWS region (required when mode=aws).",
        ),
    ] = None

    service: Annotated[
        str | None,
        Parameter(
            help="AWS service name for SigV4 signing (usually `es`).",
        ),
    ] = None


@dataclass
class OpenSearchSyncConfig:
    """Configuration for syncing data to OpenSearch.

    Attributes:
        max_processes: Maximum number of worker processes to run in parallel.
        chunk_size: Number of records to process per batch.
        max_chunk_bytes: Maximum serialized batch size in bytes.
        max_retries: Maximum number of retry attempts per batch.
        initial_backoff: Initial retry backoff in seconds.
        max_backoff: Maximum retry backoff in seconds.
        dry_run: Run the sync without writing any data to OpenSearch.
        measure_chunk_size: Measure serialized batch size before sending to OpenSearch.
        max_error_samples: Maximum number of error examples to retain for reporting.
        staggered_start: Stagger worker startup to reduce initial load spikes.
    """

    max_processes: Annotated[
        int,
        Parameter(
            help="Maximum number of worker processes to run in parallel.",
        ),
    ] = MAX_PROCESSES

    chunk_size: Annotated[
        int,
        Parameter(
            help="Number of records to process per batch.",
        ),
    ] = CHUNK_SIZE

    max_chunk_bytes: Annotated[
        int,
        Parameter(
            help="Maximum serialized batch size in bytes.",
        ),
    ] = MAX_CHUNK_BYTES

    max_retries: Annotated[
        int,
        Parameter(
            help="Maximum number of retry attempts per batch.",
        ),
    ] = MAX_RETRIES

    initial_backoff: Annotated[
        int,
        Parameter(
            help="Initial retry backoff in seconds.",
        ),
    ] = INITIAL_BACKOFF

    max_backoff: Annotated[
        int,
        Parameter(
            help="Maximum retry backoff in seconds.",
        ),
    ] = MAX_BACKOFF

    dry_run: Annotated[
        bool,
        Parameter(
            help="Run the sync without writing any data to OpenSearch.",
        ),
    ] = False

    measure_chunk_size: Annotated[
        bool,
        Parameter(
            help="Measure serialized batch size before sending to OpenSearch.",
        ),
    ] = False

    max_error_samples: Annotated[
        int,
        Parameter(
            help="Maximum number of error examples to retain for reporting.",
        ),
    ] = MAX_ERROR_SAMPLES

    staggered_start: Annotated[
        bool,
        Parameter(
            help="Stagger worker startup to reduce initial load spikes.",
        ),
    ] = False


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
    if config.mode == "aws":
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, config.region, config.service)
        client = OpenSearch(
            hosts=[{"host": config.host, "port": config.port}],
            http_auth=auth,
            http_compress=True,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            transport_class=DebugTransport,
            pool_maxsize=20,
            timeout=5 * 60,
        )
    else:
        client = OpenSearch(
            hosts=[{"host": config.host, "port": config.port}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            transport_class=DebugTransport,
            pool_maxsize=20,
            timeout=5 * 60,
        )

    return client


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
