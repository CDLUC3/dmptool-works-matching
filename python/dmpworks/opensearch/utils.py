import logging
import pathlib
from dataclasses import dataclass
from typing import Annotated, Literal, Optional, Sequence

import boto3
import pendulum
import pyarrow.dataset as ds
from cyclopts import Parameter, Token
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection, Transport
from opensearchpy.exceptions import TransportError

log = logging.getLogger(__name__)

MAX_PROCESSES = 2
CHUNK_SIZE = 1000
MAX_CHUNK_BYTES = 100 * 1024 * 1024
MAX_RETRIES = 10
INITIAL_BACKOFF = 2
MAX_BACKOFF = 600
MAX_ERROR_SAMPLES = 50


def validate_chunk_size(type_, value):
    if value <= 0:
        raise ValueError("Chunk size must be greater than zero.")


def parse_date(type_, tokens: Sequence[Token]) -> pendulum.Date:
    value = tokens[0].value
    try:
        return pendulum.from_format(value, "YYYY-MM-DD").date()
    except Exception:
        raise ValueError(f"Not a valid date: '{value}'. Expected format: YYYY-MM-DD")


Mode = Literal["local", "aws"]
ChunkSize = Annotated[int, Parameter(validator=validate_chunk_size)]
Date = Annotated[Optional[pendulum.Date], Parameter(converter=parse_date)]
QueryBuilder = Literal["build_dmp_works_search_baseline_query", "build_dmp_works_search_candidate_query"]


@dataclass
class OpenSearchClientConfig:
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
        Optional[str],
        Parameter(
            help="AWS region (required when mode=aws).",
        ),
    ] = None

    service: Annotated[
        Optional[str],
        Parameter(
            help="AWS service name for SigV4 signing (usually `es`).",
        ),
    ] = None


@dataclass
class OpenSearchSyncConfig:
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
    """Enables us to log the request body sent to OpenSearch that failed"""

    def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=(), headers=None):
        try:
            return super().perform_request(method, url, params, body, timeout, ignore, headers)
        except TransportError as e:
            log.error(f"Transport error, logging request...")
            log.error(body)
            raise e


def make_opensearch_client(config: OpenSearchClientConfig) -> OpenSearch:
    if config.mode == "aws":
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, config.region, config.service)
        client = OpenSearch(
            hosts=[{'host': config.host, 'port': config.port}],
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
    dataset = ds.dataset(in_dir, format="parquet")
    return dataset


def count_records(in_dir: pathlib.Path) -> int:
    log.info(f"Counting records: {in_dir}")
    dataset = load_dataset(in_dir)
    return dataset.count_rows()
