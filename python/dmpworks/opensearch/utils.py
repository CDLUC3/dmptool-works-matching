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


@dataclass
class OpenSearchClientConfig:
    mode: Mode = "local"
    host: str = "localhost"
    port: int = 9200
    region: Optional[str] = None
    service: Optional[str] = None


@dataclass
class OpenSearchSyncConfig:
    max_processes: int = MAX_PROCESSES
    chunk_size: int = CHUNK_SIZE
    max_chunk_bytes: int = MAX_CHUNK_BYTES
    max_retries: int = MAX_RETRIES
    initial_backoff: int = INITIAL_BACKOFF
    max_backoff: int = MAX_BACKOFF
    dry_run: bool = False
    measure_chunk_size: bool = False
    max_error_samples: int = MAX_ERROR_SAMPLES
    staggered_start: bool = False


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
