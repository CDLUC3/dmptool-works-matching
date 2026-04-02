from __future__ import annotations

from dataclasses import dataclass
import pathlib
from typing import TYPE_CHECKING, Annotated, Any, Literal, get_args, get_type_hints

from cyclopts import Parameter, Token, validators
import pendulum
import pendulum.parsing

if TYPE_CHECKING:
    from collections.abc import Sequence

from dmpworks.constants import (
    AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD,
    AUDIT_DATACITE_WORKS_THRESHOLD,
    AUDIT_NESTED_OBJECT_LIMIT,
    AUDIT_OPENALEX_WORKS_THRESHOLD,
    CROSSREF_METADATA_TRANSFORM_BATCH_SIZE,
    CROSSREF_METADATA_TRANSFORM_MAX_WORKERS,
    CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE,
    CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE,
    DATACITE_TRANSFORM_BATCH_SIZE,
    DATACITE_TRANSFORM_MAX_WORKERS,
    DATACITE_TRANSFORM_ROW_GROUP_SIZE,
    DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE,
    DMP_WORKS_SEARCH_BATCH_SIZE,
    DMP_WORKS_SEARCH_INNER_HITS_SIZE,
    DMP_WORKS_SEARCH_MAX_CONCURRENT_SEARCHES,
    DMP_WORKS_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS,
    DMP_WORKS_SEARCH_MAX_RESULTS,
    DMP_WORKS_SEARCH_PROJECT_END_BUFFER_YEARS,
    DMP_WORKS_SEARCH_QUERY_BUILDER,
    DMP_WORKS_SEARCH_ROW_GROUP_SIZE,
    DMP_WORKS_SEARCH_ROW_GROUPS_PER_FILE,
    DMP_WORKS_SEARCH_SCROLL_TIME,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_RELATIONS_DATACITE_THREADS,
    DUCKDB_THREADS,
    MAX_DOI_STATES,
    MAX_RELATION_DEGREES,
    OPENALEX_WORKS_TRANSFORM_BATCH_SIZE,
    OPENALEX_WORKS_TRANSFORM_INCLUDE_XPAC,
    OPENALEX_WORKS_TRANSFORM_MAX_WORKERS,
    OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE,
    OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE,
    OPENSEARCH_SYNC_CHUNK_SIZE,
    OPENSEARCH_SYNC_INITIAL_BACKOFF,
    OPENSEARCH_SYNC_MAX_BACKOFF,
    OPENSEARCH_SYNC_MAX_CHUNK_BYTES,
    OPENSEARCH_SYNC_MAX_ERROR_SAMPLES,
    OPENSEARCH_SYNC_MAX_PROCESSES,
    OPENSEARCH_SYNC_MAX_RETRIES,
)


def get_env_var_dict(instance: Any) -> dict[str, str | None]:
    """Scans a dataclass instance for cyclopts Parameter metadata.

    Maps defined environment variables to their stringified current values.
    None values are preserved as None so that make_env can filter them out.
    Boolean values are lowercased to match env var conventions (e.g. "true"/"false").

    Args:
        instance: The dataclass instance to scan.

    Returns:
        A dictionary mapping environment variable names to their values.
    """
    # include_extras=True preserves the Annotated metadata
    hints = get_type_hints(type(instance), include_extras=True)
    env_dict: dict[str, str | None] = {}

    for attr_name, type_hint in hints.items():
        current_value = getattr(instance, attr_name)
        args = get_args(type_hint)
        if args:
            for metadata in args[1:]:
                if isinstance(metadata, Parameter) and metadata.env_var:
                    env_vars = metadata.env_var
                    if isinstance(env_vars, str):
                        env_vars = [env_vars]
                    for ev in env_vars:
                        if current_value is None:
                            env_dict[ev] = None
                        elif isinstance(current_value, bool):
                            env_dict[ev] = str(current_value).lower()
                        else:
                            env_dict[ev] = str(current_value)
                    break
    return env_dict


def config_to_kwargs(*configs: Any) -> dict[str, Any]:
    """Merge config objects into lowercased keyword arguments for factory functions.

    Converts one or more dataclass config instances into a flat dict suitable for
    passing as ``**kwargs`` to a job factory. Environment variable names are lowercased
    and ``None`` values are filtered out.

    Args:
        *configs: Dataclass instances to convert via get_env_var_dict.

    Returns:
        Merged dict with lowercase keys. None values are filtered out.
    """
    kwargs: dict[str, Any] = {}
    for config in configs:
        kwargs.update({k.lower(): v for k, v in get_env_var_dict(config).items() if v is not None})
    return kwargs


def validate_date_str(type_, value):  # noqa: ARG001
    """Validate that a string is in YYYY-MM-DD format.

    Args:
        type_: The type of the value (unused).
        value: The string to validate.

    Raises:
        ValueError: If the string is not in the correct format.
    """
    if value is None:
        return
    try:
        pendulum.from_format(value, "YYYY-MM-DD")
    except pendulum.parsing.exceptions.ParserError as e:
        raise ValueError(f"Invalid date: '{value}'. Must be in YYYY-MM-DD format.") from e


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


Directory = Annotated[
    pathlib.Path,
    Parameter(
        validator=validators.Path(
            dir_okay=True,
            file_okay=False,
            exists=True,
        )
    ),
]
LogLevel = Annotated[
    Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
    Parameter(help="Python log level"),
]
Date = Annotated[pendulum.Date | None, Parameter(converter=parse_date)]
QueryBuilder = Literal["build_dmp_works_search_baseline_query", "build_dmp_works_search_candidate_query"]


@dataclass
class DatasetSubsetAWS:
    """Cyclopts configuration for creating a subset of datasets (AWS).

    Attributes:
        enable: Enable subset creation to filter works by specific institutions or a list of DOIs.
        institutions_s3_path: S3 path (excluding bucket URI) to a list of ROR IDs and institution names.
        dois_s3_path: S3 path (excluding bucket URI) to a specific list of Work DOIs to include in the subset.
    """

    enable: Annotated[
        bool,
        Parameter(
            env_var="DATASET_SUBSET_ENABLE",
            help="Enable subset creation to filter works by specific institutions or a list of DOIs.",
        ),
    ] = False
    institutions_s3_path: Annotated[
        str | None,
        Parameter(
            env_var="DATASET_SUBSET_INSTITUTIONS_S3_PATH",
            help="S3 path (excluding bucket URI) to a list of ROR IDs and institution names. Works authored by researchers from these institutions will be included.",
        ),
    ] = None
    dois_s3_path: Annotated[
        str | None,
        Parameter(
            env_var="DATASET_SUBSET_DOIS_S3_PATH",
            help="S3 path (excluding bucket URI) to a specific list of Work DOIs to include in the subset.",
        ),
    ] = None


@dataclass
class DatasetSubsetLocal:
    """Cyclopts configuration for creating a subset of datasets (local).

    Attributes:
        enable: Enable subset creation to filter works by specific institutions or a list of DOIs.
        institutions_path: Path to a JSON file containing ROR IDs and institution names.
        dois_path: Path to a JSON file containing specific Work DOIs to include in the subset.
    """

    enable: Annotated[
        bool,
        Parameter(
            env_var="DATASET_SUBSET_ENABLE",
            help="Enable subset creation to filter works by specific institutions or a list of DOIs.",
        ),
    ] = False
    institutions_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="DATASET_SUBSET_INSTITUTIONS_PATH",
            help="Path to a JSON file containing ROR IDs and institution names. Works authored by researchers from these institutions will be included.",
        ),
    ] = None
    dois_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="DATASET_SUBSET_DOIS_PATH",
            help="Path to a JSON file containing specific Work DOIs to include in the subset.",
        ),
    ] = None


@dataclass
class DMPSubsetAWS:
    """Cyclopts configuration for creating a subset of DMPs (AWS).

    Attributes:
        enable: Enable subset creation to filter DMPs by specific institutions or a list of DOIs.
        institutions_s3_path: S3 path (excluding bucket URI) to a list of ROR IDs and institution names.
        dois_s3_path: S3 path (excluding bucket URI) to a specific list of DMP DOIs to include in the subset.
    """

    enable: Annotated[
        bool,
        Parameter(
            env_var="DMP_SUBSET_ENABLE",
            help="Enable subset creation to filter DMPs by specific institutions or a list of DOIs.",
        ),
    ] = False
    institutions_s3_path: Annotated[
        str | None,
        Parameter(
            env_var="DMP_SUBSET_INSTITUTIONS_S3_PATH",
            help="S3 path (excluding bucket URI) to a list of ROR IDs and institution names. DMPs created by researchers from these institutions will be included.",
        ),
    ] = None
    dois_s3_path: Annotated[
        str | None,
        Parameter(
            env_var="DMP_SUBSET_DOIS_S3_PATH",
            help="S3 path (excluding bucket URI) to a specific list of DMP DOIs to include in the subset.",
        ),
    ] = None


@dataclass
class DMPSubsetLocal:
    """Cyclopts configuration for creating a subset of DMPs (local).

    Attributes:
        enable: Enable subset creation to filter DMPs by specific institutions or a list of DOIs.
        institutions_path: Path to a JSON file containing ROR IDs and institution names.
        dois_path: Path to a JSON file containing specific DMP DOIs to include in the subset.
    """

    enable: Annotated[
        bool,
        Parameter(
            env_var="DMP_SUBSET_ENABLE",
            help="Enable subset creation to filter DMPs by specific institutions or a list of DOIs.",
        ),
    ] = False
    institutions_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="DMP_SUBSET_INSTITUTIONS_PATH",
            help="Path to a JSON file containing ROR IDs and institution names. DMPs created by researchers from these institutions will be included.",
        ),
    ] = None
    dois_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="DMP_SUBSET_DOIS_PATH",
            help="Path to a JSON file containing specific DMP DOIs to include in the subset.",
        ),
    ] = None


@dataclass
class MySQLConfig:
    """Cyclopts configuration for MySQL connection.

    Attributes:
        mysql_host: MySQL hostname.
        mysql_tcp_port: MySQL port.
        mysql_user: MySQL user name.
        mysql_database: MySQL database name.
        mysql_pwd: MySQL password.
    """

    mysql_host: Annotated[
        str,
        Parameter(
            env_var="MYSQL_HOST",
            help="MySQL hostname",
        ),
    ]
    mysql_tcp_port: Annotated[
        int,
        Parameter(
            env_var="MYSQL_TCP_PORT",
            help="MySQL port",
        ),
    ]
    mysql_user: Annotated[
        str,
        Parameter(
            env_var="MYSQL_USER",
            help="MySQL user name",
        ),
    ]
    mysql_database: Annotated[
        str,
        Parameter(
            env_var="MYSQL_DATABASE",
            help="MySQL database name",
        ),
    ]
    mysql_pwd: Annotated[
        str,
        Parameter(
            env_var="MYSQL_PWD",
            help="MySQL password",
        ),
    ]


@dataclass
class CrossrefMetadataTransformConfig:
    """Cyclopts configuration for Crossref Metadata transformation.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of workers to run in parallel.
    """

    batch_size: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_METADATA_TRANSFORM_BATCH_SIZE",
            validator=validators.Number(gte=1),
            help="Number of input files to process per batch (must be >= 1).",
        ),
    ] = CROSSREF_METADATA_TRANSFORM_BATCH_SIZE
    row_group_size: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE",
            validator=validators.Number(gte=1),
            help="Parquet row group size (must be >= 1). For efficient downstream querying, target row group sizes of 128-512MB. Row groups are buffered fully in memory before being flushed to disk.",
        ),
    ] = CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE
    row_groups_per_file: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE",
            validator=validators.Number(gte=1),
            help="Number of row groups per Parquet file (must be >= 1). Target file sizes of 512MB-1GB.",
        ),
    ] = CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE
    max_workers: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_METADATA_TRANSFORM_MAX_WORKERS",
            validator=validators.Number(gte=1),
            help="Number of workers to run in parallel (must be >= 1).",
        ),
    ] = CROSSREF_METADATA_TRANSFORM_MAX_WORKERS


@dataclass
class OpenAlexWorksTransformConfig:
    """Cyclopts configuration for OpenAlex Works transformation.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of workers to run in parallel.
    """

    batch_size: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_WORKS_TRANSFORM_BATCH_SIZE",
            validator=validators.Number(gte=1),
            help="Number of input files to process per batch (must be >= 1).",
        ),
    ] = OPENALEX_WORKS_TRANSFORM_BATCH_SIZE
    row_group_size: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE",
            validator=validators.Number(gte=1),
            help="Parquet row group size (must be >= 1). For efficient downstream querying, target row group sizes of 128-512MB. Row groups are buffered fully in memory before being flushed to disk.",
        ),
    ] = OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE
    row_groups_per_file: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE",
            validator=validators.Number(gte=1),
            help="Number of row groups per Parquet file (must be >= 1). Target file sizes of 512MB-1GB.",
        ),
    ] = OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE
    max_workers: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_WORKS_TRANSFORM_MAX_WORKERS",
            validator=validators.Number(gte=1),
            help="Number of workers to run in parallel (must be >= 1).",
        ),
    ] = OPENALEX_WORKS_TRANSFORM_MAX_WORKERS
    include_xpac: Annotated[
        bool,
        Parameter(
            env_var="OPENALEX_WORKS_TRANSFORM_INCLUDE_XPAC",
            help="Include works flagged as xpac (is_xpac=true). Defaults to false.",
        ),
    ] = OPENALEX_WORKS_TRANSFORM_INCLUDE_XPAC


@dataclass
class DataCiteTransformConfig:
    """Cyclopts configuration for DataCite transformation.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of workers to run in parallel.
    """

    batch_size: Annotated[
        int,
        Parameter(
            env_var="DATACITE_TRANSFORM_BATCH_SIZE",
            validator=validators.Number(gte=1),
            help="Number of input files to process per batch (must be >= 1).",
        ),
    ] = DATACITE_TRANSFORM_BATCH_SIZE
    row_group_size: Annotated[
        int,
        Parameter(
            env_var="DATACITE_TRANSFORM_ROW_GROUP_SIZE",
            validator=validators.Number(gte=1),
            help="Parquet row group size (must be >= 1). For efficient downstream querying, target row group sizes of 128-512MB. Row groups are buffered fully in memory before being flushed to disk.",
        ),
    ] = DATACITE_TRANSFORM_ROW_GROUP_SIZE
    row_groups_per_file: Annotated[
        int,
        Parameter(
            env_var="DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE",
            validator=validators.Number(gte=1),
            help="Number of row groups per Parquet file (must be >= 1). Target file sizes of 512MB-1GB.",
        ),
    ] = DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE
    max_workers: Annotated[
        int,
        Parameter(
            env_var="DATACITE_TRANSFORM_MAX_WORKERS",
            validator=validators.Number(gte=1),
            help="Number of workers to run in parallel (must be >= 1).",
        ),
    ] = DATACITE_TRANSFORM_MAX_WORKERS


@dataclass
class OpenSearchClientConfig:
    """Configuration for the OpenSearch client.

    Attributes:
        host: OpenSearch hostname or IP address.
        port: OpenSearch HTTP port.
        use_ssl: Whether to use SSL.
        verify_certs: Whether to verify SSL certificates.
        auth_type: Authentication type (aws or basic).
        username: Username for basic auth.
        password: Password for basic auth.
        aws_region: AWS region (required when auth_type=aws).
        aws_service: AWS service name for SigV4 signing (usually `es`).
        pool_maxsize: Maximum number of connections in the pool.
        timeout: Timeout in seconds.
    """

    host: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_HOST",
            help="OpenSearch hostname or IP address.",
        ),
    ] = "localhost"
    port: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_PORT",
            help="OpenSearch HTTP port.",
        ),
    ] = 9200
    use_ssl: Annotated[
        bool,
        Parameter(
            env_var="OPENSEARCH_USE_SSL",
            help="Whether to use SSL.",
        ),
    ] = False
    verify_certs: Annotated[
        bool,
        Parameter(
            env_var="OPENSEARCH_VERIFY_CERTS",
            help="Whether to verify SSL certificates.",
        ),
    ] = False
    auth_type: Annotated[
        Literal["aws", "basic"] | None,
        Parameter(
            env_var="OPENSEARCH_AUTH_TYPE",
            help="Authentication type (aws or basic).",
        ),
    ] = None
    username: Annotated[
        str | None,
        Parameter(
            env_var="OPENSEARCH_USERNAME",
            help="Username for basic auth.",
        ),
    ] = None
    password: Annotated[
        str | None,
        Parameter(
            env_var="OPENSEARCH_PASSWORD",
            help="Password for basic auth.",
        ),
    ] = None
    aws_region: Annotated[
        str | None,
        Parameter(
            env_var="OPENSEARCH_REGION",
            help="AWS region (required when auth_type=aws).",
        ),
    ] = None
    aws_service: Annotated[
        str | None,
        Parameter(
            env_var="OPENSEARCH_SERVICE",
            help="AWS service name for SigV4 signing (usually `es`).",
        ),
    ] = None
    pool_maxsize: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_POOL_MAXSIZE",
            help="Maximum number of connections in the pool.",
        ),
    ] = 20
    timeout: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_TIMEOUT",
            help="Timeout in seconds.",
        ),
    ] = 300


@dataclass
class RunIdentifiers:
    """Run identifiers for various datasets and pipeline jobs.

    Attributes:
        openalex_works: Run identifier for OpenAlex works (timestamp).
        datacite: Run identifier for DataCite (timestamp).
        crossref_metadata: Run identifier for Crossref metadata (timestamp).
        ror: Run identifier for ROR (timestamp).
        data_citation_corpus: Run identifier for Data Citation Corpus (timestamp).
        run_id_sqlmesh_prev: Run ID of the previous SQLMesh job.
        run_id_sqlmesh: Run ID of the current SQLMesh job.
    """

    openalex_works: Annotated[
        str | None,
        Parameter(
            env_var="RUN_ID_OPENALEX_WORKS",
            help="Run identifier for OpenAlex works.",
        ),
    ] = None
    datacite: Annotated[
        str | None,
        Parameter(
            env_var="RUN_ID_DATACITE",
            help="Run identifier for DataCite.",
        ),
    ] = None
    crossref_metadata: Annotated[
        str | None,
        Parameter(
            env_var="RUN_ID_CROSSREF_METADATA",
            help="Run identifier for Crossref metadata.",
        ),
    ] = None
    ror: Annotated[
        str | None,
        Parameter(
            env_var="RUN_ID_ROR",
            help="Run identifier for ROR.",
        ),
    ] = None
    data_citation_corpus: Annotated[
        str | None,
        Parameter(
            env_var="RUN_ID_DATA_CITATION_CORPUS",
            help="Run identifier for Data Citation Corpus.",
        ),
    ] = None
    run_id_sqlmesh_prev: Annotated[
        str | None,
        Parameter(
            env_var="RUN_ID_SQLMESH_PREV",
            help="Previous SQLMesh Run ID",
        ),
    ] = None
    run_id_sqlmesh: Annotated[
        str | None,
        Parameter(
            env_var="RUN_ID_SQLMESH",
            help="SQLMesh Run ID",
        ),
    ] = None
    release_date_process_works: Annotated[
        str | None,
        Parameter(
            env_var="RELEASE_DATE_PROCESS_WORKS",
            help="Release date (YYYY-MM-DD) for the process-works pipeline run.",
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
            env_var="OPENSEARCH_SYNC_MAX_PROCESSES",
            help="Maximum number of worker processes to run in parallel.",
        ),
    ] = OPENSEARCH_SYNC_MAX_PROCESSES
    chunk_size: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_SYNC_CHUNK_SIZE",
            help="Number of records to process per batch.",
        ),
    ] = OPENSEARCH_SYNC_CHUNK_SIZE
    max_chunk_bytes: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_SYNC_MAX_CHUNK_BYTES",
            help="Maximum serialized batch size in bytes.",
        ),
    ] = OPENSEARCH_SYNC_MAX_CHUNK_BYTES
    max_retries: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_SYNC_MAX_RETRIES",
            help="Maximum number of retry attempts per batch.",
        ),
    ] = OPENSEARCH_SYNC_MAX_RETRIES
    initial_backoff: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_SYNC_INITIAL_BACKOFF",
            help="Initial retry backoff in seconds.",
        ),
    ] = OPENSEARCH_SYNC_INITIAL_BACKOFF
    max_backoff: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_SYNC_MAX_BACKOFF",
            help="Maximum retry backoff in seconds.",
        ),
    ] = OPENSEARCH_SYNC_MAX_BACKOFF
    dry_run: Annotated[
        bool,
        Parameter(
            env_var="OPENSEARCH_SYNC_DRY_RUN",
            help="Run the sync without writing any data to OpenSearch.",
        ),
    ] = False
    measure_chunk_size: Annotated[
        bool,
        Parameter(
            env_var="OPENSEARCH_SYNC_MEASURE_CHUNK_SIZE",
            help="Measure serialized batch size before sending to OpenSearch.",
        ),
    ] = False
    max_error_samples: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_SYNC_MAX_ERROR_SAMPLES",
            help="Maximum number of error examples to retain for reporting.",
        ),
    ] = OPENSEARCH_SYNC_MAX_ERROR_SAMPLES
    staggered_start: Annotated[
        bool,
        Parameter(
            env_var="OPENSEARCH_SYNC_STAGGERED_START",
            help="Stagger worker startup to reduce initial load spikes.",
        ),
    ] = False


@dataclass
class DMPWorksSearchConfig:
    """Cyclopts configuration for the DMP Works Search.

    Attributes:
        query_builder_name: Name of the query builder to use.
        rerank_model_name: Name of the re-ranking model to use.
        scroll_time: Length of time the OpenSearch scroll context remains active.
        batch_size: Number of DMPs processed per batch.
        max_results: Maximum number of works to return per DMP.
        project_end_buffer_years: Years added to project end date when searching for works.
        parallel_search: Whether to run parallel search (msearch).
        include_named_queries_score: Whether to include scores for subqueries.
        max_concurrent_searches: Maximum number of concurrent searches.
        max_concurrent_shard_requests: Maximum number of shards searched per node per request.
        inner_hits_size: Maximum number of inner hits returned per matched work.
        row_group_size: Parquet row group size for output files.
        row_groups_per_file: Number of row groups per output Parquet file.
        dmps_start_date: Return DMPs with project start dates on or after this date.
        dmps_end_date: Return DMPs with project start dates on or before this date.
        dmp_modification_window_days: Only search DMPs modified within this many days. If unset, all DMPs are searched.
    """

    query_builder_name: Annotated[
        QueryBuilder,
        Parameter(
            env_var="DMP_WORKS_SEARCH_QUERY_BUILDER",
            help="Name of the query builder to use.",
        ),
    ] = DMP_WORKS_SEARCH_QUERY_BUILDER
    rerank_model_name: Annotated[
        str | None,
        Parameter(
            env_var="DMP_WORKS_SEARCH_RERANK_MODEL",
            help="Name of the re-ranking model to use. If not supplied, no re-ranking occurs.",
        ),
    ] = None
    scroll_time: Annotated[
        str,
        Parameter(
            env_var="DMP_WORKS_SEARCH_SCROLL_TIME",
            help="Length of time the OpenSearch scroll context remains active.",
        ),
    ] = DMP_WORKS_SEARCH_SCROLL_TIME
    batch_size: Annotated[
        int,
        Parameter(
            env_var="DMP_WORKS_SEARCH_BATCH_SIZE",
            help="Number of DMPs processed per batch.",
        ),
    ] = DMP_WORKS_SEARCH_BATCH_SIZE
    max_results: Annotated[
        int,
        Parameter(
            env_var="DMP_WORKS_SEARCH_MAX_RESULTS",
            help="Maximum number of works to return per DMP.",
        ),
    ] = DMP_WORKS_SEARCH_MAX_RESULTS
    project_end_buffer_years: Annotated[
        int,
        Parameter(
            env_var="DMP_WORKS_SEARCH_PROJECT_END_BUFFER_YEARS",
            help="Years added to project end date when searching for works.",
        ),
    ] = DMP_WORKS_SEARCH_PROJECT_END_BUFFER_YEARS
    parallel_search: Annotated[
        bool,
        Parameter(
            env_var="DMP_WORKS_SEARCH_PARALLEL_SEARCH",
            help="Whether to run parallel search (msearch).",
        ),
    ] = False
    include_named_queries_score: Annotated[
        bool,
        Parameter(
            env_var="DMP_WORKS_SEARCH_INCLUDE_NAMED_QUERIES_SCORE",
            help="Whether to include scores for subqueries.",
        ),
    ] = True
    max_concurrent_searches: Annotated[
        int,
        Parameter(
            env_var="DMP_WORKS_SEARCH_MAX_CONCURRENT_SEARCHES",
            help="Maximum number of concurrent searches.",
        ),
    ] = DMP_WORKS_SEARCH_MAX_CONCURRENT_SEARCHES
    max_concurrent_shard_requests: Annotated[
        int,
        Parameter(
            env_var="DMP_WORKS_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS",
            help="Maximum number of shards searched per node per request.",
        ),
    ] = DMP_WORKS_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS
    inner_hits_size: Annotated[
        int,
        Parameter(
            env_var="DMP_WORKS_SEARCH_INNER_HITS_SIZE",
            help="Maximum number of inner hits returned per matched work.",
        ),
    ] = DMP_WORKS_SEARCH_INNER_HITS_SIZE
    row_group_size: Annotated[
        int,
        Parameter(
            env_var="DMP_WORKS_SEARCH_ROW_GROUP_SIZE",
            help="Parquet row group size for output files.",
        ),
    ] = DMP_WORKS_SEARCH_ROW_GROUP_SIZE
    row_groups_per_file: Annotated[
        int,
        Parameter(
            env_var="DMP_WORKS_SEARCH_ROW_GROUPS_PER_FILE",
            help="Number of row groups per output Parquet file.",
        ),
    ] = DMP_WORKS_SEARCH_ROW_GROUPS_PER_FILE
    dmps_start_date: Annotated[
        pendulum.Date | None,
        Parameter(
            env_var="DMP_WORKS_SEARCH_DMPS_START_DATE",
            converter=parse_date,
            help="Return DMPs with project start dates on or after this date (YYYY-MM-DD).",
        ),
    ] = None
    dmps_end_date: Annotated[
        pendulum.Date | None,
        Parameter(
            env_var="DMP_WORKS_SEARCH_DMPS_END_DATE",
            converter=parse_date,
            help="Return DMPs with project start dates on or before this date (YYYY-MM-DD).",
        ),
    ] = None
    dmp_modification_window_days: Annotated[
        int | None,
        Parameter(
            env_var="DMP_WORKS_SEARCH_DMP_MODIFICATION_WINDOW_DAYS",
            help="Only search DMPs modified within this many days. If unset, all DMPs are searched.",
        ),
    ] = None
    apply_modification_window: Annotated[
        bool,
        Parameter(
            env_var="DMP_WORKS_SEARCH_APPLY_MODIFICATION_WINDOW",
            help="Whether to apply the dmp_modification_window_days filter. Set to false to process all DMPs.",
        ),
    ] = True


@dataclass
class SQLMeshConfig:
    """Cyclopts configuration for SQLMesh threads."""

    # DuckDB database path
    duckdb_database: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="DUCKDB_DATABASE",
            help="DuckDB database path",
        ),
    ] = None

    # Paths
    crossref_metadata_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="CROSSREF_METADATA_PATH",
            help="Path to Crossref Metadata",
        ),
    ] = None
    data_citation_corpus_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="DATA_CITATION_CORPUS_PATH",
            help="Path to Data Citation Corpus",
        ),
    ] = None
    datacite_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="DATACITE_PATH",
            help="Path to DataCite",
        ),
    ] = None
    doi_state_export_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="DOI_STATE_EXPORT_PATH",
            help="Path to DOI State Export",
        ),
    ] = None
    openalex_works_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="OPENALEX_WORKS_PATH",
            help="Path to OpenAlex Works",
        ),
    ] = None
    opensearch_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="OPENSEARCH_PATH",
            help="Path to OpenSearch",
        ),
    ] = None
    ror_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="ROR_PATH",
            help="Path to ROR",
        ),
    ] = None
    works_index_export_path: Annotated[
        pathlib.Path | None,
        Parameter(
            env_var="WORKS_INDEX_EXPORT_PATH",
            help="Path to Works Index Export",
        ),
    ] = None

    # DuckDB settings
    duckdb_threads: Annotated[
        int,
        Parameter(
            env_var="DUCKDB_THREADS",
            help="Number of threads for DuckDB",
        ),
    ] = DUCKDB_THREADS
    duckdb_memory_limit: Annotated[
        str,
        Parameter(
            env_var="DUCKDB_MEMORY_LIMIT",
            help="Memory limit for DuckDB",
        ),
    ] = DUCKDB_MEMORY_LIMIT

    # Audits
    audit_crossref_metadata_works_threshold: Annotated[
        int,
        Parameter(
            env_var="AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD",
            help="Threshold for Crossref Metadata Works audit",
        ),
    ] = AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD
    audit_datacite_works_threshold: Annotated[
        int,
        Parameter(
            env_var="AUDIT_DATACITE_WORKS_THRESHOLD",
            help="Threshold for DataCite Works audit",
        ),
    ] = AUDIT_DATACITE_WORKS_THRESHOLD
    audit_nested_object_limit: Annotated[
        int,
        Parameter(
            env_var="AUDIT_NESTED_OBJECT_LIMIT",
            help="Limit for nested object audit",
        ),
    ] = AUDIT_NESTED_OBJECT_LIMIT
    audit_openalex_works_threshold: Annotated[
        int,
        Parameter(
            env_var="AUDIT_OPENALEX_WORKS_THRESHOLD",
            help="Threshold for OpenAlex Works audit",
        ),
    ] = AUDIT_OPENALEX_WORKS_THRESHOLD

    # Other settings
    max_doi_states: Annotated[
        int,
        Parameter(
            env_var="MAX_DOI_STATES",
            help="Maximum number of DOI states",
        ),
    ] = MAX_DOI_STATES
    max_relation_degrees: Annotated[
        int,
        Parameter(
            env_var="MAX_RELATION_DEGREES",
            help="Maximum number of relation degrees",
        ),
    ] = MAX_RELATION_DEGREES

    # Threads
    crossref_crossref_metadata: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_CROSSREF_METADATA_THREADS",
            help="Number of threads for the crossref.crossref_metadata SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    crossref_index_works_metadata: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_INDEX_WORKS_METADATA_THREADS",
            help="Number of threads for the crossref_index.works_metadata SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_datacite: Annotated[
        int,
        Parameter(
            env_var="DATACITE_DATACITE_THREADS",
            help="Number of threads for the datacite.datacite SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_index_awards: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_AWARDS_THREADS",
            help="Number of threads for the datacite_index.awards SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_index_datacite_index: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_DATACITE_INDEX_THREADS",
            help="Number of threads for the datacite_index.datacite_index SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_index_funders: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_FUNDERS_THREADS",
            help="Number of threads for the datacite_index.funders SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_index_institutions: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_INSTITUTIONS_THREADS",
            help="Number of threads for the datacite_index.institutions SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_index_updated_dates: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_UPDATED_DATES_THREADS",
            help="Number of threads for the datacite_index.updated_dates SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_index_work_types: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_WORK_TYPES_THREADS",
            help="Number of threads for the datacite_index.work_types SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_index_works: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_WORKS_THREADS",
            help="Number of threads for the datacite_index.works SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    datacite_index_datacite_index_hashes: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_DATACITE_INDEX_HASHES_THREADS",
            help="Number of threads for the datacite_index.datacite_index_hashes SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_openalex_works: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_OPENALEX_WORKS_THREADS",
            help="Number of threads for the openalex.openalex_works SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_abstract_stats: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_ABSTRACT_STATS_THREADS",
            help="Number of threads for the openalex_index.abstract_stats SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_abstracts: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_ABSTRACTS_THREADS",
            help="Number of threads for the openalex_index.abstracts SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_author_names: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_AUTHOR_NAMES_THREADS",
            help="Number of threads for the openalex_index.author_names SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_awards: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_AWARDS_THREADS",
            help="Number of threads for the openalex_index.awards SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_funders: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_FUNDERS_THREADS",
            help="Number of threads for the openalex_index.funders SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_openalex_index: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_OPENALEX_INDEX_THREADS",
            help="Number of threads for the openalex_index.openalex_index SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_publication_dates: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_PUBLICATION_DATES_THREADS",
            help="Number of threads for the openalex_index.publication_dates SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_title_stats: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_TITLE_STATS_THREADS",
            help="Number of threads for the openalex_index.title_stats SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_titles: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_TITLES_THREADS",
            help="Number of threads for the openalex_index.titles SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_updated_dates: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_UPDATED_DATES_THREADS",
            help="Number of threads for the openalex_index.updated_dates SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_works_metadata: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_WORKS_METADATA_THREADS",
            help="Number of threads for the openalex_index.works_metadata SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    openalex_index_openalex_index_hashes: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_OPENALEX_INDEX_HASHES_THREADS",
            help="Number of threads for the openalex_index.openalex_index_hashes SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    opensearch_current_doi_state: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_CURRENT_DOI_STATE_THREADS",
            help="Number of threads for the opensearch.current_doi_state SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    opensearch_export: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_EXPORT_THREADS",
            help="Number of threads for the opensearch.export SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    opensearch_next_doi_state: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_NEXT_DOI_STATE_THREADS",
            help="Number of threads for the opensearch.next_doi_state SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    data_citation_corpus_relations: Annotated[
        int,
        Parameter(
            env_var="DATA_CITATION_CORPUS_THREADS",
            help="Number of threads for the data_citation_corpus.relations SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    relations_crossref_metadata_degrees: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_CROSSREF_METADATA_DEGREES_THREADS",
            help="Number of threads for the relations.crossref_metadata_degrees SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    relations_crossref_metadata: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_CROSSREF_METADATA_THREADS",
            help="Number of threads for the relations.crossref_metadata SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    relations_data_citation_corpus: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_DATA_CITATION_CORPUS_THREADS",
            help="Number of threads for the relations.data_citation_corpus SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    relations_datacite_degrees: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_DATACITE_DEGREES_THREADS",
            help="Number of threads for the relations.datacite_degrees SQLMesh model",
        ),
    ] = DUCKDB_RELATIONS_DATACITE_THREADS
    relations_datacite: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_DATACITE_THREADS",
            help="Number of threads for the relations.datacite SQLMesh model",
        ),
    ] = DUCKDB_RELATIONS_DATACITE_THREADS
    relations_relations_index: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_RELATIONS_INDEX_THREADS",
            help="Number of threads for the relations.relations_index SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    ror_index: Annotated[
        int,
        Parameter(
            env_var="ROR_INDEX_THREADS",
            help="Number of threads for the ror.index SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    ror_ror: Annotated[
        int,
        Parameter(
            env_var="ROR_ROR_THREADS",
            help="Number of threads for the ror.ror SQLMesh model",
        ),
    ] = DUCKDB_THREADS
    works_index_export: Annotated[
        int,
        Parameter(
            env_var="WORKS_INDEX_EXPORT_THREADS",
            help="Number of threads for the works_index.export SQLMesh model",
        ),
    ] = DUCKDB_THREADS
