from __future__ import annotations

from dataclasses import dataclass
import pathlib
from typing import Annotated, Literal

from cyclopts import Parameter, validators
import pendulum
import pendulum.parsing

DEFAULT_DUCKDB_THREADS = 32
DEFAULT_DUCKDB_RELATIONS_DATACITE_THREADS = 16


def validate_date_str(type_, value):  # noqa: ARG001
    """Validate that a string is in YYYY-MM-DD format.

    Args:
        type_: The type of the value (unused).
        value: The string to validate.

    Raises:
        ValueError: If the string is not in the correct format.
    """
    try:
        pendulum.from_format(value, "YYYY-MM-DD")
    except pendulum.parsing.exceptions.ParserError as e:
        raise ValueError(f"Invalid date: '{value}'. Must be in YYYY-MM-DD format.") from e


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
DateString = Annotated[str, Parameter(validator=validate_date_str)]
BatchSize = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of input files to process per batch (must be >= 1).",
    ),
]
RowGroupSize = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Parquet row group size (must be >= 1). For efficient downstream querying, target row group sizes of 128-512MB. Row groups are buffered fully in memory before being flushed to disk.",
    ),
]
RowGroupsPerFile = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of row groups per Parquet file (must be >= 1). Target file sizes of 512MB-1GB.",
    ),
]
MaxWorkers = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of workers to run in parallel (must be >= 1).",
    ),
]


@dataclass
class DatasetSubset:
    """Cyclopts configuration for creating a subset of datasets.

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
class DMPSubset:
    """Cyclopts configuration for creating a subset of DMPs.

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
class MySQLConfig:
    """Cyclopts configuration for MySQL connection.

    Attributes:
        mysql_host: MySQL hostname.
        mysql_tcp_port: MySQL port.
        mysql_user: MySQL user name.
        mysql_database: MySQL database name.
        mysql_pwd: MySQL password.
    """

    mysql_host: Annotated[str, Parameter(env_var="MYSQL_HOST", help="MySQL hostname")]
    mysql_tcp_port: Annotated[int, Parameter(env_var="MYSQL_TCP_PORT", help="MySQL port")]
    mysql_user: Annotated[str, Parameter(env_var="MYSQL_USER", help="MySQL user name")]
    mysql_database: Annotated[str, Parameter(env_var="MYSQL_DATABASE", help="MySQL database name")]
    mysql_pwd: Annotated[str, Parameter(env_var="MYSQL_PWD", help="MySQL password")]


@Parameter(name="*")
@dataclass
class CrossrefMetadataTransformConfig:
    """Cyclopts configuration for Crossref Metadata transformation.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of workers to run in parallel.
        log_level: Python log level.
    """

    batch_size: BatchSize = 500
    row_group_size: RowGroupSize = 500_000
    row_groups_per_file: RowGroupsPerFile = 4
    max_workers: MaxWorkers = 32
    log_level: LogLevel = "INFO"


@Parameter(name="*")
@dataclass
class OpenAlexWorksTransformConfig:
    """Cyclopts configuration for OpenAlex Works transformation.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of workers to run in parallel.
        log_level: Python log level.
    """

    batch_size: BatchSize = 16
    row_group_size: RowGroupSize = 200_000
    row_groups_per_file: RowGroupsPerFile = 4
    max_workers: MaxWorkers = 32
    log_level: LogLevel = "INFO"


@Parameter(name="*")
@dataclass
class DataCiteTransformConfig:
    """Cyclopts configuration for DataCite transformation.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of workers to run in parallel.
        log_level: Python log level.
    """

    batch_size: BatchSize = 150
    row_group_size: RowGroupSize = 250_000
    row_groups_per_file: RowGroupsPerFile = 8
    max_workers: MaxWorkers = 8
    log_level: LogLevel = "INFO"


@dataclass
class SQLMeshThreadsConfig:
    """Cyclopts configuration for SQLMesh threads.

    Attributes:
        crossref_crossref_metadata: Number of threads for the crossref.crossref_metadata SQLMesh model.
        crossref_index_works_metadata: Number of threads for the crossref_index.works_metadata SQLMesh model.
        datacite_datacite: Number of threads for the datacite.datacite SQLMesh model.
        datacite_index_awards: Number of threads for the datacite_index.awards SQLMesh model.
        datacite_index_datacite_index: Number of threads for the datacite_index.datacite_index SQLMesh model.
        datacite_index_funders: Number of threads for the datacite_index.funders SQLMesh model.
        datacite_index_institutions: Number of threads for the datacite_index.institutions SQLMesh model.
        datacite_index_updated_dates: Number of threads for the datacite_index.updated_dates SQLMesh model.
        datacite_index_work_types: Number of threads for the datacite_index.work_types SQLMesh model.
        datacite_index_works: Number of threads for the datacite_index.works SQLMesh model.
        datacite_index_datacite_index_hashes: Number of threads for the datacite_index.datacite_index_hashes SQLMesh model.
        openalex_openalex_works: Number of threads for the openalex.openalex_works SQLMesh model.
        openalex_index_abstract_stats: Number of threads for the openalex_index.abstract_stats SQLMesh model.
        openalex_index_abstracts: Number of threads for the openalex_index.abstracts SQLMesh model.
        openalex_index_author_names: Number of threads for the openalex_index.author_names SQLMesh model.
        openalex_index_awards: Number of threads for the openalex_index.awards SQLMesh model.
        openalex_index_funders: Number of threads for the openalex_index.funders SQLMesh model.
        openalex_index_openalex_index: Number of threads for the openalex_index.openalex_index SQLMesh model.
        openalex_index_publication_dates: Number of threads for the openalex_index.publication_dates SQLMesh model.
        openalex_index_title_stats: Number of threads for the openalex_index.title_stats SQLMesh model.
        openalex_index_titles: Number of threads for the openalex_index.titles SQLMesh model.
        openalex_index_updated_dates: Number of threads for the openalex_index.updated_dates SQLMesh model.
        openalex_index_works_metadata: Number of threads for the openalex_index.works_metadata SQLMesh model.
        openalex_index_openalex_index_hashes: Number of threads for the openalex_index.openalex_index_hashes SQLMesh model.
        opensearch_current_doi_state: Number of threads for the opensearch.current_doi_state SQLMesh model.
        opensearch_export: Number of threads for the opensearch.export SQLMesh model.
        opensearch_next_doi_state: Number of threads for the opensearch.next_doi_state SQLMesh model.
        data_citation_corpus_relations: Number of threads for the data_citation_corpus.relations SQLMesh model.
        relations_crossref_metadata: Number of threads for the relations.crossref_metadata SQLMesh model.
        relations_data_citation_corpus: Number of threads for the relations.data_citation_corpus SQLMesh model.
        relations_datacite: Number of threads for the relations.datacite SQLMesh model.
        relations_relations_index: Number of threads for the relations.relations_index SQLMesh model.
        ror_index: Number of threads for the ror.index SQLMesh model.
        ror_ror: Number of threads for the ror.ror SQLMesh model.
        works_index_export: Number of threads for the works_index.export SQLMesh model.
    """

    crossref_crossref_metadata: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_CROSSREF_METADATA_THREADS",
            help="Number of threads for the crossref.crossref_metadata SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    crossref_index_works_metadata: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_INDEX_WORKS_METADATA_THREADS",
            help="Number of threads for the crossref_index.works_metadata SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_datacite: Annotated[
        int,
        Parameter(
            env_var="DATACITE_DATACITE_THREADS",
            help="Number of threads for the datacite.datacite SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_index_awards: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_AWARDS_THREADS",
            help="Number of threads for the datacite_index.awards SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_index_datacite_index: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_DATACITE_INDEX_THREADS",
            help="Number of threads for the datacite_index.datacite_index SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_index_funders: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_FUNDERS_THREADS",
            help="Number of threads for the datacite_index.funders SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_index_institutions: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_INSTITUTIONS_THREADS",
            help="Number of threads for the datacite_index.institutions SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_index_updated_dates: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_UPDATED_DATES_THREADS",
            help="Number of threads for the datacite_index.updated_dates SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_index_work_types: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_WORK_TYPES_THREADS",
            help="Number of threads for the datacite_index.work_types SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_index_works: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_WORKS_THREADS",
            help="Number of threads for the datacite_index.works SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    datacite_index_datacite_index_hashes: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_DATACITE_INDEX_HASHES_THREADS",
            help="Number of threads for the datacite_index.datacite_index_hashes SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_openalex_works: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_OPENALEX_WORKS_THREADS",
            help="Number of threads for the openalex.openalex_works SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_abstract_stats: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_ABSTRACT_STATS_THREADS",
            help="Number of threads for the openalex_index.abstract_stats SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_abstracts: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_ABSTRACTS_THREADS",
            help="Number of threads for the openalex_index.abstracts SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_author_names: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_AUTHOR_NAMES_THREADS",
            help="Number of threads for the openalex_index.author_names SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_awards: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_AWARDS_THREADS",
            help="Number of threads for the openalex_index.awards SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_funders: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_FUNDERS_THREADS",
            help="Number of threads for the openalex_index.funders SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_openalex_index: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_OPENALEX_INDEX_THREADS",
            help="Number of threads for the openalex_index.openalex_index SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_publication_dates: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_PUBLICATION_DATES_THREADS",
            help="Number of threads for the openalex_index.publication_dates SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_title_stats: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_TITLE_STATS_THREADS",
            help="Number of threads for the openalex_index.title_stats SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_titles: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_TITLES_THREADS",
            help="Number of threads for the openalex_index.titles SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_updated_dates: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_UPDATED_DATES_THREADS",
            help="Number of threads for the openalex_index.updated_dates SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_works_metadata: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_WORKS_METADATA_THREADS",
            help="Number of threads for the openalex_index.works_metadata SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    openalex_index_openalex_index_hashes: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_OPENALEX_INDEX_HASHES_THREADS",
            help="Number of threads for the openalex_index.openalex_index_hashes SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    opensearch_current_doi_state: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_CURRENT_DOI_STATE_THREADS",
            help="Number of threads for the opensearch.current_doi_state SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    opensearch_export: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_EXPORT_THREADS",
            help="Number of threads for the opensearch.export SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    opensearch_next_doi_state: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_NEXT_DOI_STATE_THREADS",
            help="Number of threads for the opensearch.next_doi_state SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    data_citation_corpus_relations: Annotated[
        int,
        Parameter(
            env_var="DATA_CITATION_CORPUS_RELATIONS_THREADS",
            help="Number of threads for the data_citation_corpus.relations SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    relations_crossref_metadata: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_CROSSREF_METADATA_THREADS",
            help="Number of threads for the relations.crossref_metadata SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    relations_data_citation_corpus: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_DATA_CITATION_CORPUS_THREADS",
            help="Number of threads for the relations.data_citation_corpus SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    relations_datacite: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_DATACITE_THREADS",
            help="Number of threads for the relations.datacite SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_RELATIONS_DATACITE_THREADS
    relations_relations_index: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_RELATIONS_INDEX_THREADS",
            help="Number of threads for the relations.relations_index SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    ror_index: Annotated[
        int,
        Parameter(
            env_var="ROR_INDEX_THREADS",
            help="Number of threads for the ror.index SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    ror_ror: Annotated[
        int,
        Parameter(
            env_var="ROR_ROR_THREADS",
            help="Number of threads for the ror.ror SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
    works_index_export: Annotated[
        int,
        Parameter(
            env_var="WORKS_INDEX_EXPORT_THREADS",
            help="Number of threads for the works_index.export SQLMesh model",
        ),
    ] = DEFAULT_DUCKDB_THREADS
