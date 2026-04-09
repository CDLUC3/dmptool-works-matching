"""Lambda environment settings and SSM YAML config models for the dmpworks scheduler."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

import boto3
from pydantic import BaseModel
from pydantic_settings import BaseSettings
import yaml

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
    DMP_WORKS_SEARCH_RECORDS_PER_FILE,
    DMP_WORKS_SEARCH_SCROLL_TIME,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_RELATIONS_DATACITE_THREADS,
    DUCKDB_THREADS,
    MAX_DOI_STATES,
    MAX_RELATION_DEGREES,
    MERGE_RELATED_WORKS_INSERT_BATCH_SIZE,
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


class LambdaEnvSettings(BaseSettings):
    """Minimal Lambda environment variables set by CloudFormation at deploy time.

    Attributes:
        aws_env: The AWS environment name (e.g. dev, stg, prd).
        aws_region: AWS region (automatically injected by the Lambda runtime).
    """

    aws_env: str
    aws_region: str


class VersionCheckerEnvSettings(LambdaEnvSettings):
    """Extended settings for the version checker Lambda.

    Attributes:
        bucket_name: Main dmpworks S3 bucket name.
        state_machine_arn: Step Functions state machine ARN.
        datacite_credentials_secret_arn: ARN of the Secrets Manager secret containing DataCite credentials.
    """

    bucket_name: str
    state_machine_arn: str
    datacite_credentials_secret_arn: str


class StartProcessEnvSettings(LambdaEnvSettings):
    """Extended settings for start-process-works and start-process-dmps Lambdas.

    Attributes:
        bucket_name: Main dmpworks S3 bucket name.
        state_machine_arn: ARN of the state machine to start.
    """

    bucket_name: str
    state_machine_arn: str


class S3CleanupEnvSettings(LambdaEnvSettings):
    """Settings for the monthly S3 cleanup Lambda.

    Attributes:
        bucket_name: Main dmpworks S3 bucket name.
    """

    bucket_name: str


# ---------------------------------------------------------------------------
# Sub-models (mirror the SSM YAML structure)
# ---------------------------------------------------------------------------


class DatasetSubsetConfig(BaseModel):
    """Configuration for dataset subset filtering (works).

    Attributes:
        enable: Whether to enable subset filtering.
        institutions_s3_path: S3 path to a JSON file of institution ROR IDs.
        dois_s3_path: S3 path to a JSON file of specific work DOIs.
    """

    enable: bool = False
    institutions_s3_path: str = "meta/institutions.json"
    dois_s3_path: str = "meta/work_dois.json"


class DmpSubsetConfig(BaseModel):
    """Configuration for DMP subset filtering.

    Attributes:
        enable: Whether to enable subset filtering.
        institutions_s3_path: S3 path to a JSON file of institution ROR IDs.
        dois_s3_path: S3 path to a JSON file of specific DMP DOIs.
    """

    enable: bool = False
    institutions_s3_path: str = "meta/dmp_institutions.json"
    dois_s3_path: str = "meta/dmp_dois.json"


class CrossrefMetadataConfig(BaseModel):
    """Source configuration for Crossref Metadata.

    Attributes:
        bucket_name: S3 bucket containing Crossref Metadata snapshots.
    """

    bucket_name: str


class CrossrefMetadataTransformConfig(BaseModel):
    """Transform configuration for Crossref Metadata.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of parallel workers.
    """

    batch_size: int = CROSSREF_METADATA_TRANSFORM_BATCH_SIZE
    row_group_size: int = CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE
    row_groups_per_file: int = CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE
    max_workers: int = CROSSREF_METADATA_TRANSFORM_MAX_WORKERS


class DataciteConfig(BaseModel):
    """Source configuration for DataCite.

    Attributes:
        bucket_name: S3 bucket containing DataCite monthly data files.
        bucket_region: AWS region of the DataCite bucket.
    """

    bucket_name: str
    bucket_region: str


class DataciteTransformConfig(BaseModel):
    """Transform configuration for DataCite.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of parallel workers.
    """

    batch_size: int = DATACITE_TRANSFORM_BATCH_SIZE
    row_group_size: int = DATACITE_TRANSFORM_ROW_GROUP_SIZE
    row_groups_per_file: int = DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE
    max_workers: int = DATACITE_TRANSFORM_MAX_WORKERS


class OpenalexWorksConfig(BaseModel):
    """Source configuration for OpenAlex Works.

    Attributes:
        bucket_name: S3 bucket containing OpenAlex data.
    """

    bucket_name: str


class OpenalexWorksTransformConfig(BaseModel):
    """Transform configuration for OpenAlex Works.

    Attributes:
        batch_size: Number of input files to process per batch.
        row_group_size: Parquet row group size.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Number of parallel workers.
        include_xpac: Whether to include works flagged as xpac.
    """

    batch_size: int = OPENALEX_WORKS_TRANSFORM_BATCH_SIZE
    row_group_size: int = OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE
    row_groups_per_file: int = OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE
    max_workers: int = OPENALEX_WORKS_TRANSFORM_MAX_WORKERS
    include_xpac: bool = OPENALEX_WORKS_TRANSFORM_INCLUDE_XPAC


class DmpWorksSearchConfig(BaseModel):
    """Configuration for the DMP-works search step.

    Attributes:
        query_builder: Name of the query builder function to use.
        rerank_model: Optional re-ranking model name.
        scroll_time: Duration the OpenSearch scroll context stays active.
        batch_size: Number of DMPs processed per batch.
        max_results: Maximum works returned per DMP.
        project_end_buffer_years: Extra years added to project end date for search.
        parallel_search: Whether to use msearch for parallel execution.
        include_named_queries_score: Whether to include sub-query scores.
        max_concurrent_searches: Maximum concurrent OpenSearch searches.
        max_concurrent_shard_requests: Maximum shards searched per node per request.
        inner_hits_size: Maximum inner hits returned per matched work.
        row_group_size: Parquet row group size for output files.
        row_groups_per_file: Number of row groups per output Parquet file.
        dmps_start_date: Filter DMPs with project start on or after this date (YYYY-MM-DD).
        dmps_end_date: Filter DMPs with project start on or before this date (YYYY-MM-DD).
        dmp_modification_window_days: Only search DMPs modified within this many days. None means all DMPs.
        apply_modification_window: Whether to apply the modification window filter.
    """

    query_builder: str = DMP_WORKS_SEARCH_QUERY_BUILDER
    rerank_model: str | None = None
    scroll_time: str = DMP_WORKS_SEARCH_SCROLL_TIME
    batch_size: int = DMP_WORKS_SEARCH_BATCH_SIZE
    max_results: int = DMP_WORKS_SEARCH_MAX_RESULTS
    project_end_buffer_years: int = DMP_WORKS_SEARCH_PROJECT_END_BUFFER_YEARS
    parallel_search: bool = False
    include_named_queries_score: bool = True
    max_concurrent_searches: int = DMP_WORKS_SEARCH_MAX_CONCURRENT_SEARCHES
    max_concurrent_shard_requests: int = DMP_WORKS_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS
    inner_hits_size: int = DMP_WORKS_SEARCH_INNER_HITS_SIZE
    records_per_file: int = DMP_WORKS_SEARCH_RECORDS_PER_FILE
    dmps_start_date: str | None = None
    dmps_end_date: str | None = None
    dmp_modification_window_days: int | None = None
    apply_modification_window: bool = True


class OpenSearchClientConfig(BaseModel):
    """Configuration for the OpenSearch client.

    Attributes:
        host: OpenSearch hostname or domain endpoint.
        port: OpenSearch HTTPS port.
        use_ssl: Whether to use SSL.
        verify_certs: Whether to verify SSL certificates.
        auth_type: Authentication type (aws or basic).
        region: AWS region for SigV4 signing.
        service: AWS service name for SigV4 signing.
        pool_maxsize: Maximum connections in the pool.
        timeout: Request timeout in seconds.
    """

    host: str
    port: int = 443
    use_ssl: bool = True
    verify_certs: bool = True
    auth_type: Literal["aws", "basic"] = "aws"
    region: str = "us-west-2"
    service: str = "es"
    pool_maxsize: int = 20
    timeout: int = 300


class OpenSearchSyncConfig(BaseModel):
    """Configuration for syncing data to OpenSearch.

    Attributes:
        max_processes: Maximum parallel worker processes.
        chunk_size: Records per batch.
        max_chunk_bytes: Maximum serialized batch size in bytes.
        max_retries: Maximum retry attempts per batch.
        initial_backoff: Initial retry backoff in seconds.
        max_backoff: Maximum retry backoff in seconds.
        dry_run: Run without writing to OpenSearch.
        measure_chunk_size: Measure serialized size before sending.
        max_error_samples: Maximum error examples to retain.
        staggered_start: Stagger worker startup to reduce load spikes.
    """

    max_processes: int = OPENSEARCH_SYNC_MAX_PROCESSES
    chunk_size: int = OPENSEARCH_SYNC_CHUNK_SIZE
    max_chunk_bytes: int = OPENSEARCH_SYNC_MAX_CHUNK_BYTES
    max_retries: int = OPENSEARCH_SYNC_MAX_RETRIES
    initial_backoff: int = OPENSEARCH_SYNC_INITIAL_BACKOFF
    max_backoff: int = OPENSEARCH_SYNC_MAX_BACKOFF
    dry_run: bool = False
    measure_chunk_size: bool = False
    max_error_samples: int = OPENSEARCH_SYNC_MAX_ERROR_SAMPLES
    staggered_start: bool = False


class MergeRelatedWorksConfig(BaseModel):
    """Configuration for the merge-related-works step.

    Attributes:
        insert_batch_size: Number of rows per SQL INSERT batch.
    """

    insert_batch_size: int = MERGE_RELATED_WORKS_INSERT_BATCH_SIZE


class SQLMeshConfig(BaseModel):
    """Configuration for SQLMesh execution and DuckDB settings.

    Attributes:
        duckdb_threads: Number of DuckDB threads.
        duckdb_memory_limit: DuckDB memory limit string (e.g. "225GB").
        audit_crossref_metadata_works_threshold: Minimum expected Crossref works count.
        audit_datacite_works_threshold: Minimum expected DataCite works count.
        audit_nested_object_limit: Nested object audit limit.
        audit_openalex_works_threshold: Minimum expected OpenAlex works count.
        max_doi_states: Maximum DOI states to retain per DOI.
        max_relation_degrees: Maximum relation graph degrees.
        crossref_crossref_metadata: Threads for crossref.crossref_metadata model.
        crossref_index_works_metadata: Threads for crossref_index.works_metadata model.
        data_citation_corpus: Threads for data_citation_corpus.relations model.
        datacite_datacite: Threads for datacite.datacite model.
        datacite_index_awards: Threads for datacite_index.awards model.
        datacite_index_datacite_index: Threads for datacite_index.datacite_index model.
        datacite_index_funders: Threads for datacite_index.funders model.
        datacite_index_institutions: Threads for datacite_index.institutions model.
        datacite_index_updated_dates: Threads for datacite_index.updated_dates model.
        datacite_index_work_types: Threads for datacite_index.work_types model.
        datacite_index_works: Threads for datacite_index.works model.
        datacite_index_datacite_index_hashes: Threads for datacite_index.datacite_index_hashes model.
        openalex_openalex_works: Threads for openalex.openalex_works model.
        openalex_index_abstract_stats: Threads for openalex_index.abstract_stats model.
        openalex_index_abstracts: Threads for openalex_index.abstracts model.
        openalex_index_author_names: Threads for openalex_index.author_names model.
        openalex_index_awards: Threads for openalex_index.awards model.
        openalex_index_funders: Threads for openalex_index.funders model.
        openalex_index_openalex_index: Threads for openalex_index.openalex_index model.
        openalex_index_publication_dates: Threads for openalex_index.publication_dates model.
        openalex_index_title_stats: Threads for openalex_index.title_stats model.
        openalex_index_titles: Threads for openalex_index.titles model.
        openalex_index_updated_dates: Threads for openalex_index.updated_dates model.
        openalex_index_works_metadata: Threads for openalex_index.works_metadata model.
        openalex_index_openalex_index_hashes: Threads for openalex_index.openalex_index_hashes model.
        opensearch_current_doi_state: Threads for opensearch.current_doi_state model.
        opensearch_export: Threads for opensearch.export model.
        opensearch_next_doi_state: Threads for opensearch.next_doi_state model.
        relations_crossref_metadata_degrees: Threads for relations.crossref_metadata_degrees model.
        relations_crossref_metadata: Threads for relations.crossref_metadata model.
        relations_data_citation_corpus: Threads for relations.data_citation_corpus model.
        relations_datacite_degrees: Threads for relations.datacite_degrees model.
        relations_datacite: Threads for relations.datacite model.
        relations_relations_index: Threads for relations.relations_index model.
        ror_index: Threads for ror.index model.
        ror_ror: Threads for ror.ror model.
        works_index_export: Threads for works_index.export model.
    """

    duckdb_threads: int = DUCKDB_THREADS
    duckdb_memory_limit: str = DUCKDB_MEMORY_LIMIT

    audit_crossref_metadata_works_threshold: int = AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD
    audit_datacite_works_threshold: int = AUDIT_DATACITE_WORKS_THRESHOLD
    audit_nested_object_limit: int = AUDIT_NESTED_OBJECT_LIMIT
    audit_openalex_works_threshold: int = AUDIT_OPENALEX_WORKS_THRESHOLD

    max_doi_states: int = MAX_DOI_STATES
    max_relation_degrees: int = MAX_RELATION_DEGREES

    crossref_crossref_metadata: int = DUCKDB_THREADS
    crossref_index_works_metadata: int = DUCKDB_THREADS

    data_citation_corpus: int = DUCKDB_THREADS

    datacite_datacite: int = DUCKDB_THREADS
    datacite_index_awards: int = DUCKDB_THREADS
    datacite_index_datacite_index: int = DUCKDB_THREADS
    datacite_index_funders: int = DUCKDB_THREADS
    datacite_index_institutions: int = DUCKDB_THREADS
    datacite_index_updated_dates: int = DUCKDB_THREADS
    datacite_index_work_types: int = DUCKDB_THREADS
    datacite_index_works: int = DUCKDB_THREADS
    datacite_index_datacite_index_hashes: int = DUCKDB_THREADS

    openalex_openalex_works: int = DUCKDB_THREADS
    openalex_index_abstract_stats: int = DUCKDB_THREADS
    openalex_index_abstracts: int = DUCKDB_THREADS
    openalex_index_author_names: int = DUCKDB_THREADS
    openalex_index_awards: int = DUCKDB_THREADS
    openalex_index_funders: int = DUCKDB_THREADS
    openalex_index_openalex_index: int = DUCKDB_THREADS
    openalex_index_publication_dates: int = DUCKDB_THREADS
    openalex_index_title_stats: int = DUCKDB_THREADS
    openalex_index_titles: int = DUCKDB_THREADS
    openalex_index_updated_dates: int = DUCKDB_THREADS
    openalex_index_works_metadata: int = DUCKDB_THREADS
    openalex_index_openalex_index_hashes: int = DUCKDB_THREADS

    opensearch_current_doi_state: int = DUCKDB_THREADS
    opensearch_export: int = DUCKDB_THREADS
    opensearch_next_doi_state: int = DUCKDB_THREADS

    relations_crossref_metadata_degrees: int = DUCKDB_THREADS
    relations_crossref_metadata: int = DUCKDB_THREADS
    relations_data_citation_corpus: int = DUCKDB_THREADS
    relations_datacite_degrees: int = DUCKDB_RELATIONS_DATACITE_THREADS
    relations_datacite: int = DUCKDB_RELATIONS_DATACITE_THREADS
    relations_relations_index: int = DUCKDB_THREADS

    ror_index: int = DUCKDB_THREADS
    ror_ror: int = DUCKDB_THREADS

    works_index_export: int = DUCKDB_THREADS


# ---------------------------------------------------------------------------
# Root config model
# ---------------------------------------------------------------------------


class LambdaConfig(BaseModel):
    """Root configuration loaded from the SSM YAML parameter.

    Required fields (no defaults): crossref_metadata_config, datacite_config,
    openalex_works_config, opensearch_client_config.

    Attributes:
        enabled_datasets: List of datasets to enable (e.g. ["ror", "datacite"]).
        dataset_subset: Dataset subset filtering configuration for works.
        dmp_subset: Dataset subset filtering configuration for DMPs.
        crossref_metadata_config: Crossref Metadata source configuration.
        crossref_metadata_transform_config: Crossref Metadata transform settings.
        datacite_config: DataCite source configuration.
        datacite_transform_config: DataCite transform settings.
        openalex_works_config: OpenAlex Works source configuration.
        openalex_works_transform_config: OpenAlex Works transform settings.
        dmp_works_search_config: DMP-works search settings.
        opensearch_client_config: OpenSearch client connection settings.
        opensearch_sync_config: OpenSearch sync settings.
        merge_related_works_config: Merge related works settings.
        sqlmesh_config: SQLMesh execution and DuckDB settings.
    """

    enabled_datasets: list[str] = []
    dataset_subset: DatasetSubsetConfig = DatasetSubsetConfig()
    dmp_subset: DmpSubsetConfig = DmpSubsetConfig()
    crossref_metadata_config: CrossrefMetadataConfig
    crossref_metadata_transform_config: CrossrefMetadataTransformConfig = CrossrefMetadataTransformConfig()
    datacite_config: DataciteConfig
    datacite_transform_config: DataciteTransformConfig = DataciteTransformConfig()
    openalex_works_config: OpenalexWorksConfig
    openalex_works_transform_config: OpenalexWorksTransformConfig = OpenalexWorksTransformConfig()
    dmp_works_search_config: DmpWorksSearchConfig = DmpWorksSearchConfig()
    opensearch_client_config: OpenSearchClientConfig
    opensearch_sync_config: OpenSearchSyncConfig = OpenSearchSyncConfig()
    merge_related_works_config: MergeRelatedWorksConfig = MergeRelatedWorksConfig()
    sqlmesh_config: SQLMeshConfig = SQLMeshConfig()

    def to_env_dict(self) -> dict[str, str | None]:
        """Return a flat dict mapping env var names to string values for Batch container overrides.

        Booleans are lowercased to match env var conventions (e.g. "true"/"false").
        All non-None values are coerced to str. Optional fields are returned as None
        so that make_env / compute_batch_params can filter them out.

        Returns:
            dict mapping uppercase env var names to string values (or None for absent optional fields).
        """

        def s(v: object) -> str:
            if isinstance(v, bool):
                return str(v).lower()
            return str(v)

        t = self.crossref_metadata_transform_config
        o = self.openalex_works_transform_config
        d = self.datacite_transform_config
        sub = self.dataset_subset
        dmp = self.dmp_subset
        dws = self.dmp_works_search_config
        osc = self.opensearch_client_config
        oss = self.opensearch_sync_config
        mrw = self.merge_related_works_config
        sm = self.sqlmesh_config

        return {
            # Crossref Metadata
            "CROSSREF_METADATA_BUCKET_NAME": self.crossref_metadata_config.bucket_name,
            "CROSSREF_METADATA_TRANSFORM_BATCH_SIZE": s(t.batch_size),
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE": s(t.row_group_size),
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE": s(t.row_groups_per_file),
            "CROSSREF_METADATA_TRANSFORM_MAX_WORKERS": s(t.max_workers),
            # DataCite
            "DATACITE_BUCKET_NAME": self.datacite_config.bucket_name,
            "DATACITE_BUCKET_REGION": self.datacite_config.bucket_region,
            "DATACITE_TRANSFORM_BATCH_SIZE": s(d.batch_size),
            "DATACITE_TRANSFORM_ROW_GROUP_SIZE": s(d.row_group_size),
            "DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE": s(d.row_groups_per_file),
            "DATACITE_TRANSFORM_MAX_WORKERS": s(d.max_workers),
            # OpenAlex Works
            "OPENALEX_BUCKET_NAME": self.openalex_works_config.bucket_name,
            "OPENALEX_WORKS_TRANSFORM_BATCH_SIZE": s(o.batch_size),
            "OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE": s(o.row_group_size),
            "OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE": s(o.row_groups_per_file),
            "OPENALEX_WORKS_TRANSFORM_MAX_WORKERS": s(o.max_workers),
            "OPENALEX_WORKS_TRANSFORM_INCLUDE_XPAC": s(o.include_xpac),
            # Dataset subset (works)
            "DATASET_SUBSET_INSTITUTIONS_S3_PATH": sub.institutions_s3_path,
            "DATASET_SUBSET_DOIS_S3_PATH": sub.dois_s3_path,
            # DMP subset
            "DMP_SUBSET_ENABLE": s(dmp.enable),
            "DMP_SUBSET_INSTITUTIONS_S3_PATH": dmp.institutions_s3_path,
            "DMP_SUBSET_DOIS_S3_PATH": dmp.dois_s3_path,
            # DMP works search
            "DMP_WORKS_SEARCH_QUERY_BUILDER": dws.query_builder,
            "DMP_WORKS_SEARCH_RERANK_MODEL": dws.rerank_model,
            "DMP_WORKS_SEARCH_SCROLL_TIME": dws.scroll_time,
            "DMP_WORKS_SEARCH_BATCH_SIZE": s(dws.batch_size),
            "DMP_WORKS_SEARCH_MAX_RESULTS": s(dws.max_results),
            "DMP_WORKS_SEARCH_PROJECT_END_BUFFER_YEARS": s(dws.project_end_buffer_years),
            "DMP_WORKS_SEARCH_PARALLEL_SEARCH": s(dws.parallel_search),
            "DMP_WORKS_SEARCH_INCLUDE_NAMED_QUERIES_SCORE": s(dws.include_named_queries_score),
            "DMP_WORKS_SEARCH_MAX_CONCURRENT_SEARCHES": s(dws.max_concurrent_searches),
            "DMP_WORKS_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS": s(dws.max_concurrent_shard_requests),
            "DMP_WORKS_SEARCH_INNER_HITS_SIZE": s(dws.inner_hits_size),
            "DMP_WORKS_SEARCH_RECORDS_PER_FILE": s(dws.records_per_file),
            "DMP_WORKS_SEARCH_DMPS_START_DATE": dws.dmps_start_date,
            "DMP_WORKS_SEARCH_DMPS_END_DATE": dws.dmps_end_date,
            "DMP_WORKS_SEARCH_DMP_MODIFICATION_WINDOW_DAYS": (
                s(dws.dmp_modification_window_days) if dws.dmp_modification_window_days is not None else None
            ),
            "DMP_WORKS_SEARCH_APPLY_MODIFICATION_WINDOW": s(dws.apply_modification_window),
            # OpenSearch client
            "OPENSEARCH_HOST": osc.host,
            "OPENSEARCH_PORT": s(osc.port),
            "OPENSEARCH_USE_SSL": s(osc.use_ssl),
            "OPENSEARCH_VERIFY_CERTS": s(osc.verify_certs),
            "OPENSEARCH_AUTH_TYPE": osc.auth_type,
            "OPENSEARCH_REGION": osc.region,
            "OPENSEARCH_SERVICE": osc.service,
            "OPENSEARCH_POOL_MAXSIZE": s(osc.pool_maxsize),
            "OPENSEARCH_TIMEOUT": s(osc.timeout),
            # OpenSearch sync
            "OPENSEARCH_SYNC_MAX_PROCESSES": s(oss.max_processes),
            "OPENSEARCH_SYNC_CHUNK_SIZE": s(oss.chunk_size),
            "OPENSEARCH_SYNC_MAX_CHUNK_BYTES": s(oss.max_chunk_bytes),
            "OPENSEARCH_SYNC_MAX_RETRIES": s(oss.max_retries),
            "OPENSEARCH_SYNC_INITIAL_BACKOFF": s(oss.initial_backoff),
            "OPENSEARCH_SYNC_MAX_BACKOFF": s(oss.max_backoff),
            "OPENSEARCH_SYNC_DRY_RUN": s(oss.dry_run),
            "OPENSEARCH_SYNC_MEASURE_CHUNK_SIZE": s(oss.measure_chunk_size),
            "OPENSEARCH_SYNC_MAX_ERROR_SAMPLES": s(oss.max_error_samples),
            "OPENSEARCH_SYNC_STAGGERED_START": s(oss.staggered_start),
            # Merge related works
            "MERGE_RELATED_WORKS_INSERT_BATCH_SIZE": s(mrw.insert_batch_size),
            # SQLMesh / DuckDB
            "DUCKDB_THREADS": s(sm.duckdb_threads),
            "DUCKDB_MEMORY_LIMIT": sm.duckdb_memory_limit,
            "AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD": s(sm.audit_crossref_metadata_works_threshold),
            "AUDIT_DATACITE_WORKS_THRESHOLD": s(sm.audit_datacite_works_threshold),
            "AUDIT_NESTED_OBJECT_LIMIT": s(sm.audit_nested_object_limit),
            "AUDIT_OPENALEX_WORKS_THRESHOLD": s(sm.audit_openalex_works_threshold),
            "MAX_DOI_STATES": s(sm.max_doi_states),
            "MAX_RELATION_DEGREES": s(sm.max_relation_degrees),
            # SQLMesh per-model thread counts
            "CROSSREF_CROSSREF_METADATA_THREADS": s(sm.crossref_crossref_metadata),
            "CROSSREF_INDEX_WORKS_METADATA_THREADS": s(sm.crossref_index_works_metadata),
            "DATA_CITATION_CORPUS_THREADS": s(sm.data_citation_corpus),
            "DATACITE_DATACITE_THREADS": s(sm.datacite_datacite),
            "DATACITE_INDEX_AWARDS_THREADS": s(sm.datacite_index_awards),
            "DATACITE_INDEX_DATACITE_INDEX_THREADS": s(sm.datacite_index_datacite_index),
            "DATACITE_INDEX_FUNDERS_THREADS": s(sm.datacite_index_funders),
            "DATACITE_INDEX_INSTITUTIONS_THREADS": s(sm.datacite_index_institutions),
            "DATACITE_INDEX_UPDATED_DATES_THREADS": s(sm.datacite_index_updated_dates),
            "DATACITE_INDEX_WORK_TYPES_THREADS": s(sm.datacite_index_work_types),
            "DATACITE_INDEX_WORKS_THREADS": s(sm.datacite_index_works),
            "DATACITE_INDEX_DATACITE_INDEX_HASHES_THREADS": s(sm.datacite_index_datacite_index_hashes),
            "OPENALEX_OPENALEX_WORKS_THREADS": s(sm.openalex_openalex_works),
            "OPENALEX_INDEX_ABSTRACT_STATS_THREADS": s(sm.openalex_index_abstract_stats),
            "OPENALEX_INDEX_ABSTRACTS_THREADS": s(sm.openalex_index_abstracts),
            "OPENALEX_INDEX_AUTHOR_NAMES_THREADS": s(sm.openalex_index_author_names),
            "OPENALEX_INDEX_AWARDS_THREADS": s(sm.openalex_index_awards),
            "OPENALEX_INDEX_FUNDERS_THREADS": s(sm.openalex_index_funders),
            "OPENALEX_INDEX_OPENALEX_INDEX_THREADS": s(sm.openalex_index_openalex_index),
            "OPENALEX_INDEX_PUBLICATION_DATES_THREADS": s(sm.openalex_index_publication_dates),
            "OPENALEX_INDEX_TITLE_STATS_THREADS": s(sm.openalex_index_title_stats),
            "OPENALEX_INDEX_TITLES_THREADS": s(sm.openalex_index_titles),
            "OPENALEX_INDEX_UPDATED_DATES_THREADS": s(sm.openalex_index_updated_dates),
            "OPENALEX_INDEX_WORKS_METADATA_THREADS": s(sm.openalex_index_works_metadata),
            "OPENALEX_INDEX_OPENALEX_INDEX_HASHES_THREADS": s(sm.openalex_index_openalex_index_hashes),
            "OPENSEARCH_CURRENT_DOI_STATE_THREADS": s(sm.opensearch_current_doi_state),
            "OPENSEARCH_EXPORT_THREADS": s(sm.opensearch_export),
            "OPENSEARCH_NEXT_DOI_STATE_THREADS": s(sm.opensearch_next_doi_state),
            "RELATIONS_CROSSREF_METADATA_DEGREES_THREADS": s(sm.relations_crossref_metadata_degrees),
            "RELATIONS_CROSSREF_METADATA_THREADS": s(sm.relations_crossref_metadata),
            "RELATIONS_DATA_CITATION_CORPUS_THREADS": s(sm.relations_data_citation_corpus),
            "RELATIONS_DATACITE_DEGREES_THREADS": s(sm.relations_datacite_degrees),
            "RELATIONS_DATACITE_THREADS": s(sm.relations_datacite),
            "RELATIONS_RELATIONS_INDEX_THREADS": s(sm.relations_relations_index),
            "ROR_INDEX_THREADS": s(sm.ror_index),
            "ROR_ROR_THREADS": s(sm.ror_ror),
            "WORKS_INDEX_EXPORT_THREADS": s(sm.works_index_export),
        }


# ---------------------------------------------------------------------------
# SSM loader
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def load_lambda_config(aws_env: str) -> LambdaConfig:
    """Fetch and parse the Lambda YAML config from SSM.

    Cached so SSM is only called once per Lambda container lifecycle.

    Args:
        aws_env: The environment name (e.g. dev, stg, prd).

    Returns:
        Parsed and validated LambdaConfig instance.
    """
    ssm = boto3.client("ssm")
    param_name = f"/uc3/dmp/dmpworks/{aws_env}/LambdaConfig"
    response = ssm.get_parameter(Name=param_name)
    return LambdaConfig.model_validate(yaml.safe_load(response["Parameter"]["Value"]))
