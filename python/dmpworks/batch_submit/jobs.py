import logging
from typing import TypedDict

import boto3
import pendulum

from dmpworks.cli_utils import (
    CrossrefMetadataTransformConfig,
    DataCiteTransformConfig,
    DatasetSubset,
    DMPSubset,
    OpenAlexWorksTransformConfig,
    SQLMeshThreadsConfig,
)
from dmpworks.transform.dataset_subset import Dataset

# CPU and memory groups
NANO_VCPUS = 1
NANO_MEMORY = 1024

SMALL_VCPUS = 2
SMALL_MEMORY = 3686

MEDIUM_VCPUS = 8
MEDIUM_MEMORY = 28_762

LARGE_VCPUS = 32
LARGE_MEMORY = 58_368

VERY_LARGE_VCPUS = 32
VERY_LARGE_MEMORY = 250_880

TQDM_POSITION = "-1"
TQDM_MININTERVAL = "120"


_aws_batch_client = None


def get_aws_batch_client():
    """Get the AWS Batch client, creating it if it doesn't exist.

    Returns:
        boto3.client: The AWS Batch client.
    """
    global _aws_batch_client
    if _aws_batch_client is None:
        _aws_batch_client = boto3.client("batch")
    return _aws_batch_client


class EnvVarDict(TypedDict):
    """Type definition for an environment variable dictionary.

    Attributes:
        name: The name of the environment variable.
        value: The value of the environment variable.
    """

    name: str
    value: str


class DependsOnDict(TypedDict):
    """Type definition for a job dependency dictionary.

    Attributes:
        jobId: The ID of the job to depend on.
    """

    jobId: str


def format_date(date: pendulum.Date):
    """Format a date as YYYY-MM-DD.

    Args:
        date: The date to format.

    Returns:
        str: The formatted date string.
    """
    return date.format("YYYY-MM-DD")


def standard_job_definition(env: str) -> str:
    """Get the standard job definition name for the given environment.

    Args:
        env: The environment name (e.g., 'dev', 'prod').

    Returns:
        str: The job definition name.
    """
    return f"dmp-tool-{env}-batch-dmpworks-job"


def datacite_download_job_definition(env: str) -> str:
    """Get the DataCite download job definition name for the given environment.

    Args:
        env: The environment name.

    Returns:
        str: The job definition name.
    """
    return f"dmp-tool-{env}-batch-dmpworks-datacite-download-job"


def standard_job_queue(env: str) -> str:
    """Get the standard job queue name for the given environment.

    Args:
        env: The environment name.

    Returns:
        str: The job queue name.
    """
    return f"dmp-tool-{env}-batch-job-queue"


def database_job_definition(env: str) -> str:
    """Get the database job definition name for the given environment.

    Args:
        env: The environment name.

    Returns:
        str: The job definition name.
    """
    return f"dmp-tool-{env}-batch-dmpworks-database-job"


def submit_job(
    *,
    job_name: str,
    run_id: str,
    job_queue: str,
    job_definition: str,
    vcpus: int | None = None,
    memory: int | None = None,
    command: str | None = None,
    environment: list[EnvVarDict] | None = None,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submit a job to AWS Batch.

    Args:
        job_name: The name of the job.
        run_id: The unique run identifier.
        job_queue: The job queue to submit to.
        job_definition: The job definition to use.
        vcpus: The number of vCPUs to request.
        memory: The amount of memory to request (in MiB).
        command: The command to run.
        environment: A list of environment variables.
        depends_on: A list of job dependencies.

    Returns:
        str: The ID of the submitted job.
    """
    # Create container overrides
    container_overrides = {}
    if vcpus is not None:
        container_overrides["vcpus"] = vcpus
    if memory is not None:
        container_overrides["memory"] = memory
    if command is not None:
        container_overrides["command"] = ["/bin/bash", "-c", command]
    if environment is not None:
        container_overrides["environment"] = environment

    if depends_on is None:
        depends_on = []

    # Submit job
    batch_client = get_aws_batch_client()
    full_job_name = f"{job_name}-{run_id}"
    response = batch_client.submit_job(
        jobName=full_job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition,
        dependsOn=depends_on,
        containerOverrides=container_overrides,
    )
    job_id = response["jobId"]
    logging.info(f"Submitted job {full_job_name} with ID: {job_id}")

    return job_id


def make_env(env_vars: dict[str, str | None]) -> list[EnvVarDict]:
    """Create a list of environment variable dictionaries from a dictionary.

    Args:
        env_vars: A dictionary of environment variables.

    Returns:
        list[EnvVarDict]: A list of environment variable dictionaries.
    """
    return [{"name": k, "value": str(v)} for k, v in env_vars.items() if v is not None]


def dataset_subset_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    dataset: Dataset,
    dataset_subset: DatasetSubset,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the create dataset subset job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the file to.
        run_id: a unique ID to represent this run of the job.
        dataset: the dataset to create the subset for.
        dataset_subset: settings used to filter the dataset into a subset.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    logging.info("Creating dataset subset")
    logging.info(f"dataset_subset: {dataset_subset}")

    return submit_job(
        job_name=f"{dataset}-dataset-subset",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        command="dmpworks aws-batch $DATASET dataset-subset $BUCKET_NAME $RUN_ID",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "DATASET": dataset,
                "DATASET_SUBSET_ENABLE": str(dataset_subset.enable).lower(),
                "DATASET_SUBSET_INSTITUTIONS_S3_PATH": dataset_subset.institutions_s3_path,
                "DATASET_SUBSET_DOIS_S3_PATH": dataset_subset.dois_s3_path,
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        vcpus=vcpus,
        memory=memory,
        depends_on=depends_on,
    )


def ror_download_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    download_url: str,
    hash: str,
    vcpus: int = NANO_VCPUS,
    memory: int = NANO_MEMORY,
) -> str:
    """Submits the ROR data download job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the file to.
        run_id: a unique ID to represent this run of the job.
        download_url: the Zenodo download URL for the ROR data file.
        hash: the expected hash of the data file.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="ror-download",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        command="dmpworks aws-batch ror download $BUCKET_NAME $RUN_ID $DOWNLOAD_URL --hash $HASH",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "DOWNLOAD_URL": download_url,
                "HASH": hash,
            }
        ),
        vcpus=vcpus,
        memory=memory,
    )


def openalex_works_download_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    openalex_bucket_name: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
) -> str:
    """Submits the OpenAlex works data download job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the files to.
        run_id: a unique ID to represent this run of the job.
        openalex_bucket_name: Name of the OpenAlex AWS S3 bucket.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="openalex-works-download",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-works download $BUCKET_NAME $RUN_ID $OPENALEX_BUCKET_NAME",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "OPENALEX_BUCKET_NAME": openalex_bucket_name,
            }
        ),
    )


def openalex_works_transform_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    config: OpenAlexWorksTransformConfig,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenAlex works data transform job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the raw data.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: configuration for the transform job.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="openalex-works-transform",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-works transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --batch-size=$BATCH_SIZE --row-group-size=$ROW_GROUP_SIZE --row-groups-per-file=$ROW_GROUPS_PER_FILE --max-workers=$MAX_WORKERS log-level=$LOG_LEVEL",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "USE_SUBSET": str(use_subset).lower(),
                "BATCH_SIZE": str(config.batch_size),
                "ROW_GROUP_SIZE": str(config.row_group_size),
                "ROW_GROUPS_PER_FILE": str(config.row_groups_per_file),
                "MAX_WORKERS": str(config.max_workers),
                "LOG_LEVEL": config.log_level,
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        depends_on=depends_on,
    )


def crossref_metadata_download_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    file_name: str,
    crossref_bucket_name: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
) -> str:
    """Submits the Crossref metadata download job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the file to.
        run_id: a unique ID to represent this run of the job.
        file_name: the name of the Crossref metadata file to download.
        crossref_bucket_name: Name of the Crossref AWS S3 bucket.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="crossref-metadata-download",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch crossref-metadata download $BUCKET_NAME $RUN_ID $FILE_NAME $CROSSREF_BUCKET",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "FILE_NAME": file_name,
                "CROSSREF_BUCKET": crossref_bucket_name,
            }
        ),
    )


def crossref_metadata_transform_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    config: CrossrefMetadataTransformConfig,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the Crossref metadata transform job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the raw data.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: configuration for the transform job.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="crossref-metadata-transform",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch crossref-metadata transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --batch-size=$BATCH_SIZE --row-group-size=$ROW_GROUP_SIZE --row-groups-per-file=$ROW_GROUPS_PER_FILE --max-workers=$MAX_WORKERS --log-level=$LOG_LEVEL",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "USE_SUBSET": str(use_subset).lower(),
                "BATCH_SIZE": str(config.batch_size),
                "ROW_GROUP_SIZE": str(config.row_group_size),
                "ROW_GROUPS_PER_FILE": str(config.row_groups_per_file),
                "MAX_WORKERS": str(config.max_workers),
                "LOG_LEVEL": config.log_level,
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        depends_on=depends_on,
    )


def datacite_download_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    datacite_bucket_name: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
) -> str:
    """Submits the DataCite data download job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the file to.
        run_id: a unique ID to represent this run of the job.
        datacite_bucket_name: Name of the DataCite AWS S3 bucket.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="datacite-download",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=datacite_download_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch datacite download $BUCKET_NAME $RUN_ID $DATACITE_BUCKET_NAME",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "DATACITE_BUCKET_NAME": datacite_bucket_name,
            }
        ),
    )


def datacite_transform_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    config: DataCiteTransformConfig,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the DataCite data transform job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the raw data.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: configuration for the transform job.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="datacite-transform",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch datacite transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --batch-size=$BATCH_SIZE --row-group-size=$ROW_GROUP_SIZE --row-groups-per-file=$ROW_GROUPS_PER_FILE --max-workers=$MAX_WORKERS --log-level=$LOG_LEVEL",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "USE_SUBSET": str(use_subset).lower(),
                "BATCH_SIZE": str(config.batch_size),
                "ROW_GROUP_SIZE": str(config.row_group_size),
                "ROW_GROUPS_PER_FILE": str(config.row_groups_per_file),
                "MAX_WORKERS": str(config.max_workers),
                "LOG_LEVEL": config.log_level,
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        depends_on=depends_on,
    )


def submit_sqlmesh_job(
    *,
    env: str,
    bucket_name: str,
    prev_run_id: str,
    run_id: str,
    openalex_works_run_id: str,
    datacite_run_id: str,
    crossref_metadata_run_id: str,
    ror_run_id: str,
    data_citation_corpus_run_id: str,
    vcpus: int = VERY_LARGE_VCPUS,
    memory: int = VERY_LARGE_MEMORY,
    duckdb_memory_limit: str = "225GB",
    sqlmesh_threads_config: SQLMeshThreadsConfig,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the main SQLMesh transformation job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket for SQLMesh data.
        prev_run_id: a unique ID to represent the previous run of the SQLMesh job.
        run_id: a unique ID to represent this run of the SQLMesh job.
        openalex_works_run_id: The run_id of the OpenAlex works data to use.
        datacite_run_id: The run_id of the DataCite data to use.
        crossref_metadata_run_id: The run_id of the Crossref metadata to use.
        ror_run_id: The run_id of the ROR data to use.
        data_citation_corpus_run_id: The run_id of the Data Citation Corpus data to use.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        duckdb_memory_limit: Memory limit for DuckDB (e.g., "225GB").
        sqlmesh_threads_config: Config for SQLMesh threads.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="sqlmesh",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch sqlmesh plan $BUCKET_NAME $PREV_RUN_ID $RUN_ID --release-dates.openalex-works $OPENALEX_WORKS_RUN_ID --release-dates.datacite $DATACITE_RUN_ID --release-dates.crossref-metadata $CROSSREF_METADATA_RUN_ID --release-dates.ror $ROR_RUN_ID --release-dates.data-citation-corpus $DATA_CITATION_CORPUS_RUN_ID",
        environment=make_env(
            {
                "PREV_RUN_ID": prev_run_id,
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "OPENALEX_WORKS_RUN_ID": openalex_works_run_id,
                "DATACITE_RUN_ID": datacite_run_id,
                "CROSSREF_METADATA_RUN_ID": crossref_metadata_run_id,
                "ROR_RUN_ID": ror_run_id,
                "DATA_CITATION_CORPUS_RUN_ID": data_citation_corpus_run_id,
                "SQLMESH__GATEWAYS__DUCKDB__CONNECTION__CONNECTOR_CONFIG__MEMORY_LIMIT": duckdb_memory_limit,
                "SQLMESH__VARIABLES__CROSSREF_CROSSREF_METADATA_THREADS": sqlmesh_threads_config.crossref_crossref_metadata,
                "SQLMESH__VARIABLES__CROSSREF_INDEX_WORKS_METADATA_THREADS": sqlmesh_threads_config.crossref_index_works_metadata,
                "SQLMESH__VARIABLES__DATACITE_DATACITE_THREADS": sqlmesh_threads_config.datacite_datacite,
                "SQLMESH__VARIABLES__DATACITE_INDEX_AWARDS_THREADS": sqlmesh_threads_config.datacite_index_awards,
                "SQLMESH__VARIABLES__DATACITE_INDEX_DATACITE_INDEX_THREADS": sqlmesh_threads_config.datacite_index_datacite_index,
                "SQLMESH__VARIABLES__DATACITE_INDEX_FUNDERS_THREADS": sqlmesh_threads_config.datacite_index_funders,
                "SQLMESH__VARIABLES__DATACITE_INDEX_INSTITUTIONS_THREADS": sqlmesh_threads_config.datacite_index_institutions,
                "SQLMESH__VARIABLES__DATACITE_INDEX_UPDATED_DATES_THREADS": sqlmesh_threads_config.datacite_index_updated_dates,
                "SQLMESH__VARIABLES__DATACITE_INDEX_WORK_TYPES_THREADS": sqlmesh_threads_config.datacite_index_work_types,
                "SQLMESH__VARIABLES__DATACITE_INDEX_WORKS_THREADS": sqlmesh_threads_config.datacite_index_works,
                "SQLMESH__VARIABLES__DATACITE_INDEX_DATACITE_INDEX_HASHES_THREADS": sqlmesh_threads_config.datacite_index_datacite_index_hashes,
                "SQLMESH__VARIABLES__OPENALEX_OPENALEX_WORKS_THREADS": sqlmesh_threads_config.openalex_openalex_works,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_ABSTRACT_STATS_THREADS": sqlmesh_threads_config.openalex_index_abstract_stats,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_ABSTRACTS_THREADS": sqlmesh_threads_config.openalex_index_abstracts,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_AUTHOR_NAMES_THREADS": sqlmesh_threads_config.openalex_index_author_names,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_AWARDS_THREADS": sqlmesh_threads_config.openalex_index_awards,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_FUNDERS_THREADS": sqlmesh_threads_config.openalex_index_funders,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_OPENALEX_INDEX_THREADS": sqlmesh_threads_config.openalex_index_openalex_index,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_PUBLICATION_DATES_THREADS": sqlmesh_threads_config.openalex_index_publication_dates,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_TITLE_STATS_THREADS": sqlmesh_threads_config.openalex_index_title_stats,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_TITLES_THREADS": sqlmesh_threads_config.openalex_index_titles,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_UPDATED_DATES_THREADS": sqlmesh_threads_config.openalex_index_updated_dates,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_WORKS_METADATA_THREADS": sqlmesh_threads_config.openalex_index_works_metadata,
                "SQLMESH__VARIABLES__OPENALEX_INDEX_OPENALEX_INDEX_HASHES_THREADS": sqlmesh_threads_config.openalex_index_openalex_index_hashes,
                "SQLMESH__VARIABLES__OPENSEARCH_CURRENT_DOI_STATE_THREADS": sqlmesh_threads_config.opensearch_current_doi_state,
                "SQLMESH__VARIABLES__OPENSEARCH_EXPORT_THREADS": sqlmesh_threads_config.opensearch_export,
                "SQLMESH__VARIABLES__OPENSEARCH_NEXT_DOI_STATE_THREADS": sqlmesh_threads_config.opensearch_next_doi_state,
                "SQLMESH__VARIABLES__DATA_CITATION_CORPUS_RELATIONS_THREADS": sqlmesh_threads_config.data_citation_corpus_relations,
                "SQLMESH__VARIABLES__RELATIONS_CROSSREF_METADATA_THREADS": sqlmesh_threads_config.relations_crossref_metadata,
                "SQLMESH__VARIABLES__RELATIONS_DATA_CITATION_CORPUS_THREADS": sqlmesh_threads_config.relations_data_citation_corpus,
                "SQLMESH__VARIABLES__RELATIONS_DATACITE_THREADS": sqlmesh_threads_config.relations_datacite,
                "SQLMESH__VARIABLES__RELATIONS_RELATIONS_INDEX_THREADS": sqlmesh_threads_config.relations_relations_index,
                "SQLMESH__VARIABLES__ROR_INDEX_THREADS": sqlmesh_threads_config.ror_index,
                "SQLMESH__VARIABLES__ROR_ROR_THREADS": sqlmesh_threads_config.ror_ror,
                "SQLMESH__VARIABLES__WORKS_INDEX_EXPORT_THREADS": sqlmesh_threads_config.works_index_export,
            }
        ),
        depends_on=depends_on,
    )


def submit_sync_works_job(
    *,
    env: str,
    bucket_name: str,
    sqlmesh_run_id: str,
    host: str,
    region: str,
    index_name: str = "works-index",
    mode: str = "aws",
    port: int = 443,
    service: str = "es",
    max_processes: int = 16,
    chunk_size: int = 1000,
    max_retries: int = 3,
    vcpus: int = MEDIUM_VCPUS,
    memory: int = MEDIUM_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenSearch works-index sync job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the SQLMesh-transformed data.
        sqlmesh_run_id: the run_id of the SQLMesh job to sync from.
        host: the OpenSearch host URL.
        region: the AWS region of the OpenSearch cluster.
        index_name: The name of the OpenSearch index to sync to.
        mode: client connection mode (e.g., "aws").
        port: OpenSearch connection port.
        service: OpenSearch service name (e.g., "es").
        max_processes: max number of processes for the sync job.
        chunk_size: number of documents per sync chunk.
        max_retries: max retries for failed chunks.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="opensearch-sync-works",
        run_id=sqlmesh_run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch sync-works $BUCKET_NAME $RUN_ID $INDEX_NAME --client-config.mode=$MODE --client-config.host=$HOST --client-config.port=$PORT --client-config.region=$REGION --client-config.service=$SERVICE --sync-config.max-processes=$MAX_PROCESSES --sync-config.chunk-size=$CHUNK_SIZE --sync-config.max-retries=$MAX_RETRIES",
        environment=make_env(
            {
                "RUN_ID": sqlmesh_run_id,
                "BUCKET_NAME": bucket_name,
                "INDEX_NAME": index_name,
                "MODE": mode,
                "HOST": host,
                "PORT": str(port),
                "REGION": region,
                "SERVICE": service,
                "MAX_PROCESSES": str(max_processes),
                "CHUNK_SIZE": str(chunk_size),
                "MAX_RETRIES": str(max_retries),
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        depends_on=depends_on,
    )


def submit_sync_dmps_job(
    *,
    env: str,
    dmps_run_id: str,
    host: str,
    region: str,
    index_name: str = "dmps-index",
    mode: str = "aws",
    port: int = 443,
    service: str = "es",
    chunk_size: int = 1000,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenSearch dmps-index sync job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        dmps_run_id: The run_id of the DMPs data to use.
        host: The OpenSearch host URL.
        region: The AWS region of the OpenSearch cluster.
        index_name: The name of the OpenSearch index to sync to.
        mode: Client connection mode (e.g., "aws").
        port: OpenSearch connection port.
        service: OpenSearch service name (e.g., "es").
        chunk_size: Number of documents per sync chunk.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="opensearch-sync-dmps",
        run_id=dmps_run_id,
        job_queue=standard_job_queue(env),
        job_definition=database_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks opensearch sync-dmps $INDEX_NAME --opensearch-config.mode=$OPENSEARCH_MODE --opensearch-config.host=$OPENSEARCH_HOST --opensearch-config.port=$OPENSEARCH_PORT --opensearch-config.region=$OPENSEARCH_REGION --opensearch-config.service=$OPENSEARCH_SERVICE --chunk-size=$CHUNK_SIZE",
        environment=make_env(
            {
                "INDEX_NAME": index_name,
                "OPENSEARCH_MODE": mode,
                "OPENSEARCH_HOST": host,
                "OPENSEARCH_PORT": str(port),
                "OPENSEARCH_REGION": region,
                "OPENSEARCH_SERVICE": service,
                "CHUNK_SIZE": str(chunk_size),
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        depends_on=depends_on,
    )


def submit_enrich_dmps_job(
    *,
    env: str,
    dmps_run_id: str,
    host: str,
    region: str,
    index_name: str = "dmps-index",
    mode: str = "aws",
    port: int = 443,
    service: str = "es",
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenSearch DMPs enrichment job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        dmps_run_id: The run_id, used for job tracking.
        host: The OpenSearch host URL.
        region: The AWS region of the OpenSearch cluster.
        index_name: The name of the OpenSearch DMPs index to enrich.
        mode: Client connection mode (e.g., "aws").
        port: OpenSearch connection port.
        service: OpenSearch service name (e.g., "es").
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="opensearch-enrich-dmps",
        run_id=dmps_run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch enrich-dmps $INDEX_NAME --client-config.mode=$MODE --client-config.host=$HOST --client-config.port=$PORT --client-config.region=$REGION --client-config.service=$SERVICE",
        environment=make_env(
            {
                "INDEX_NAME": index_name,
                "MODE": mode,
                "HOST": host,
                "PORT": str(port),
                "REGION": region,
                "SERVICE": service,
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        depends_on=depends_on,
    )


def submit_dmp_works_search_job(
    *,
    env: str,
    bucket_name: str,
    dmps_run_id: str,
    host: str,
    region: str,
    dmps_index_name: str = "dmps-index",
    works_index_name: str = "works-index",
    mode: str = "aws",
    port: int = 443,
    service: str = "es",
    dmp_subset: DMPSubset,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenSearch DMP-to-Works search job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to output search results to.
        dmps_run_id: the run_id, used to export related works search results.
        host: The OpenSearch host URL.
        region: The AWS region of the OpenSearch cluster.
        dmps_index_name: The name of the OpenSearch DMPs index.
        works_index_name: The name of the OpenSearch Works index.
        mode: Client connection mode (e.g., "aws").
        port: OpenSearch connection port.
        service: OpenSearch service name (e.g., "es").
        dmp_subset: settings for including a subset of DMPs.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    environment = {
        "BUCKET_NAME": bucket_name,
        "RUN_ID": dmps_run_id,
        "DMPS_INDEX_NAME": dmps_index_name,
        "WORKS_INDEX_NAME": works_index_name,
        "MODE": mode,
        "HOST": host,
        "PORT": str(port),
        "REGION": region,
        "SERVICE": service,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }

    if dmp_subset is not None:
        environment["DMP_SUBSET_ENABLE"] = dmp_subset.enable
        environment["DMP_SUBSET_INSTITUTIONS_S3_PATH"] = dmp_subset.institutions_s3_path
        environment["DMP_SUBSET_DOIS_S3_PATH"] = dmp_subset.dois_s3_path

    return submit_job(
        job_name="opensearch-dmp-works-search",
        run_id=dmps_run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch dmp-works-search $BUCKET_NAME $RUN_ID $DMPS_INDEX_NAME $WORKS_INDEX_NAME --client-config.mode=$MODE --client-config.host=$HOST --client-config.port=$PORT --client-config.region=$REGION --client-config.service=$SERVICE",
        environment=make_env(environment),
        depends_on=depends_on,
    )


def submit_merge_related_works_job(
    *,
    env: str,
    bucket_name: str,
    dmps_run_id: str,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Creates a job to merge related works.

    The database parameters are set in the job defintion and read by the Cyclopts
    built command line interface as environment variables.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing search results to merge.
        dmps_run_id: the run_id of the DMPs data to use.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="opensearch-merge-related-works",
        run_id=dmps_run_id,
        job_queue=standard_job_queue(env),
        job_definition=database_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch merge-related-works $BUCKET_NAME $RUN_ID",
        environment=make_env(
            {
                "BUCKET_NAME": bucket_name,
                "RUN_ID": dmps_run_id,
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        depends_on=depends_on,
    )
