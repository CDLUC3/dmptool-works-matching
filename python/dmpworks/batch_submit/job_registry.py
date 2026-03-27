from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

AWSEnv = Literal["dev", "stg", "prd"]

# Queue capacity constants (sized to each queue's instance type)
# small queue:      c5ad.large    (2 vCPUs,  ~4 GiB)
SMALL_QUEUE_VCPUS = 2
SMALL_QUEUE_MEMORY = 3686

# download queue:   c5ad.8xlarge  (32 vCPUs, ~57 GiB)
DOWNLOAD_QUEUE_VCPUS = 32
DOWNLOAD_QUEUE_MEMORY = 58_368

# transform queue:  c5ad.8xlarge  (32 vCPUs, ~57 GiB)
TRANSFORM_QUEUE_VCPUS = 32
TRANSFORM_QUEUE_MEMORY = 58_368

# sqlmesh queue:    r6id.8xlarge  (32 vCPUs, ~245 GiB)
SQLMESH_QUEUE_VCPUS = 32
SQLMESH_QUEUE_MEMORY = 250_880

# opensearch queue: m5dn.2xlarge  (8 vCPUs,  ~28 GiB)
OPENSEARCH_QUEUE_VCPUS = 8
OPENSEARCH_QUEUE_MEMORY = 28_762

TQDM_POSITION = "-1"
TQDM_MININTERVAL = "120"


def standard_job_definition(env: AWSEnv) -> str:
    """Get the standard job definition name for the given environment.

    Args:
        env: The environment name (e.g., 'dev', 'stg').

    Returns:
        str: The job definition name.
    """
    return f"dmpworks-{env}-job"


def database_job_definition(env: AWSEnv) -> str:
    """Get the database job definition name for the given environment.

    Args:
        env: The environment name.

    Returns:
        str: The job definition name.
    """
    return f"dmpworks-{env}-database-job"


def datacite_download_job_definition(env: AWSEnv) -> str:
    """Get the DataCite download job definition name for the given environment.

    Args:
        env: The environment name.

    Returns:
        str: The job definition name.
    """
    return f"dmpworks-{env}-datacite-download-job"


def small_job_queue(env: AWSEnv) -> str:
    """Get the small job queue name for the given environment.

    Used for lightweight download jobs (ROR, Data Citation Corpus) on c5ad.large.

    Args:
        env: The environment name.

    Returns:
        str: The job queue name.
    """
    return f"dmpworks-{env}-batch-small-job-queue"


standard_job_queue = small_job_queue


def download_job_queue(env: AWSEnv) -> str:
    """Get the download job queue name for the given environment.

    Used for large download jobs (OpenAlex, Crossref, DataCite) on c5ad.8xlarge.

    Args:
        env: The environment name.

    Returns:
        str: The job queue name.
    """
    return f"dmpworks-{env}-batch-download-job-queue"


def transform_job_queue(env: AWSEnv) -> str:
    """Get the transform job queue name for the given environment.

    Used for dataset-subset and transform jobs on c5ad.8xlarge.

    Args:
        env: The environment name.

    Returns:
        str: The job queue name.
    """
    return f"dmpworks-{env}-batch-transform-job-queue"


def sqlmesh_job_queue(env: AWSEnv) -> str:
    """Get the SQLMesh job queue name for the given environment.

    Used for SQLMesh plan jobs on r6id.8xlarge (high memory).

    Args:
        env: The environment name.

    Returns:
        str: The job queue name.
    """
    return f"dmpworks-{env}-batch-sqlmesh-job-queue"


def opensearch_job_queue(env: AWSEnv) -> str:
    """Get the OpenSearch job queue name for the given environment.

    Used for OpenSearch sync and search jobs on m5dn.2xlarge.

    Args:
        env: The environment name.

    Returns:
        str: The job queue name.
    """
    return f"dmpworks-{env}-batch-opensearch-job-queue"


def build_env_list(env_vars: dict[str, Any]) -> list[dict[str, str]]:
    """Build a PascalCase Name/Value environment list for SFN Batch SDK integration.

    Filters None values. Converts bool to lowercase string ("true"/"false").

    Args:
        env_vars: Dict of env var names to values.

    Returns:
        list[dict[str, str]]: Items with "Name" and "Value" keys (PascalCase for SFN).
    """
    result = []
    for k, v in env_vars.items():
        if v is None:
            continue
        result.append({"Name": k, "Value": str(v).lower() if isinstance(v, bool) else str(v)})
    return result


def build_batch_params(
    *,
    run_name: str,
    env: AWSEnv,
    queue: Callable[[AWSEnv], str],
    job_definition: Callable[[AWSEnv], str],
    vcpus: int,
    memory: int,
    command: str,
    env_vars: dict[str, Any],
) -> dict[str, Any]:
    """Build SFN-compatible AWS Batch job parameters.

    Args:
        run_name: Job identifier for DynamoDB tracking.
        env: AWS environment (dev/stg/prd).
        queue: Function that returns the job queue name for the environment.
        job_definition: Function that returns the job definition name for the environment.
        vcpus: Number of vCPUs for the container.
        memory: Memory in MiB for the container.
        command: Shell command string (wrapped in /bin/bash -c).
        env_vars: Environment variables dict (passed to build_env_list).

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return {
        "run_name": run_name,
        "JobQueue": queue(env),
        "JobDefinition": job_definition(env),
        "ContainerOverrides": {
            "Command": ["/bin/bash", "-c", command],
            "Vcpus": vcpus,
            "Memory": memory,
            "Environment": build_env_list(env_vars),
        },
    }


PIPELINE_TASK_TYPES: dict[str, list[str]] = {
    "ror": ["download"],
    "data-citation-corpus": ["download"],
    "openalex-works": ["download", "subset", "transform"],
    "crossref-metadata": ["download", "subset", "transform"],
    "datacite": ["download", "subset", "transform"],
}


def get_task_types_to_run(pipeline: str, use_subset: bool) -> list[str]:
    """Return the ordered list of task types to execute for a pipeline run.

    Args:
        pipeline: The pipeline name (e.g., 'openalex-works').
        use_subset: Whether the subset task type should be included.

    Returns:
        list[str]: Ordered task type names to run.
    """
    return [t for t in PIPELINE_TASK_TYPES[pipeline] if use_subset or t != "subset"]


# ---------------------------------------------------------------------------
# Registry factory functions (wired to JOB_FACTORIES)
# ---------------------------------------------------------------------------


def ror_download_factory(
    *,
    run_id: str,
    bucket_name: str,
    download_url: str,
    file_hash: str,
    env: AWSEnv,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the ROR download job.

    Args:
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        download_url: Zenodo download URL for the ROR data file.
        file_hash: Expected hash of the data file.
        env: AWS environment (dev/stg/prd).
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="ror-download",
        env=env,
        queue=small_job_queue,
        job_definition=standard_job_definition,
        vcpus=SMALL_QUEUE_VCPUS,
        memory=SMALL_QUEUE_MEMORY,
        command="dmpworks aws-batch ror download $BUCKET_NAME $RUN_ID $DOWNLOAD_URL --file-hash $FILE_HASH",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "DOWNLOAD_URL": download_url,
            "FILE_HASH": file_hash,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        },
    )


def dcc_download_factory(
    *,
    run_id: str,
    bucket_name: str,
    download_url: str,
    file_hash: str,
    env: AWSEnv,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the Data Citation Corpus download job.

    Args:
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        download_url: Zenodo download URL for the DCC JSON zip file.
        file_hash: Expected hash of the data file.
        env: AWS environment (dev/stg/prd).
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="data-citation-corpus-download",
        env=env,
        queue=small_job_queue,
        job_definition=standard_job_definition,
        vcpus=SMALL_QUEUE_VCPUS,
        memory=SMALL_QUEUE_MEMORY,
        command="dmpworks aws-batch data-citation-corpus download $BUCKET_NAME $RUN_ID $DOWNLOAD_URL --file-hash $FILE_HASH",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "DOWNLOAD_URL": download_url,
            "FILE_HASH": file_hash,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        },
    )


def openalex_works_download_factory(
    *,
    run_id: str,
    bucket_name: str,
    openalex_bucket_name: str,
    env: AWSEnv,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the OpenAlex Works download job.

    Args:
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        openalex_bucket_name: Name of the OpenAlex AWS S3 bucket.
        env: AWS environment (dev/stg/prd).
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="openalex-works-download",
        env=env,
        queue=download_job_queue,
        job_definition=standard_job_definition,
        vcpus=DOWNLOAD_QUEUE_VCPUS,
        memory=DOWNLOAD_QUEUE_MEMORY,
        command="dmpworks aws-batch openalex-works download $BUCKET_NAME $RUN_ID $OPENALEX_BUCKET_NAME",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "OPENALEX_BUCKET_NAME": openalex_bucket_name,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        },
    )


def crossref_metadata_download_factory(
    *,
    run_id: str,
    bucket_name: str,
    file_name: str,
    crossref_metadata_bucket_name: str,
    env: AWSEnv,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the Crossref Metadata download job.

    Args:
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        file_name: Name of the Crossref metadata file to download.
        crossref_metadata_bucket_name: Name of the Crossref AWS S3 bucket.
        env: AWS environment (dev/stg/prd).
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="crossref-metadata-download",
        env=env,
        queue=download_job_queue,
        job_definition=standard_job_definition,
        vcpus=DOWNLOAD_QUEUE_VCPUS,
        memory=DOWNLOAD_QUEUE_MEMORY,
        command="dmpworks aws-batch crossref-metadata download $BUCKET_NAME $RUN_ID $FILE_NAME $CROSSREF_METADATA_BUCKET_NAME",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "FILE_NAME": file_name,
            "CROSSREF_METADATA_BUCKET_NAME": crossref_metadata_bucket_name,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        },
    )


def datacite_download_factory(
    *,
    run_id: str,
    bucket_name: str,
    datacite_bucket_name: str,
    env: AWSEnv,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the DataCite download job.

    Args:
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        datacite_bucket_name: Name of the DataCite AWS S3 bucket.
        env: AWS environment (dev/stg/prd).
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="datacite-download",
        env=env,
        queue=download_job_queue,
        job_definition=datacite_download_job_definition,
        vcpus=DOWNLOAD_QUEUE_VCPUS,
        memory=DOWNLOAD_QUEUE_MEMORY,
        command="dmpworks aws-batch datacite download $BUCKET_NAME $RUN_ID $DATACITE_BUCKET_NAME",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "DATACITE_BUCKET_NAME": datacite_bucket_name,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        },
    )


def dataset_subset_factory(
    *,
    run_name: str,
    run_id: str,
    bucket_name: str,
    env: AWSEnv,
    dataset: str,
    dataset_subset_enable: str | bool | None = None,
    dataset_subset_institutions_s3_path: str | None = None,
    dataset_subset_dois_s3_path: str | None = None,
    prev_job_run_id: str | None = None,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for a dataset-subset job.

    Args:
        run_name: Job base name for DynamoDB, e.g. "openalex-works-dataset-subset".
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        env: AWS environment (dev/stg/prd).
        dataset: Dataset identifier, e.g. "openalex-works".
        dataset_subset_enable: Whether to enable subset filtering.
        dataset_subset_institutions_s3_path: S3 path to institutions filter file.
        dataset_subset_dois_s3_path: S3 path to DOIs filter file.
        prev_job_run_id: Run ID of the prior download job, if any.
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name=run_name,
        env=env,
        queue=transform_job_queue,
        job_definition=standard_job_definition,
        vcpus=TRANSFORM_QUEUE_VCPUS,
        memory=TRANSFORM_QUEUE_MEMORY,
        command="dmpworks aws-batch $DATASET dataset-subset $BUCKET_NAME $RUN_ID",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "PREV_JOB_RUN_ID": prev_job_run_id,
            "DATASET": dataset,
            "DATASET_SUBSET_ENABLE": dataset_subset_enable,
            "DATASET_SUBSET_INSTITUTIONS_S3_PATH": dataset_subset_institutions_s3_path,
            "DATASET_SUBSET_DOIS_S3_PATH": dataset_subset_dois_s3_path,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        },
    )


def openalex_works_transform_factory(
    *,
    run_id: str,
    bucket_name: str,
    env: AWSEnv,
    use_subset: str | bool = "false",
    log_level: str = "INFO",
    prev_job_run_id: str | None = None,
    openalex_works_transform_batch_size: str | int,
    openalex_works_transform_row_group_size: str | int,
    openalex_works_transform_row_groups_per_file: str | int,
    openalex_works_transform_max_workers: str | int,
    openalex_works_transform_include_xpac: str | bool,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the OpenAlex Works transform job.

    Args:
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        env: AWS environment (dev/stg/prd).
        use_subset: Whether to use a subset of the dataset.
        log_level: Logging level for the job.
        prev_job_run_id: Run ID of the prior download job, if any.
        openalex_works_transform_batch_size: Number of input files per batch.
        openalex_works_transform_row_group_size: Parquet row group size.
        openalex_works_transform_row_groups_per_file: Row groups per Parquet file.
        openalex_works_transform_max_workers: Number of parallel workers.
        openalex_works_transform_include_xpac: Whether to include xpac-flagged works.
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="openalex-works-transform",
        env=env,
        queue=transform_job_queue,
        job_definition=standard_job_definition,
        vcpus=TRANSFORM_QUEUE_VCPUS,
        memory=TRANSFORM_QUEUE_MEMORY,
        command="dmpworks aws-batch openalex-works transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --log-level=$LOG_LEVEL",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "PREV_JOB_RUN_ID": prev_job_run_id,
            "USE_SUBSET": use_subset,
            "LOG_LEVEL": log_level,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "OPENALEX_WORKS_TRANSFORM_BATCH_SIZE": openalex_works_transform_batch_size,
            "OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE": openalex_works_transform_row_group_size,
            "OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE": openalex_works_transform_row_groups_per_file,
            "OPENALEX_WORKS_TRANSFORM_MAX_WORKERS": openalex_works_transform_max_workers,
            "OPENALEX_WORKS_TRANSFORM_INCLUDE_XPAC": openalex_works_transform_include_xpac,
        },
    )


def crossref_metadata_transform_factory(
    *,
    run_id: str,
    bucket_name: str,
    env: AWSEnv,
    use_subset: str | bool = "false",
    log_level: str = "INFO",
    prev_job_run_id: str | None = None,
    crossref_metadata_transform_batch_size: str | int,
    crossref_metadata_transform_row_group_size: str | int,
    crossref_metadata_transform_row_groups_per_file: str | int,
    crossref_metadata_transform_max_workers: str | int,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the Crossref Metadata transform job.

    Args:
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        env: AWS environment (dev/stg/prd).
        use_subset: Whether to use a subset of the dataset.
        log_level: Logging level for the job.
        prev_job_run_id: Run ID of the prior download job, if any.
        crossref_metadata_transform_batch_size: Number of input files per batch.
        crossref_metadata_transform_row_group_size: Parquet row group size.
        crossref_metadata_transform_row_groups_per_file: Row groups per Parquet file.
        crossref_metadata_transform_max_workers: Number of parallel workers.
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="crossref-metadata-transform",
        env=env,
        queue=transform_job_queue,
        job_definition=standard_job_definition,
        vcpus=TRANSFORM_QUEUE_VCPUS,
        memory=TRANSFORM_QUEUE_MEMORY,
        command="dmpworks aws-batch crossref-metadata transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --log-level=$LOG_LEVEL",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "PREV_JOB_RUN_ID": prev_job_run_id,
            "USE_SUBSET": use_subset,
            "LOG_LEVEL": log_level,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "CROSSREF_METADATA_TRANSFORM_BATCH_SIZE": crossref_metadata_transform_batch_size,
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE": crossref_metadata_transform_row_group_size,
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE": crossref_metadata_transform_row_groups_per_file,
            "CROSSREF_METADATA_TRANSFORM_MAX_WORKERS": crossref_metadata_transform_max_workers,
        },
    )


def datacite_transform_factory(
    *,
    run_id: str,
    bucket_name: str,
    env: AWSEnv,
    use_subset: str | bool = "false",
    log_level: str = "INFO",
    prev_job_run_id: str | None = None,
    datacite_transform_batch_size: str | int,
    datacite_transform_row_group_size: str | int,
    datacite_transform_row_groups_per_file: str | int,
    datacite_transform_max_workers: str | int,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the DataCite transform job.

    Args:
        run_id: Unique run ID for this execution.
        bucket_name: S3 bucket for job I/O.
        env: AWS environment (dev/stg/prd).
        use_subset: Whether to use a subset of the dataset.
        log_level: Logging level for the job.
        prev_job_run_id: Run ID of the prior download job, if any.
        datacite_transform_batch_size: Number of input files per batch.
        datacite_transform_row_group_size: Parquet row group size.
        datacite_transform_row_groups_per_file: Row groups per Parquet file.
        datacite_transform_max_workers: Number of parallel workers.
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="datacite-transform",
        env=env,
        queue=transform_job_queue,
        job_definition=standard_job_definition,
        vcpus=TRANSFORM_QUEUE_VCPUS,
        memory=TRANSFORM_QUEUE_MEMORY,
        command="dmpworks aws-batch datacite transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --log-level=$LOG_LEVEL",
        env_vars={
            "RUN_ID": run_id,
            "BUCKET_NAME": bucket_name,
            "PREV_JOB_RUN_ID": prev_job_run_id,
            "USE_SUBSET": use_subset,
            "LOG_LEVEL": log_level,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "DATACITE_TRANSFORM_BATCH_SIZE": datacite_transform_batch_size,
            "DATACITE_TRANSFORM_ROW_GROUP_SIZE": datacite_transform_row_group_size,
            "DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE": datacite_transform_row_groups_per_file,
            "DATACITE_TRANSFORM_MAX_WORKERS": datacite_transform_max_workers,
        },
    )


def sqlmesh_process_works_factory(
    *,
    run_id: str,
    env: AWSEnv,
    bucket_name: str,
    run_id_sqlmesh_prev: str,
    run_id_openalex_works: str,
    run_id_datacite: str,
    run_id_crossref_metadata: str,
    run_id_ror: str,
    run_id_data_citation_corpus: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the process-works SQLMesh plan job.

    Args:
        run_id: Unique run ID for this SQLMesh execution (RUN_ID_SQLMESH).
        env: AWS environment (dev/stg/prd).
        bucket_name: S3 bucket for SQLMesh data.
        run_id_sqlmesh_prev: Run ID of the prior SQLMesh execution for incremental runs.
        run_id_openalex_works: Run ID of the OpenAlex Works transform checkpoint.
        run_id_datacite: Run ID of the DataCite transform checkpoint.
        run_id_crossref_metadata: Run ID of the Crossref Metadata transform checkpoint.
        run_id_ror: Run ID of the ROR download checkpoint.
        run_id_data_citation_corpus: Run ID of the Data Citation Corpus download checkpoint.
        **kwargs: SQLMesh config vars from config.to_env_dict() (lowercased keys).

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    # Collect SQLMesh config env vars from kwargs (uppercase keys from config)
    sqlmesh_config_vars = {k.upper(): v for k, v in kwargs.items() if isinstance(v, str)}
    return build_batch_params(
        run_name="process-works-sqlmesh",
        env=env,
        queue=sqlmesh_job_queue,
        job_definition=standard_job_definition,
        vcpus=SQLMESH_QUEUE_VCPUS,
        memory=SQLMESH_QUEUE_MEMORY,
        command="dmpworks aws-batch sqlmesh plan $BUCKET_NAME",
        env_vars={
            "RUN_ID_SQLMESH": run_id,
            "RUN_ID_SQLMESH_PREV": run_id_sqlmesh_prev,
            "RUN_ID_OPENALEX_WORKS": run_id_openalex_works,
            "RUN_ID_DATACITE": run_id_datacite,
            "RUN_ID_CROSSREF_METADATA": run_id_crossref_metadata,
            "RUN_ID_ROR": run_id_ror,
            "RUN_ID_DATA_CITATION_CORPUS": run_id_data_citation_corpus,
            "BUCKET_NAME": bucket_name,
            **sqlmesh_config_vars,
        },
    )


def sync_works_process_works_factory(
    *,
    run_id: str,
    env: AWSEnv,
    bucket_name: str,
    sqlmesh_run_id: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the process-works OpenSearch sync-works job.

    Args:
        run_id: Unique run ID for this sync-works execution (RUN_ID_SYNC_WORKS).
        env: AWS environment (dev/stg/prd).
        bucket_name: S3 bucket containing SQLMesh-transformed data.
        sqlmesh_run_id: Run ID of the SQLMesh job whose output to read (RUN_ID_SQLMESH).
        **kwargs: OpenSearch config vars from config.to_env_dict() (lowercased keys).

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    opensearch_config_vars = {k.upper(): v for k, v in kwargs.items() if isinstance(v, str)}
    return build_batch_params(
        run_name="process-works-sync-works",
        env=env,
        queue=opensearch_job_queue,
        job_definition=standard_job_definition,
        vcpus=OPENSEARCH_QUEUE_VCPUS,
        memory=OPENSEARCH_QUEUE_MEMORY,
        command="dmpworks aws-batch opensearch sync-works $BUCKET_NAME $INDEX_NAME",
        env_vars={
            "RUN_ID_SYNC_WORKS": run_id,
            "RUN_ID_SQLMESH": sqlmesh_run_id,
            "BUCKET_NAME": bucket_name,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            **opensearch_config_vars,
        },
    )


def sync_dmps_process_dmps_factory(
    *,
    run_id: str,  # noqa: ARG001
    env: AWSEnv,
    bucket_name: str,
    dmps_index_name: str = "dmps-index",
    **kwargs: Any,
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the process-dmps sync-dmps job.

    Args:
        run_id: Unique run ID for this execution (not used in the batch command, absorbed).
        env: AWS environment (dev/stg/prd).
        bucket_name: S3 bucket for job I/O and DMP subset downloads.
        dmps_index_name: Name of the OpenSearch DMPs index to sync to.
        **kwargs: OpenSearch client/sync and DMP subset config vars (lowercased keys).

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    config_vars = {k.upper(): v for k, v in kwargs.items() if isinstance(v, str)}
    return build_batch_params(
        run_name="process-dmps-sync-dmps",
        env=env,
        queue=opensearch_job_queue,
        job_definition=database_job_definition,
        vcpus=OPENSEARCH_QUEUE_VCPUS,
        memory=OPENSEARCH_QUEUE_MEMORY,
        command="dmpworks aws-batch opensearch sync-dmps $BUCKET_NAME $INDEX_NAME",
        env_vars={
            "BUCKET_NAME": bucket_name,
            "INDEX_NAME": dmps_index_name,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            **config_vars,
        },
    )


def enrich_dmps_process_dmps_factory(
    *,
    run_id: str,  # noqa: ARG001
    env: AWSEnv,
    bucket_name: str,
    dmps_index_name: str = "dmps-index",
    **kwargs: Any,
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the process-dmps enrich-dmps job.

    Args:
        run_id: Unique run ID for this execution (not used in the batch command, absorbed).
        env: AWS environment (dev/stg/prd).
        bucket_name: S3 bucket for DMP subset downloads.
        dmps_index_name: Name of the OpenSearch DMPs index to enrich.
        **kwargs: OpenSearch client and DMP subset config vars (lowercased keys).

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    config_vars = {k.upper(): v for k, v in kwargs.items() if isinstance(v, str)}
    return build_batch_params(
        run_name="process-dmps-enrich-dmps",
        env=env,
        queue=opensearch_job_queue,
        job_definition=standard_job_definition,
        vcpus=OPENSEARCH_QUEUE_VCPUS,
        memory=OPENSEARCH_QUEUE_MEMORY,
        command="dmpworks aws-batch opensearch enrich-dmps $INDEX_NAME --bucket-name $BUCKET_NAME",
        env_vars={
            "BUCKET_NAME": bucket_name,
            "INDEX_NAME": dmps_index_name,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            **config_vars,
        },
    )


def dmp_works_search_process_dmps_factory(
    *,
    run_id: str,
    env: AWSEnv,
    bucket_name: str,
    dmps_index_name: str = "dmps-index",
    works_index_name: str = "works-index",
    run_all_dmps: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the process-dmps dmp-works-search job.

    When run_all_dmps is True (triggered from process-works), overrides
    DMP_WORKS_SEARCH_APPLY_MODIFICATION_WINDOW to "false" so the batch job ignores
    dmp_modification_window_days and processes all DMPs.

    Args:
        run_id: Unique run ID for this execution (passed as RUN_ID to the batch command).
        env: AWS environment (dev/stg/prd).
        bucket_name: S3 bucket to output search results to.
        dmps_index_name: Name of the OpenSearch DMPs index.
        works_index_name: Name of the OpenSearch Works index.
        run_all_dmps: If True, disables the modification window filter.
        **kwargs: OpenSearch client, dmp-works-search, and DMP subset config vars (lowercased keys).

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    config_vars = {k.upper(): v for k, v in kwargs.items() if isinstance(v, str)}
    if run_all_dmps:
        config_vars["DMP_WORKS_SEARCH_APPLY_MODIFICATION_WINDOW"] = "false"
    return build_batch_params(
        run_name="process-dmps-dmp-works-search",
        env=env,
        queue=opensearch_job_queue,
        job_definition=standard_job_definition,
        vcpus=OPENSEARCH_QUEUE_VCPUS,
        memory=OPENSEARCH_QUEUE_MEMORY,
        command="dmpworks aws-batch opensearch dmp-works-search $BUCKET_NAME $RUN_ID $DMPS_INDEX_NAME $WORKS_INDEX_NAME",
        env_vars={
            "BUCKET_NAME": bucket_name,
            "RUN_ID": run_id,
            "DMPS_INDEX_NAME": dmps_index_name,
            "WORKS_INDEX_NAME": works_index_name,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            **config_vars,
        },
    )


def merge_related_works_process_dmps_factory(
    *,
    run_id: str,
    env: AWSEnv,
    bucket_name: str,
    search_run_id: str,
    **kwargs: Any,  # noqa: ARG001
) -> dict[str, Any]:
    """Build SFN-compatible Batch params for the process-dmps merge-related-works job.

    MySQL credentials are provided by the database job definition, so no config vars
    are needed beyond the explicit parameters.

    Args:
        run_id: Unique run ID for this merge-related-works execution.
        env: AWS environment (dev/stg/prd).
        bucket_name: S3 bucket containing dmp-works-search output to merge.
        search_run_id: Run ID of the dmp-works-search job whose output to read.
        **kwargs: Absorbs unused keyword arguments.

    Returns:
        dict: SFN-compatible Batch params including run_name.
    """
    return build_batch_params(
        run_name="process-dmps-merge-related-works",
        env=env,
        queue=opensearch_job_queue,
        job_definition=database_job_definition,
        vcpus=OPENSEARCH_QUEUE_VCPUS,
        memory=OPENSEARCH_QUEUE_MEMORY,
        command="dmpworks aws-batch opensearch merge-related-works $BUCKET_NAME $RUN_ID $SEARCH_RUN_ID",
        env_vars={
            "BUCKET_NAME": bucket_name,
            "RUN_ID": run_id,
            "SEARCH_RUN_ID": search_run_id,
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        },
    )


JOB_FACTORIES: dict[tuple[str, str], Callable[..., dict[str, Any]]] = {
    ("ror", "download"): ror_download_factory,
    ("data-citation-corpus", "download"): dcc_download_factory,
    ("openalex-works", "download"): openalex_works_download_factory,
    ("crossref-metadata", "download"): crossref_metadata_download_factory,
    ("datacite", "download"): datacite_download_factory,
    ("openalex-works", "subset"): partial(dataset_subset_factory, run_name="openalex-works-dataset-subset"),
    ("crossref-metadata", "subset"): partial(dataset_subset_factory, run_name="crossref-metadata-dataset-subset"),
    ("datacite", "subset"): partial(dataset_subset_factory, run_name="datacite-dataset-subset"),
    ("openalex-works", "transform"): openalex_works_transform_factory,
    ("crossref-metadata", "transform"): crossref_metadata_transform_factory,
    ("datacite", "transform"): datacite_transform_factory,
    ("process-works", "sqlmesh"): sqlmesh_process_works_factory,
    ("process-works", "sync-works"): sync_works_process_works_factory,
    ("process-dmps", "sync-dmps"): sync_dmps_process_dmps_factory,
    ("process-dmps", "enrich-dmps"): enrich_dmps_process_dmps_factory,
    ("process-dmps", "dmp-works-search"): dmp_works_search_process_dmps_factory,
    ("process-dmps", "merge-related-works"): merge_related_works_process_dmps_factory,
}
