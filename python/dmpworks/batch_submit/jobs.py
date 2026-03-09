from collections.abc import Callable
import inspect
import logging
from typing import TypedDict

import boto3
import pendulum

from dmpworks.cli_utils import (
    CrossrefMetadataTransformConfig,
    DataCiteTransformConfig,
    DatasetSubsetAWS,
    DMPSubsetAWS,
    DMPWorksSearchConfig,
    OpenAlexWorksTransformConfig,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
    RunIdentifiers,
    SQLMeshConfig,
    get_env_var_dict,
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


def run_job_pipeline(
    *,
    task_definitions: dict[str, Callable],
    task_order: list[str],
    start_task_name: str,
):
    """Run a pipeline of AWS Batch jobs.

    Args:
        task_definitions: A dictionary mapping task names to callable job submission functions.
        task_order: A list of task names defining the execution order.
        start_task_name: The name of the task to start the pipeline from.

    Raises:
        ValueError: If the start task is unknown or a task definition is missing.
    """
    # Build list of tasks
    try:
        start_index = task_order.index(start_task_name)
    except ValueError as e:
        msg = f"Unknown start_task '{start_task_name}'"
        raise ValueError(msg) from e
    task_names = task_order[start_index:]

    # Execute tasks
    job_ids = []
    for task_name in task_names:
        if task_name not in task_definitions:
            msg = f"No function defined for task '{task_name}'"
            raise ValueError(msg)

        # Add any dependent jobs
        task_func = task_definitions[task_name]
        kwargs = {}
        sig = inspect.signature(task_func)
        if "depends_on" in sig.parameters and len(job_ids) > 0:
            kwargs["depends_on"] = [job_ids[-1]]

        # Call the task
        job_id = task_func(**kwargs)
        if job_id is not None:
            job_ids.append({"jobId": str(job_id)})


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
    dataset_subset: DatasetSubsetAWS,
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

    env_vars = {
        "RUN_ID": run_id,
        "BUCKET_NAME": bucket_name,
        "DATASET": dataset,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }
    env_vars.update(get_env_var_dict(dataset_subset))

    return submit_job(
        job_name=f"{dataset}-dataset-subset",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        command="dmpworks aws-batch $DATASET dataset-subset $BUCKET_NAME $RUN_ID",
        environment=make_env(env_vars),
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
                "BUCKET_NAME": bucket_name,
                "RUN_ID": run_id,
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
    log_level: str = "INFO",
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
        log_level: logging level.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    env_vars = {
        "RUN_ID": run_id,
        "BUCKET_NAME": bucket_name,
        "USE_SUBSET": str(use_subset).lower(),
        "LOG_LEVEL": log_level,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }
    env_vars.update(get_env_var_dict(config))

    return submit_job(
        job_name="openalex-works-transform",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-works transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --log-level=$LOG_LEVEL",
        environment=make_env(env_vars),
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
    log_level: str = "INFO",
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
        log_level: logging level.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    env_vars = {
        "RUN_ID": run_id,
        "BUCKET_NAME": bucket_name,
        "USE_SUBSET": str(use_subset).lower(),
        "LOG_LEVEL": log_level,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }
    env_vars.update(get_env_var_dict(config))

    return submit_job(
        job_name="crossref-metadata-transform",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch crossref-metadata transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --log-level=$LOG_LEVEL",
        environment=make_env(env_vars),
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
    log_level: str = "INFO",
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
        log_level: logging level.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    env_vars = {
        "RUN_ID": run_id,
        "BUCKET_NAME": bucket_name,
        "USE_SUBSET": str(use_subset).lower(),
        "LOG_LEVEL": log_level,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }
    env_vars.update(get_env_var_dict(config))

    return submit_job(
        job_name="datacite-transform",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch datacite transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET --log-level=$LOG_LEVEL",
        environment=make_env(env_vars),
        depends_on=depends_on,
    )


def submit_sqlmesh_job(
    *,
    env: str,
    bucket_name: str,
    run_identifiers: RunIdentifiers,
    sqlmesh_config: SQLMeshConfig,
    vcpus: int = VERY_LARGE_VCPUS,
    memory: int = VERY_LARGE_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the main SQLMesh transformation job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket for SQLMesh data.
        run_identifiers: Unique identifiers for each data source.
        sqlmesh_config: The SQLMesh config.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    env_vars = {
        "BUCKET_NAME": bucket_name,
    }
    env_vars.update(get_env_var_dict(run_identifiers))
    env_vars.update(get_env_var_dict(sqlmesh_config))

    return submit_job(
        job_name="sqlmesh",
        run_id=run_identifiers.run_id_process_works,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch sqlmesh plan $BUCKET_NAME",
        environment=make_env(env_vars),
        depends_on=depends_on,
    )


def submit_sync_works_job(
    *,
    env: str,
    bucket_name: str,
    run_identifiers: RunIdentifiers,
    os_client_config: OpenSearchClientConfig | None = None,
    os_sync_config: OpenSearchSyncConfig | None = None,
    index_name: str = "works-index",
    vcpus: int = MEDIUM_VCPUS,
    memory: int = MEDIUM_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenSearch works-index sync job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the SQLMesh-transformed data.
        run_identifiers: Unique identifiers for each data source.
        os_client_config: The OpenSearch client config.
        os_sync_config: The OpenSearch sync config.
        index_name: The name of the OpenSearch index to sync to.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    if os_client_config is None:
        os_client_config = OpenSearchClientConfig()
    if os_sync_config is None:
        os_sync_config = OpenSearchSyncConfig()

    env_vars = {
        "BUCKET_NAME": bucket_name,
        "INDEX_NAME": index_name,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }
    env_vars.update(get_env_var_dict(run_identifiers))
    env_vars.update(get_env_var_dict(os_client_config))
    env_vars.update(get_env_var_dict(os_sync_config))

    return submit_job(
        job_name="opensearch-sync-works",
        run_id=run_identifiers.run_id_process_works,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch sync-works $BUCKET_NAME $INDEX_NAME",
        environment=make_env(env_vars),
        depends_on=depends_on,
    )


def submit_sync_dmps_job(
    *,
    env: str,
    bucket_name: str,
    run_id_dmps: str,
    os_client_config: OpenSearchClientConfig | None = None,
    os_sync_config: OpenSearchSyncConfig | None = None,
    index_name: str = "dmps-index",
    dmp_subset: DMPSubsetAWS | None = None,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenSearch dmps-index sync job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket for job I/O and DMP subset downloads.
        run_id_dmps: The run_id of the DMPs data to use.
        os_client_config: The OpenSearch client config.
        os_sync_config: The OpenSearch sync config.
        index_name: The name of the OpenSearch index to sync to.
        dmp_subset: settings for including a subset of DMPs.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    if os_client_config is None:
        os_client_config = OpenSearchClientConfig()
    if os_sync_config is None:
        os_sync_config = OpenSearchSyncConfig()

    env_vars = {
        "BUCKET_NAME": bucket_name,
        "INDEX_NAME": index_name,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }
    env_vars.update(get_env_var_dict(os_client_config))
    env_vars.update(get_env_var_dict(os_sync_config))
    if dmp_subset is not None:
        env_vars.update(get_env_var_dict(dmp_subset))

    return submit_job(
        job_name="opensearch-sync-dmps",
        run_id=run_id_dmps,
        job_queue=standard_job_queue(env),
        job_definition=database_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch sync-dmps $BUCKET_NAME $INDEX_NAME",
        environment=make_env(env_vars),
        depends_on=depends_on,
    )


def submit_enrich_dmps_job(
    *,
    env: str,
    bucket_name: str,
    run_id_dmps: str,
    os_client_config: OpenSearchClientConfig | None = None,
    index_name: str = "dmps-index",
    dmp_subset: DMPSubsetAWS | None = None,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenSearch DMPs enrichment job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket for DMP subset downloads.
        run_id_dmps: The run_id of the DMPs data, used for job tracking.
        os_client_config: The OpenSearch client config.
        index_name: The name of the OpenSearch DMPs index to enrich.
        dmp_subset: settings for including a subset of DMPs.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    if os_client_config is None:
        os_client_config = OpenSearchClientConfig()

    env_vars = {
        "BUCKET_NAME": bucket_name,
        "INDEX_NAME": index_name,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }
    env_vars.update(get_env_var_dict(os_client_config))
    if dmp_subset is not None:
        env_vars.update(get_env_var_dict(dmp_subset))

    return submit_job(
        job_name="opensearch-enrich-dmps",
        run_id=run_id_dmps,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch enrich-dmps $INDEX_NAME --bucket-name $BUCKET_NAME",
        environment=make_env(env_vars),
        depends_on=depends_on,
    )


def submit_dmp_works_search_job(
    *,
    env: str,
    bucket_name: str,
    run_id_dmps: str,
    os_client_config: OpenSearchClientConfig | None = None,
    dmps_index_name: str = "dmps-index",
    works_index_name: str = "works-index",
    dmp_subset: DMPSubsetAWS,
    dmp_works_search_config: DMPWorksSearchConfig | None = None,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submits the OpenSearch DMP-to-Works search job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to output search results to.
        run_id_dmps: the DMPs run_id, used to export related works search results.
        os_client_config: The OpenSearch client config.
        dmps_index_name: The name of the OpenSearch DMPs index.
        works_index_name: The name of the OpenSearch Works index.
        dmp_subset: settings for including a subset of DMPs.
        dmp_works_search_config: DMP works search settings.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    if os_client_config is None:
        os_client_config = OpenSearchClientConfig()
    if dmp_works_search_config is None:
        dmp_works_search_config = DMPWorksSearchConfig()

    env_vars = {
        "BUCKET_NAME": bucket_name,
        "RUN_ID": run_id_dmps,
        "DMPS_INDEX_NAME": dmps_index_name,
        "WORKS_INDEX_NAME": works_index_name,
        "TQDM_POSITION": TQDM_POSITION,
        "TQDM_MININTERVAL": TQDM_MININTERVAL,
    }
    env_vars.update(get_env_var_dict(os_client_config))
    env_vars.update(get_env_var_dict(dmp_works_search_config))

    if dmp_subset is not None:
        env_vars.update(get_env_var_dict(dmp_subset))

    return submit_job(
        job_name="opensearch-dmp-works-search",
        run_id=run_id_dmps,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch dmp-works-search $BUCKET_NAME $RUN_ID $DMPS_INDEX_NAME $WORKS_INDEX_NAME",
        environment=make_env(env_vars),
        depends_on=depends_on,
    )


def submit_merge_related_works_job(
    *,
    env: str,
    bucket_name: str,
    run_id_dmps: str,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Creates a job to merge related works.

    The database parameters are set in the job definition and read by the Cyclopts
    built command line interface as environment variables.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing search results to merge.
        run_id_dmps: the DMPs run_id.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """
    return submit_job(
        job_name="opensearch-merge-related-works",
        run_id=run_id_dmps,
        job_queue=standard_job_queue(env),
        job_definition=database_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch merge-related-works $BUCKET_NAME $RUN_ID",
        environment=make_env(
            {
                "BUCKET_NAME": bucket_name,
                "RUN_ID": run_id_dmps,
                "TQDM_POSITION": TQDM_POSITION,
                "TQDM_MININTERVAL": TQDM_MININTERVAL,
            }
        ),
        depends_on=depends_on,
    )
