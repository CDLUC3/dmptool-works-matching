import logging
from typing import Optional, TypedDict

import boto3
import pendulum

from dmpworks.cli_utils import DatasetSubset
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
    global _aws_batch_client
    if _aws_batch_client is None:
        _aws_batch_client = boto3.client('batch')
    return _aws_batch_client


class EnvVarDict(TypedDict):
    name: str
    value: str


class DependsOnDict(TypedDict):
    jobId: str


def format_date(date: pendulum.Date):
    return date.format("YYYY-MM-DD")


def standard_job_definition(env: str) -> str:
    return f"dmp-tool-{env}-batch-dmpworks-job"


def standard_job_queue(env: str) -> str:
    return f"dmp-tool-{env}-batch-job-queue"


def database_job_definition(env: str) -> str:
    return f"dmp-tool-{env}-batch-dmpworks-database-job"


def submit_job(
    *,
    job_name: str,
    run_id: str,
    job_queue: str,
    job_definition: str,
    vcpus: Optional[int] = None,
    memory: Optional[int] = None,
    command: Optional[str] = None,
    environment: Optional[list[EnvVarDict]] = None,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
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
    job_id = response['jobId']
    logging.info(f"Submitted job {full_job_name} with ID: {job_id}")

    return job_id


def make_env(env_vars: dict[str, str]) -> list[EnvVarDict]:
    return [{"name": k, "value": str(v)} for k, v in env_vars.items()]


def dataset_subset_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    dataset: Dataset,
    institutions: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the create dataset subset job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the file to.
        run_id: a unique ID to represent this run of the job.
        dataset: the dataset to create the subset for.
        institutions: a list of the institutions to include.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """

    logging.info("Creating dataset subset")
    logging.info(f"institutions: {institutions}")

    return submit_job(
        job_name=f"{dataset}-dataset-subset",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        command=f'dmpworks aws-batch $DATASET dataset-subset $BUCKET_NAME $RUN_ID',
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "DATASET": dataset,
                "DATASET_SUBSET_INSTITUTIONS": institutions,
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
    """
    Submits the ROR data download job to AWS Batch.

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


def ror_transform_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    file_name: str,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the ROR data transform job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the data.
        run_id: a unique ID to represent this run of the job.
        file_name: the name of the file to be transformed.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """

    return submit_job(
        job_name="ror-transform",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch ror transform $BUCKET_NAME $RUN_ID $FILE_NAME",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "FILE_NAME": file_name,
            }
        ),
        depends_on=depends_on,
    )


def openalex_funders_download_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    vcpus: int = NANO_VCPUS,
    memory: int = NANO_MEMORY,
) -> str:
    """
    Submits the OpenAlex funders data download job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the file to.
        run_id: a unique ID to represent this run of the job.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """

    return submit_job(
        job_name="openalex-funders-download",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-funders download $BUCKET_NAME $RUN_ID",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
            }
        ),
    )


def openalex_funders_transform_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    vcpus: int = NANO_VCPUS,
    memory: int = NANO_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the OpenAlex funders data transform job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the raw data.
        run_id: a unique ID to represent this run of the job.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        depends_on: optional list of job dependencies.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """

    return submit_job(
        job_name="openalex-funders-transform",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-funders transform $BUCKET_NAME $RUN_ID",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
            }
        ),
        depends_on=depends_on,
    )


def openalex_works_download_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
) -> str:
    """
    Submits the OpenAlex works data download job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the files to.
        run_id: a unique ID to represent this run of the job.
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
        command="dmpworks aws-batch openalex-works download $BUCKET_NAME $RUN_ID",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
            }
        ),
    )


def openalex_works_transform_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    max_file_processes: int = 8,
    batch_size: int = 8,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the OpenAlex works data transform job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the raw data.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        max_file_processes: max number of files to read in parallel.
        batch_size: number of records to process in a batch.
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
        command="dmpworks aws-batch openalex-works transform $BUCKET_NAME $RUN_ID --max-file-processes=$MAX_FILE_PROCESSES --batch-size=$BATCH_SIZE --use-subset=$USE_SUBSET",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "USE_SUBSET": str(use_subset).lower(),
                "MAX_FILE_PROCESSES": str(max_file_processes),
                "BATCH_SIZE": str(batch_size),
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
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
) -> str:
    """
    Submits the Crossref metadata download job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the file to.
        run_id: a unique ID to represent this run of the job.
        file_name: the name of the Crossref metadata file to download.
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
        command="dmpworks aws-batch crossref-metadata download $BUCKET_NAME $RUN_ID $FILE_NAME",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "FILE_NAME": file_name,
            }
        ),
    )


def crossref_metadata_transform_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the Crossref metadata transform job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the raw data.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
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
        command="dmpworks aws-batch crossref-metadata transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "USE_SUBSET": str(use_subset).lower(),
            }
        ),
        depends_on=depends_on,
    )


def datacite_download_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    allocation_id: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
) -> str:
    """
    Submits the DataCite data download job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket to download the file to.
        run_id: a unique ID to represent this run of the job.
        allocation_id: the AWS Elastic IP allocation ID.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.

    Returns:
        str: the job ID of the submitted AWS Batch job.
    """

    return submit_job(
        job_name="datacite-download",
        run_id=run_id,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch datacite download $BUCKET_NAME $RUN_ID $ALLOCATION_ID",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "ALLOCATION_ID": allocation_id,
            }
        ),
    )


def datacite_transform_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the DataCite data transform job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the raw data.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
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
        command="dmpworks aws-batch datacite transform $BUCKET_NAME $RUN_ID --use-subset=$USE_SUBSET",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "USE_SUBSET": str(use_subset).lower(),
            }
        ),
        depends_on=depends_on,
    )


def submit_sqlmesh_job(
    *,
    env: str,
    bucket_name: str,
    run_id: str,
    openalex_works_run_id: str,
    openalex_funders_run_id: str,
    datacite_run_id: str,
    crossref_metadata_run_id: str,
    ror_run_id: str,
    vcpus: int = VERY_LARGE_VCPUS,
    memory: int = VERY_LARGE_MEMORY,
    duckdb_threads: int = 32,
    duckdb_memory_limit: str = "200GB",
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the main SQLMesh transformation job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket for SQLMesh data.
        run_id: a unique ID to represent this run of the SQLMesh job.
        openalex_works_run_id: The run_id of the OpenAlex works data to use.
        openalex_funders_run_id: The run_id of the OpenAlex funders data to use.
        datacite_run_id: The run_id of the DataCite data to use.
        crossref_metadata_run_id: The run_id of the Crossref metadata to use.
        ror_run_id: The run_id of the ROR data to use.
        vcpus: number of vCPUs for the job.
        memory: memory (in MiB) for the job.
        duckdb_threads: Number of threads for DuckDB to use.
        duckdb_memory_limit: Memory limit for DuckDB (e.g., "200GB").
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
        command="dmpworks aws-batch sqlmesh plan $BUCKET_NAME $RUN_ID --release-dates.openalex-works $OPENALEX_WORKS_RUN_ID --release-dates.openalex-funders $OPENALEX_FUNDERS_RUN_ID --release-dates.datacite $DATACITE_RUN_ID --release-dates.crossref-metadata $CROSSREF_METADATA_RUN_ID --release-dates.ror $ROR_RUN_ID",
        environment=make_env(
            {
                "RUN_ID": run_id,
                "BUCKET_NAME": bucket_name,
                "OPENALEX_WORKS_RUN_ID": openalex_works_run_id,
                "OPENALEX_FUNDERS_RUN_ID": openalex_funders_run_id,
                "DATACITE_RUN_ID": datacite_run_id,
                "CROSSREF_METADATA_RUN_ID": crossref_metadata_run_id,
                "ROR_RUN_ID": ror_run_id,
                "SQLMESH__GATEWAYS__DUCKDB__CONNECTION__CONNECTOR_CONFIG__THREADS": str(duckdb_threads),
                "SQLMESH__GATEWAYS__DUCKDB__CONNECTION__CONNECTOR_CONFIG__MEMORY_LIMIT": duckdb_memory_limit,
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
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the OpenSearch works-index sync job to AWS Batch.

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
    bucket_name: str,
    dmps_run_id: str,
    host: str,
    region: str,
    index_name: str = "dmps-index",
    mode: str = "aws",
    port: int = 443,
    service: str = "es",
    max_processes: int = 2,
    chunk_size: int = 1000,
    max_retries: int = 3,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the OpenSearch dmps-index sync job to AWS Batch.

    Args:
        env: environment, i.e., dev, stage, prod.
        bucket_name: S3 bucket containing the data to sync.
        dmps_run_id: The run_id of the DMPs data to use.
        host: The OpenSearch host URL.
        region: The AWS region of the OpenSearch cluster.
        index_name: The name of the OpenSearch index to sync to.
        mode: Client connection mode (e.g., "aws").
        port: OpenSearch connection port.
        service: OpenSearch service name (e.g., "es").
        max_processes: Max number of processes for the sync job.
        chunk_size: Number of documents per sync chunk.
        max_retries: Max retries for failed chunks.
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
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch sync-dmps $BUCKET_NAME $RUN_ID $INDEX_NAME --client-config.mode=$MODE --client-config.host=$HOST --client-config.port=$PORT --client-config.region=$REGION --client-config.service=$SERVICE --sync-config.max-processes=$MAX_PROCESSES --sync-config.chunk-size=$CHUNK_SIZE --sync-config.max-retries=$MAX_RETRIES",
        environment=make_env(
            {
                "RUN_ID": dmps_run_id,
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
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the OpenSearch DMPs enrichment job to AWS Batch.

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
    dataset_subset: DatasetSubset,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Submits the OpenSearch DMP-to-Works search job to AWS Batch.

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
        dataset_subset: whether to filter the DMPs to a subset of institutions.
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

    if dataset_subset is not None:
        environment["DATASET_SUBSET_ENABLE"] = dataset_subset.enable
        environment["DATASET_SUBSET_INSTITUTIONS"] = dataset_subset.institutions

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
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    """
    Creates a job to merge related works.

    The database parameters are set in the job defintion and read by the Cyclopts
    built command line interface as environment variables.

    Args:
        env: environment, i.e., dev, stage, prod.
        dmps_run_id: the run_id of the DMPs data to use.
        bucket_name: S3 bucket containing search results to merge.
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
