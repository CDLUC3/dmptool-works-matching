import logging
from typing import Optional, TypedDict

import boto3
import pendulum


# CPU and memory groups
NANO_VCPUS = 1
NANO_MEMORY = 1024

SMALL_VCPUS = 2
SMALL_MEMORY = 3686

MEDIUM_VCPUS = 8
MEDIUM_MEMORY = 28_762

LARGE_VCPUS = 32
LARGE_MEMORY = 58_368

VERY_LARGE_VCPU = 32
VERY_LARGE_MEMORY = 250_880


class EnvVarDict(TypedDict):
    name: str
    value: str


class DependsOnDict(TypedDict):
    jobId: str


def format_job_date(job_date: pendulum.Date):
    return job_date.format("YYYY-MM-DD")


def standard_job_definition(env: str) -> str:
    return f"dmp-tool-{env}-batch-dmpworks-job"


def standard_job_queue(env: str) -> str:
    return f"dmp-tool-{env}-batch-job-queue"


def database_job_definition(env: str) -> str:
    return f"dmp-tool-{env}-batch-dmpworks-database-job"


def submit_job(
    *,
    job_name: str,
    job_date: pendulum.Date,
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

    # Submit job
    batch_client = boto3.client('batch')
    response = batch_client.submit_job(
        job_name=f"{job_name}-{format_job_date(job_date)}",
        job_queue=job_queue,
        job_definition=job_definition,
        depends_on=depends_on,
        container_overrides=container_overrides,
    )
    job_id = response['jobId']
    logging.info(f"Submitted job with ID: {job_id}")

    return job_id


def ror_download_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    download_url: str,
    hash: str,
    vcpus: int = NANO_VCPUS,
    memory: int = NANO_MEMORY,
):
    return submit_job(
        job_name="ror-download",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        command="dmpworks aws-batch ror download $BUCKET_NAME $TASK_ID $DOWNLOAD_URL --hash $HASH",
        environment=[
            {"name": "TASK_ID", "value": "2025-06-24"},
            {"name": "BUCKET_NAME", "value": bucket_name},
            {"name": "DOWNLOAD_URL", "value": download_url},
            {"name": "HASH", "value": hash},
        ],
        vcpus=vcpus,
        memory=memory,
    )


def ror_transform_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    file_name: str,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
):
    return submit_job(
        job_name="ror-transform",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch ror transform $BUCKET_NAME $TASK_ID $FILE_NAME",
        environment=[
            {"name": "TASK_ID", "value": "2025-06-24"},
            {"name": "BUCKET_NAME", "value": bucket_name},
            {"name": "FILE_NAME", "value": file_name},
        ],
        depends_on=depends_on,
    )


def openalex_funders_download_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    vcpus: int = NANO_VCPUS,
    memory: int = NANO_MEMORY,
):
    return submit_job(
        job_name="openalex-funders-download",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-funders download $BUCKET_NAME $TASK_ID",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
        ],
    )


def openalex_funders_transform_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    vcpus: int = NANO_VCPUS,
    memory: int = NANO_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
):
    return submit_job(
        job_name="openalex-funders-transform",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-funders transform $BUCKET_NAME $TASK_ID",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
        ],
        depends_on=depends_on,
    )


def openalex_works_download_job(
    *,
    env: str,
    job_date: pendulum.Date,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    bucket_name: str,
):
    return submit_job(
        job_name="openalex-works-download",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-works download $BUCKET_NAME $TASK_ID",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
        ],
    )


def openalex_works_transform_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
):
    return submit_job(
        job_name="openalex-works-transform",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch openalex-works transform $BUCKET_NAME $TASK_ID --max-file-processes=8 --batch-size=8",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
        ],
        depends_on=depends_on,
    )


def crossref_metadata_download_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    file_name: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
) -> str:
    return submit_job(
        job_name="crossref-metadata-download",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch crossref-metadata download $BUCKET_NAME $TASK_ID $FILE_NAME",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
            {"name": "FILE_NAME", "value": file_name},
        ],
    )


def crossref_metadata_transform_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    return submit_job(
        job_name="crossref-metadata-transform",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch crossref-metadata transform $BUCKET_NAME $TASK_ID",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
        ],
        depends_on=depends_on,
    )


def datacite_download_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    allocation_id: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
) -> str:
    return submit_job(
        job_name="datacite-download",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch datacite download $BUCKET_NAME $TASK_ID $ALLOCATION_ID",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
            {"name": "ALLOCATION_ID", "value": allocation_id},
        ],
    )


def datacite_transform_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    vcpus: int = LARGE_VCPUS,
    memory: int = LARGE_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    return submit_job(
        job_name="datacite-transform",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch datacite transform $BUCKET_NAME $TASK_ID",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
        ],
        depends_on=depends_on,
    )


def submit_sqlmesh_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    openalex_works_job_date: pendulum.Date,
    openalex_funders_job_date: pendulum.Date,
    datacite_job_date: pendulum.Date,
    crossref_metadata_job_date: pendulum.Date,
    ror_job_date: pendulum.Date,
    vcpus: int = VERY_LARGE_VCPU,
    memory: int = VERY_LARGE_MEMORY,
    duckdb_threads: int = 32,
    duckdb_memory_limit: str = "200GB",
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    return submit_job(
        job_name="sqlmesh",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch sqlmesh plan $BUCKET_NAME $TASK_ID --release-dates.openalex-works $OPENALEX_WORKS --release-dates.openalex-funders $OPENALEX_FUNDERS --release-dates.datacite $DATACITE --release-dates.crossref-metadata $CROSSREF_METADATA --release-dates.ror $ROR",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
            {"name": "OPENALEX_WORKS", "value": format_job_date(openalex_works_job_date)},
            {"name": "OPENALEX_FUNDERS", "value": format_job_date(openalex_funders_job_date)},
            {"name": "DATACITE", "value": format_job_date(datacite_job_date)},
            {"name": "CROSSREF_METADATA", "value": format_job_date(crossref_metadata_job_date)},
            {"name": "ROR", "value": format_job_date(ror_job_date)},
            {"name": "SQLMESH__GATEWAYS__DUCKDB__CONNECTION__CONNECTOR_CONFIG__THREADS", "value": str(duckdb_threads)},
            {
                "name": "SQLMESH__GATEWAYS__DUCKDB__CONNECTION__CONNECTOR_CONFIG__MEMORY_LIMIT",
                "value": duckdb_memory_limit,
            },
        ],
        depends_on=depends_on,
    )


def submit_sync_works_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    export_date: pendulum.Date,
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
    return submit_job(
        job_name="opensearch-sync-works",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch sync-works $BUCKET_NAME $EXPORT_DATE $INDEX_NAME --client-config.mode=$MODE --client-config.host=$HOST --client-config.port=$PORT --client-config.region=$REGION --client-config.service=$SERVICE --sync-config.max-processes=$MAX_PROCESSES --sync-config.chunk-size=$CHUNK_SIZE --sync-config.max-retries=$MAX_RETRIES",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
            {"name": "EXPORT_DATE", "value": export_date.format('YYYY-MM-DD')},
            {"name": "INDEX_NAME", "value": index_name},
            {"name": "MODE", "value": mode},
            {"name": "HOST", "value": host},
            {"name": "PORT", "value": str(port)},
            {"name": "REGION", "value": region},
            {"name": "SERVICE", "value": service},
            {"name": "MAX_PROCESSES", "value": str(max_processes)},
            {"name": "CHUNK_SIZE", "value": str(chunk_size)},
            {"name": "MAX_RETRIES", "value": str(max_retries)},
            {"name": "TQDM_POSITION", "value": "-1"},
            {"name": "TQDM_MININTERVAL", "value": "120"},
        ],
        depends_on=depends_on,
    )


def submit_sync_dmps_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    export_date: pendulum.Date,
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
    return submit_job(
        job_name="opensearch-sync-dmps",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch sync-dmps $BUCKET_NAME $EXPORT_DATE $INDEX_NAME --client-config.mode=$MODE --client-config.host=$HOST --client-config.port=$PORT --client-config.region=$REGION --client-config.service=$SERVICE --sync-config.max-processes=$MAX_PROCESSES --sync-config.chunk-size=$CHUNK_SIZE --sync-config.max-retries=$MAX_RETRIES",
        environment=[
            {"name": "TASK_ID", "value": format_job_date(job_date)},
            {"name": "BUCKET_NAME", "value": bucket_name},
            {"name": "EXPORT_DATE", "value": export_date.format('YYYY-MM-DD')},
            {"name": "INDEX_NAME", "value": index_name},
            {"name": "MODE", "value": mode},
            {"name": "HOST", "value": host},
            {"name": "PORT", "value": str(port)},
            {"name": "REGION", "value": region},
            {"name": "SERVICE", "value": service},
            {"name": "MAX_PROCESSES", "value": str(max_processes)},
            {"name": "CHUNK_SIZE", "value": str(chunk_size)},
            {"name": "MAX_RETRIES", "value": str(max_retries)},
            {"name": "TQDM_POSITION", "value": "-1"},
            {"name": "TQDM_MININTERVAL", "value": "120"},
        ],
        depends_on=depends_on,
    )


def submit_enrich_dmps_job(
    *,
    env: str,
    job_date: pendulum.Date,
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
    return submit_job(
        job_name="opensearch-enrich-dmps",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch enrich-dmps $INDEX_NAME --client-config.mode=$MODE --client-config.host=$HOST --client-config.port=$PORT --client-config.region=$REGION --client-config.service=$SERVICE",
        environment=[
            {"name": "INDEX_NAME", "value": index_name},
            {"name": "MODE", "value": mode},
            {"name": "HOST", "value": host},
            {"name": "PORT", "value": str(port)},
            {"name": "REGION", "value": region},
            {"name": "SERVICE", "value": service},
            {"name": "TQDM_POSITION", "value": "-1"},
            {"name": "TQDM_MININTERVAL", "value": "120"},
        ],
        depends_on=depends_on,
    )


def submit_dmp_works_search_job(
    *,
    env: str,
    job_date: pendulum.Date,
    host: str,
    region: str,
    dmp_index_name: str = "dmps-index",
    works_index_name: str = "works-index",
    mode: str = "aws",
    port: int = 443,
    service: str = "es",
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
) -> str:
    return submit_job(
        job_name="opensearch-dmp-works-search",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=standard_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch dmp-works-search $DMP_INDEX_NAME $WORKS_INDEX_NAME --client-config.mode=$MODE --client-config.host=$HOST --client-config.port=$PORT --client-config.region=$REGION --client-config.service=$SERVICE",
        environment=[
            {"name": "DMP_INDEX_NAME", "value": dmp_index_name},
            {"name": "WORKS_INDEX_NAME", "value": works_index_name},
            {"name": "MODE", "value": mode},
            {"name": "HOST", "value": host},
            {"name": "PORT", "value": str(port)},
            {"name": "REGION", "value": region},
            {"name": "SERVICE", "value": service},
            {"name": "TQDM_POSITION", "value": "-1"},
            {"name": "TQDM_MININTERVAL", "value": "120"},
        ],
        depends_on=depends_on,
    )


def submit_merge_related_works_job(
    *,
    env: str,
    job_date: pendulum.Date,
    bucket_name: str,
    export_date: pendulum.Date,
    vcpus: int = SMALL_VCPUS,
    memory: int = SMALL_MEMORY,
    depends_on: Optional[list[DependsOnDict]] = None,
):
    """Creates a job to merge related works.

    The database parameters are set in the job defintion and read by the Cyclopts
    built command line interface as environment variables.

    :param env:
    :param job_date:
    :param bucket_name:
    :param export_date:
    :param vcpus:
    :param memory:
    :param depends_on:
    :return:
    """

    return submit_job(
        job_name="opensearch-merge-related-works",
        job_date=job_date,
        job_queue=standard_job_queue(env),
        job_definition=database_job_definition(env),
        vcpus=vcpus,
        memory=memory,
        command="dmpworks aws-batch opensearch merge-related-works $BUCKET_NAME $EXPORT_DATE",
        environment=[
            {"name": "BUCKET_NAME", "value": bucket_name},
            {"name": "EXPORT_DATE", "value": format_job_date(export_date)},
            {"name": "TQDM_POSITION", "value": "-1"},
            {"name": "TQDM_MININTERVAL", "value": "120"},
        ],
        depends_on=depends_on,
    )
