import inspect
import logging
from functools import partial
from typing import Annotated, Callable, Dict, Literal

from cyclopts import App, Parameter

from dmpworks.batch_submit.jobs import (
    crossref_metadata_download_job,
    crossref_metadata_transform_job,
    datacite_download_job,
    datacite_transform_job,
    dataset_subset_job,
    openalex_funders_download_job,
    openalex_funders_transform_job,
    openalex_works_download_job,
    openalex_works_transform_job,
    ror_download_job,
    ror_transform_job,
    submit_dmp_works_search_job,
    submit_enrich_dmps_job,
    submit_merge_related_works_job,
    submit_sqlmesh_job,
    submit_sync_dmps_job,
    submit_sync_works_job,
)
from dmpworks.cli_utils import DatasetSubset

app = App(name="batch-submit", help="Commands to submit AWS Batch jobs.")

EnvTypes = Literal["dev", "stage", "prod"]


def run_job_pipeline(
    *,
    task_definitions: Dict[str, Callable],
    task_order: list[str],
    start_task_name: str,
):
    # Build list of tasks
    try:
        start_index = task_order.index(start_task_name)
    except ValueError:
        msg = f"Unknown start_task '{start_task_name}'"
        logging.error(msg)
        raise ValueError(msg)
    task_names = task_order[start_index:]

    # Execute tasks
    job_ids = []
    for task_name in task_names:
        if task_name not in task_definitions:
            msg = f"No function defined for task '{task_name}'"
            logging.error(msg)
            raise ValueError(msg)

        # Add any dependent jobs
        task_func = task_definitions[task_name]
        kwargs = {}
        sig = inspect.signature(task_func)
        if "depends_on" in sig.parameters and len(job_ids):
            kwargs["depends_on"] = [job_ids[-1]]

        # Call the task
        job_id = task_func(**kwargs)
        if job_id is not None:
            job_ids.append({"jobId": str(job_id)})


ROR_JOBS: tuple[str, ...] = ("download", "transform")


@app.command(name="ror")
def ror_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="ENV",
            help="Environment (e.g., dev, stage, prod)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="ROR_RUN_ID",
            help="A unique ID to represent this run of the job.",
        ),
    ],
    bucket_name: Annotated[
        str,
        Parameter(
            env_var="BUCKET_NAME",
            help="S3 bucket name for job I/O.",
        ),
    ],
    download_url: Annotated[
        str,
        Parameter(
            env_var="ROR_DOWNLOAD_URL",
            help="The Zenodo download URL for the ROR data file.",
        ),
    ],
    hash: Annotated[
        str,
        Parameter(
            env_var="ROR_HASH",
            help="The expected hash of the data file.",
        ),
    ],
    file_name: Annotated[
        str,
        Parameter(
            env_var="ROR_FILE_NAME",
            help="The name of the file to be transformed.",
        ),
    ],
    start_job: Annotated[
        Literal[*ROR_JOBS],
        Parameter(
            env_var="ROR_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = ROR_JOBS[0],
):
    logging.basicConfig(level=logging.INFO)

    task_definitions = {
        "download": partial(
            ror_download_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            download_url=download_url,
            hash=hash,
        ),
        "transform": partial(
            ror_transform_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            file_name=file_name,
        ),
    }

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=list(ROR_JOBS),
        start_task_name=start_job,
    )


CROSSREF_METADATA_JOBS: tuple[str, ...] = ("download", "dataset-subset", "transform")


@app.command(name="crossref-metadata")
def crossref_metadata_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="ENV",
            help="Environment (e.g., dev, stage, prod)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="CROSSREF_METADATA_RUN_ID",
            help="A unique ID to represent this run of the job.",
        ),
    ],
    bucket_name: Annotated[
        str,
        Parameter(
            env_var="BUCKET_NAME",
            help="S3 bucket name for job I/O.",
        ),
    ],
    file_name: Annotated[
        str,
        Parameter(
            env_var="CROSSREF_METADATA_FILE_NAME",
            help="The name of the Crossref metadata file to download.",
        ),
    ],
    dataset_subset: DatasetSubset = None,
    start_job: Annotated[
        Literal[*CROSSREF_METADATA_JOBS],
        Parameter(
            env_var="CROSSREF_METADATA_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = CROSSREF_METADATA_JOBS[0],
):
    logging.basicConfig(level=logging.INFO)
    use_subset = dataset_subset is not None and dataset_subset.enable

    task_definitions = {
        "download": partial(
            crossref_metadata_download_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            file_name=file_name,
        ),
        "transform": partial(
            crossref_metadata_transform_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            use_subset=use_subset,
        ),
    }

    # Add dataset subset task
    task_id = "dataset-subset"
    task_order = list(CROSSREF_METADATA_JOBS)
    if use_subset:
        task_definitions[task_id] = partial(
            dataset_subset_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            dataset="crossref-metadata",
            institution_rors=dataset_subset.institution_rors,
            institution_names=dataset_subset.institution_names,
        )
    else:
        task_order.remove(task_id)

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=task_order,
        start_task_name=start_job,
    )


DATACITE_JOBS: tuple[str, ...] = ("download", "dataset-subset", "transform")


@app.command(name="datacite")
def datacite_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="ENV",
            help="Environment (e.g., dev, stage, prod)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="DATACITE_RUN_ID",
            help="A unique ID to represent this run of the job.",
        ),
    ],
    bucket_name: Annotated[
        str,
        Parameter(
            env_var="BUCKET_NAME",
            help="S3 bucket name for job I/O.",
        ),
    ],
    allocation_id: Annotated[
        str,
        Parameter(
            env_var="DATACITE_ALLOCATION_ID",
            help="The DataCite allocation ID for the download.",
        ),
    ],
    dataset_subset: DatasetSubset = None,
    start_job: Annotated[
        Literal[*DATACITE_JOBS],
        Parameter(
            env_var="DATACITE_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = DATACITE_JOBS[0],
):
    logging.basicConfig(level=logging.INFO)
    use_subset = dataset_subset is not None and dataset_subset.enable

    task_definitions = {
        "download": partial(
            datacite_download_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            allocation_id=allocation_id,
        ),
        "transform": partial(
            datacite_transform_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            use_subset=use_subset,
        ),
    }

    # Add dataset subset task
    task_id = "dataset-subset"
    task_order = list(DATACITE_JOBS)
    if use_subset:
        task_definitions[task_id] = partial(
            dataset_subset_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            dataset="datacite",
            institutions=dataset_subset.institutions,
        )
    else:
        task_order.remove(task_id)

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=task_order,
        start_task_name=start_job,
    )


OPENALEX_WORKS_JOBS: tuple[str, ...] = ("download", "dataset-subset", "transform")


@app.command(name="openalex-works")
def openalex_works_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="ENV",
            help="Environment (e.g., dev, stage, prod)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="OPENALEX_WORKS_RUN_ID",
            help="A unique ID to represent this run of the job.",
        ),
    ],
    bucket_name: Annotated[
        str,
        Parameter(
            env_var="BUCKET_NAME",
            help="S3 bucket name.",
        ),
    ],
    max_file_processes: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_MAX_FILE_PROCESSES",
            help="The max number of files to read in parallel.",
        ),
    ] = 8,
    batch_size: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_BATCH_SIZE",
            help="Number of records to process in a batch.",
        ),
    ] = 8,
    dataset_subset: DatasetSubset = None,
    start_job: Annotated[
        Literal[*OPENALEX_WORKS_JOBS],
        Parameter(
            env_var="OPENALEX_WORKS_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = OPENALEX_WORKS_JOBS[0],
):
    logging.basicConfig(level=logging.INFO)
    use_subset = dataset_subset is not None and dataset_subset.enable

    task_definitions = {
        "download": partial(
            openalex_works_download_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
        ),
        "transform": partial(
            openalex_works_transform_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            max_file_processes=max_file_processes,
            batch_size=batch_size,
            use_subset=use_subset,
        ),
    }
    # Add dataset subset task
    task_id = "dataset-subset"
    task_order = list(OPENALEX_WORKS_JOBS)
    if use_subset:
        task_definitions[task_id] = partial(
            dataset_subset_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            dataset="openalex-works",
            institutions=dataset_subset.institutions,
        )
    else:
        task_order.remove(task_id)

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=task_order,
        start_task_name=start_job,
    )


OPENALEX_FUNDERS_JOBS: tuple[str, ...] = ("download", "transform")


@app.command(name="openalex-funders")
def openalex_funders_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="ENV",
            help="Environment (e.g., dev, stage, prod)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="OPENALEX_FUNDERS_RUN_ID",
            help="A unique ID to represent this run of the job.",
        ),
    ],
    bucket_name: Annotated[
        str,
        Parameter(
            env_var="BUCKET_NAME",
            help="S3 bucket name for job I/O.",
        ),
    ],
    start_job: Annotated[
        Literal[*OPENALEX_FUNDERS_JOBS],
        Parameter(
            env_var="OPENALEX_FUNDERS_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = OPENALEX_FUNDERS_JOBS[0],
):
    logging.basicConfig(level=logging.INFO)

    task_definitions = {
        "download": partial(
            openalex_funders_download_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
        ),
        "transform": partial(
            openalex_funders_transform_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
        ),
    }

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=list(OPENALEX_FUNDERS_JOBS),
        start_task_name=start_job,
    )


PROCESS_WORKS_JOBS: tuple[str, ...] = ("sqlmesh-transform", "sync-works")


@app.command(name="process-works")
def process_works_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="ENV", help="Environment (e.g., dev, stage, prod)"),
    ],
    bucket_name: Annotated[
        str,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name for job I/O."),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="PROCESS_WORKS_RUN_ID",
            help="A unique ID for this SQLMesh run.",
        ),
    ],
    openalex_works_run_id: Annotated[
        str,
        Parameter(
            env_var="OPENALEX_WORKS_RUN_ID",
            help="The run_id of the OpenAlex works data to use (for SQLMesh).",
        ),
    ],
    openalex_funders_run_id: Annotated[
        str,
        Parameter(
            env_var="OPENALEX_FUNDERS_RUN_ID",
            help="The run_id of the OpenAlex funders data to use (for SQLMesh).",
        ),
    ],
    datacite_run_id: Annotated[
        str,
        Parameter(
            env_var="DATACITE_RUN_ID",
            help="The run_id of the DataCite data to use (for SQLMesh).",
        ),
    ],
    crossref_metadata_run_id: Annotated[
        str,
        Parameter(
            env_var="CROSSREF_METADATA_RUN_ID",
            help="The run_id of the Crossref metadata to use (for SQLMesh).",
        ),
    ],
    ror_run_id: Annotated[
        str,
        Parameter(
            env_var="ROR_RUN_ID",
            help="The run_id of the ROR data to use (for SQLMesh).",
        ),
    ],
    opensearch_host: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_HOST",
            help="The OpenSearch host URL (for sync-works).",
        ),
    ],
    opensearch_region: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_REGION",
            help="The AWS region of the OpenSearch cluster (for sync-works).",
        ),
    ],
    sqlmesh_duckdb_threads: Annotated[
        int,
        Parameter(
            env_var="SQLMESH_DUCKDB_THREADS",
            help="Number of threads for DuckDB (for SQLMesh).",
        ),
    ] = 32,
    sqlmesh_duckdb_memory_limit: Annotated[
        str,
        Parameter(
            env_var="SQLMESH_DUCKDB_MEMORY_LIMIT",
            help="Memory limit for DuckDB (e.g., '200GB') (for SQLMesh).",
        ),
    ] = "200GB",
    opensearch_index_name: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_INDEX_NAME",
            help="The name of the OpenSearch index to sync to (for sync-works).",
        ),
    ] = "works-index",
    opensearch_mode: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_MODE",
            help="Client connection mode (e.g., 'aws') (for sync-works).",
        ),
    ] = "aws",
    opensearch_port: Annotated[
        int,
        Parameter(env_var="OPENSEARCH_PORT", help="OpenSearch connection port (for sync-works)."),
    ] = 443,
    opensearch_service: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_SERVICE",
            help="OpenSearch service name (e.g., 'es') (for sync-works).",
        ),
    ] = "es",
    sync_max_processes: Annotated[
        int,
        Parameter(
            env_var="SYNC_MAX_PROCESSES",
            help="Max number of processes for the sync job (for sync-works).",
        ),
    ] = 16,
    sync_chunk_size: Annotated[
        int,
        Parameter(
            env_var="SYNC_CHUNK_SIZE",
            help="Number of documents per sync chunk (for sync-works).",
        ),
    ] = 1000,
    sync_max_retries: Annotated[
        int,
        Parameter(
            env_var="SYNC_MAX_RETRIES",
            help="Max retries for failed chunks (for sync-works).",
        ),
    ] = 3,
    start_job: Annotated[
        Literal[*PROCESS_WORKS_JOBS],
        Parameter(
            env_var="PROCESS_WORKS_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = PROCESS_WORKS_JOBS[0],
):
    logging.basicConfig(level=logging.INFO)

    task_definitions = {
        "sqlmesh-transform": partial(
            submit_sqlmesh_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
            openalex_works_run_id=openalex_works_run_id,
            openalex_funders_run_id=openalex_funders_run_id,
            datacite_run_id=datacite_run_id,
            crossref_metadata_run_id=crossref_metadata_run_id,
            ror_run_id=ror_run_id,
            duckdb_threads=sqlmesh_duckdb_threads,
            duckdb_memory_limit=sqlmesh_duckdb_memory_limit,
        ),
        "sync-works": partial(
            submit_sync_works_job,
            env=env,
            bucket_name=bucket_name,
            sqlmesh_run_id=run_id,
            host=opensearch_host,
            region=opensearch_region,
            index_name=opensearch_index_name,
            mode=opensearch_mode,
            port=opensearch_port,
            service=opensearch_service,
            max_processes=sync_max_processes,
            chunk_size=sync_chunk_size,
            max_retries=sync_max_retries,
        ),
    }

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=list(PROCESS_WORKS_JOBS),
        start_task_name=start_job,
    )


PROCESS_DMPS_JOBS: tuple[str, ...] = (
    "sync-dmps",
    "enrich-dmps",
    "dmps-work-search",
    "merge-related-works",
)


@app.command(name="process-dmps")
def process_dmps_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="ENV", help="Environment (e.g., dev, stage, prod)"),
    ],
    bucket_name: Annotated[
        str,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name for job I/O."),
    ],
    dmps_run_id: Annotated[
        str,
        Parameter(
            env_var="DMPS_RUN_ID",
            help="A unique ID of the run containing the DMPs file.",
        ),
    ],
    opensearch_host: Annotated[
        str,
        Parameter(env_var="OPENSEARCH_HOST", help="The OpenSearch host URL."),
    ],
    opensearch_region: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_REGION",
            help="The AWS region of the OpenSearch cluster.",
        ),
    ],
    opensearch_dmps_index_name: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_DMPS_INDEX_NAME",
            help="The name of the OpenSearch DMPs index.",
        ),
    ] = "dmps-index",
    opensearch_works_index_name: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_WORKS_INDEX_NAME",
            help="The name of the OpenSearch works index.",
        ),
    ] = "works-index",
    opensearch_mode: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_MODE",
            help="Client connection mode (e.g., 'aws').",
        ),
    ] = "aws",
    opensearch_port: Annotated[
        int,
        Parameter(env_var="OPENSEARCH_PORT", help="OpenSearch connection port."),
    ] = 443,
    opensearch_service: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_SERVICE",
            help="OpenSearch service name (e.g., 'es').",
        ),
    ] = "es",
    sync_max_processes: Annotated[
        int,
        Parameter(
            env_var="SYNC_MAX_PROCESSES",
            help="Max number of processes for the sync-dmps job.",
        ),
    ] = 2,
    sync_chunk_size: Annotated[
        int,
        Parameter(
            env_var="SYNC_CHUNK_SIZE",
            help="Number of documents per chunk for the sync-dmps job.",
        ),
    ] = 1000,
    sync_max_retries: Annotated[
        int,
        Parameter(
            env_var="SYNC_MAX_RETRIES",
            help="Max retries for failed chunks for the sync-dmps job.",
        ),
    ] = 3,
    dataset_subset: DatasetSubset = None,
    start_job: Annotated[
        Literal[*PROCESS_DMPS_JOBS],
        Parameter(
            env_var="PROCESS_DMPS_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = PROCESS_DMPS_JOBS[0],
):
    logging.basicConfig(level=logging.INFO)

    task_definitions = {
        "sync-dmps": partial(
            submit_sync_dmps_job,
            env=env,
            bucket_name=bucket_name,
            dmps_run_id=dmps_run_id,
            host=opensearch_host,
            region=opensearch_region,
            index_name=opensearch_dmps_index_name,
            mode=opensearch_mode,
            port=opensearch_port,
            service=opensearch_service,
            max_processes=sync_max_processes,
            chunk_size=sync_chunk_size,
            max_retries=sync_max_retries,
        ),
        "enrich-dmps": partial(
            submit_enrich_dmps_job,
            env=env,
            dmps_run_id=dmps_run_id,
            host=opensearch_host,
            region=opensearch_region,
            index_name=opensearch_dmps_index_name,
            mode=opensearch_mode,
            port=opensearch_port,
            service=opensearch_service,
        ),
        "dmps-work-search": partial(
            submit_dmp_works_search_job,
            env=env,
            bucket_name=bucket_name,
            dmps_run_id=dmps_run_id,
            host=opensearch_host,
            region=opensearch_region,
            dmps_index_name=opensearch_dmps_index_name,
            works_index_name=opensearch_works_index_name,
            mode=opensearch_mode,
            port=opensearch_port,
            service=opensearch_service,
            institutions=dataset_subset.institutions if dataset_subset is not None and dataset_subset.enable else None,
        ),
        "merge-related-works": partial(
            submit_merge_related_works_job,
            env=env,
            bucket_name=bucket_name,
            dmps_run_id=dmps_run_id,
        ),
    }

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=list(PROCESS_DMPS_JOBS),
        start_task_name=start_job,
    )


if __name__ == "__main__":
    app()
