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
    dmps_transform_job,
    openalex_works_download_job,
    openalex_works_transform_job,
    ror_download_job,
    submit_dmp_works_search_job,
    submit_enrich_dmps_job,
    submit_merge_related_works_job,
    submit_sqlmesh_job,
    submit_sync_dmps_job,
    submit_sync_works_job,
)
from dmpworks.cli_utils import DatasetSubset, DMPSubset

app = App(name="batch-submit", help="Commands to submit AWS Batch jobs.")

EnvTypes = Literal["dev", "stage", "prod"]
DEFAULT_NUM_WORKERS = 32


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


DMPS_JOBS: tuple[str, ...] = ("transform",)


@app.command(name="dmps")
def dmps_cmd(
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
            env_var="DMPS_RUN_ID",
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
        Literal[*DMPS_JOBS],
        Parameter(
            env_var="DMPS_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = DMPS_JOBS[0],
):
    logging.basicConfig(level=logging.INFO)

    task_definitions = {
        "transform": partial(
            dmps_transform_job,
            env=env,
            bucket_name=bucket_name,
            run_id=run_id,
        ),
    }

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=list(CROSSREF_METADATA_JOBS),
        start_task_name=start_job,
    )


ROR_JOBS: tuple[str, ...] = ("download",)


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
        )
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
    crossref_bucket_name: Annotated[
        str,
        Parameter(
            env_var="CROSSREF_METADATA_BUCKET_NAME",
            help="Name of the Crossref AWS S3 bucket.",
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
            crossref_bucket_name=crossref_bucket_name,
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
            dataset_subset=dataset_subset,
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
    datacite_bucket_name: Annotated[
        str,
        Parameter(
            env_var="DATACITE_BUCKET_NAME",
            help="Name of the DataCite AWS S3 bucket.",
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
            datacite_bucket_name=datacite_bucket_name,
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
            dataset_subset=dataset_subset,
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
    openalex_bucket_name: Annotated[
        str,
        Parameter(
            env_var="OPENALEX_BUCKET_NAME",
            help="Name of the OpenAlex AWS S3 bucket.",
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
            openalex_bucket_name=openalex_bucket_name,
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
            dataset_subset=dataset_subset,
        )
    else:
        task_order.remove(task_id)

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=task_order,
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
    prev_run_id: Annotated[
        str,
        Parameter(
            env_var="PREV_PROCESS_WORKS_RUN_ID",
            help="A unique ID for the previous SQLMesh run.",
        ),
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
    data_citation_corpus_run_id: Annotated[
        str,
        Parameter(
            env_var="DATA_CITATION_CORPUS_RUN_ID",
            help="The run_id of the Data Citation Corpus data to use (for SQLMesh).",
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
    sqlmesh_duckdb_memory_limit: Annotated[
        str,
        Parameter(
            env_var="SQLMESH_DUCKDB_MEMORY_LIMIT",
            help="Memory limit for DuckDB (e.g., '225GB') (for SQLMesh).",
        ),
    ] = "225GB",
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
    crossref_crossref_metadata_threads: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_CROSSREF_METADATA_THREADS",
            help="Memory limit for SQLMesh DuckDB crossref_crossref_metadata query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    crossref_index_works_metadata_threads: Annotated[
        int,
        Parameter(
            env_var="CROSSREF_INDEX_WORKS_METADATA_THREADS",
            help="Memory limit for SQLMesh DuckDB crossref_index_works_metadata query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_datacite_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_DATACITE_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_datacite query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_index_awards_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_AWARDS_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_index_awards query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_index_datacite_index_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_DATACITE_INDEX_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_index_datacite_index query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_index_funders_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_FUNDERS_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_index_funders query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_index_institutions_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_INSTITUTIONS_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_index_institutions query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_index_updated_dates_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_UPDATED_DATES_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_index_updated_dates query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_index_work_types_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_WORK_TYPES_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_index_work_types query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_index_works_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_WORKS_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_index_works query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    datacite_index_datacite_index_hashes_threads: Annotated[
        int,
        Parameter(
            env_var="DATACITE_INDEX_DATACITE_INDEX_HASHES_THREADS",
            help="Memory limit for SQLMesh DuckDB datacite_index_datacite_index_hashes query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_openalex_works_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_OPENALEX_WORKS_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_openalex_works query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_abstract_stats_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_ABSTRACT_STATS_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_abstract_stats query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_abstracts_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_ABSTRACTS_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_abstracts query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_author_names_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_AUTHOR_NAMES_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_author_names query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_awards_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_AWARDS_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_awards query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_funders_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_FUNDERS_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_funders query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_openalex_index_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_OPENALEX_INDEX_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_openalex_index query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_publication_dates_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_PUBLICATION_DATES_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_publication_dates query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_title_stats_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_TITLE_STATS_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_title_stats query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_titles_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_TITLES_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_titles query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_updated_dates_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_UPDATED_DATES_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_updated_dates query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_works_metadata_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_WORKS_METADATA_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_works_metadata query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    openalex_index_openalex_index_hashes_threads: Annotated[
        int,
        Parameter(
            env_var="OPENALEX_INDEX_OPENALEX_INDEX_HASHES_THREADS",
            help="Memory limit for SQLMesh DuckDB openalex_index_openalex_index_hashes query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    opensearch_current_doi_state_threads: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_CURRENT_DOI_STATE_THREADS",
            help="Memory limit for SQLMesh DuckDB opensearch_current_doi_state query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    opensearch_export_threads: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_EXPORT_THREADS",
            help="Memory limit for SQLMesh DuckDB opensearch_export query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    opensearch_next_doi_state_threads: Annotated[
        int,
        Parameter(
            env_var="OPENSEARCH_NEXT_DOI_STATE_THREADS",
            help="Memory limit for SQLMesh DuckDB opensearch_next_doi_state query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    data_citation_corpus_2_threads: Annotated[
        int,
        Parameter(
            env_var="DATA_CITATION_CORPUS_RELATIONS_THREADS",
            help="Memory limit for SQLMesh DuckDB data_citation_corpus_relations query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    relations_crossref_metadata_threads: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_CROSSREF_METADATA_THREADS",
            help="Memory limit for SQLMesh DuckDB relations_crossref_metadata query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    relations_data_citation_corpus_threads: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_DATA_CITATION_CORPUS_THREADS",
            help="Memory limit for SQLMesh DuckDB relations_data_citation_corpus query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    relations_datacite_threads: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_DATACITE_THREADS",
            help="Memory limit for SQLMesh DuckDB relations_datacite query (e.g., 16).",
        ),
    ] = 16,
    relations_relations_index_threads: Annotated[
        int,
        Parameter(
            env_var="RELATIONS_RELATIONS_INDEX_THREADS",
            help="Memory limit for SQLMesh DuckDB relations_relations_index query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    ror_index_threads: Annotated[
        int,
        Parameter(
            env_var="ROR_INDEX_THREADS",
            help="Memory limit for SQLMesh DuckDB ror_index query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    ror_ror_threads: Annotated[
        int,
        Parameter(
            env_var="ROR_ROR_THREADS",
            help="Memory limit for SQLMesh DuckDB ror_ror query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
    works_index_export_threads: Annotated[
        int,
        Parameter(
            env_var="WORKS_INDEX_EXPORT_THREADS",
            help="Memory limit for SQLMesh DuckDB works_index_export query (e.g., 32).",
        ),
    ] = DEFAULT_NUM_WORKERS,
):
    logging.basicConfig(level=logging.INFO)

    task_definitions = {
        "sqlmesh-transform": partial(
            submit_sqlmesh_job,
            env=env,
            bucket_name=bucket_name,
            prev_run_id=prev_run_id,
            run_id=run_id,
            openalex_works_run_id=openalex_works_run_id,
            datacite_run_id=datacite_run_id,
            crossref_metadata_run_id=crossref_metadata_run_id,
            ror_run_id=ror_run_id,
            data_citation_corpus_run_id=data_citation_corpus_run_id,
            duckdb_memory_limit=sqlmesh_duckdb_memory_limit,
            crossref_crossref_metadata_threads=crossref_crossref_metadata_threads,
            crossref_index_works_metadata_threads=crossref_index_works_metadata_threads,
            datacite_datacite_threads=datacite_datacite_threads,
            datacite_index_awards_threads=datacite_index_awards_threads,
            datacite_index_datacite_index_threads=datacite_index_datacite_index_threads,
            datacite_index_funders_threads=datacite_index_funders_threads,
            datacite_index_institutions_threads=datacite_index_institutions_threads,
            datacite_index_updated_dates_threads=datacite_index_updated_dates_threads,
            datacite_index_work_types_threads=datacite_index_work_types_threads,
            datacite_index_works_threads=datacite_index_works_threads,
            datacite_index_datacite_index_hashes_threads=datacite_index_datacite_index_hashes_threads,
            openalex_openalex_works_threads=openalex_openalex_works_threads,
            openalex_index_abstract_stats_threads=openalex_index_abstract_stats_threads,
            openalex_index_abstracts_threads=openalex_index_abstracts_threads,
            openalex_index_author_names_threads=openalex_index_author_names_threads,
            openalex_index_awards_threads=openalex_index_awards_threads,
            openalex_index_funders_threads=openalex_index_funders_threads,
            openalex_index_openalex_index_threads=openalex_index_openalex_index_threads,
            openalex_index_publication_dates_threads=openalex_index_publication_dates_threads,
            openalex_index_title_stats_threads=openalex_index_title_stats_threads,
            openalex_index_titles_threads=openalex_index_titles_threads,
            openalex_index_updated_dates_threads=openalex_index_updated_dates_threads,
            openalex_index_works_metadata_threads=openalex_index_works_metadata_threads,
            openalex_index_openalex_index_hashes_threads=openalex_index_openalex_index_hashes_threads,
            opensearch_current_doi_state_threads=opensearch_current_doi_state_threads,
            opensearch_export_threads=opensearch_export_threads,
            opensearch_next_doi_state_threads=opensearch_next_doi_state_threads,
            data_citation_corpus_relations_threads=data_citation_corpus_relations_threads,
            relations_crossref_metadata_threads=relations_crossref_metadata_threads,
            relations_data_citation_corpus_threads=relations_data_citation_corpus_threads,
            relations_datacite_threads=relations_datacite_threads,
            relations_relations_index_threads=relations_relations_index_threads,
            ror_index_threads=ror_index_threads,
            ror_ror_threads=ror_ror_threads,
            works_index_export_threads=works_index_export_threads,
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
    "dmp-works-search",
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
    sync_chunk_size: Annotated[
        int,
        Parameter(
            env_var="SYNC_CHUNK_SIZE",
            help="Number of documents per chunk for the sync-dmps job.",
        ),
    ] = 1000,
    dmp_subset: DMPSubset = None,
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
            dmps_run_id=dmps_run_id,
            host=opensearch_host,
            region=opensearch_region,
            index_name=opensearch_dmps_index_name,
            mode=opensearch_mode,
            port=opensearch_port,
            service=opensearch_service,
            chunk_size=sync_chunk_size,
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
        "dmp-works-search": partial(
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
            dmp_subset=dmp_subset,
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
