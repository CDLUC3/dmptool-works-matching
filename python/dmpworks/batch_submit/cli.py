from functools import partial
import logging
from typing import Annotated, Literal

from cyclopts import App, Parameter

from dmpworks.cli_utils import (
    CrossrefMetadataTransformConfig,
    DataCiteTransformConfig,
    DatasetSubsetAWS,
    DMPSubsetAWS,
    DMPWorksSearchConfig,
    LogLevel,
    OpenAlexWorksTransformConfig,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
    RunIdentifiers,
    SQLMeshConfig,
)

app = App(name="batch-submit", help="Commands to submit AWS Batch jobs.")

EnvTypes = Literal["dev", "stg", "prd"]
ROR_JOBS: tuple[str, ...] = ("download",)
CROSSREF_METADATA_JOBS: tuple[str, ...] = ("download", "dataset-subset", "transform")
DATACITE_JOBS: tuple[str, ...] = ("download", "dataset-subset", "transform")
OPENALEX_WORKS_JOBS: tuple[str, ...] = ("download", "dataset-subset", "transform")
PROCESS_WORKS_JOBS: tuple[str, ...] = ("sqlmesh-transform", "sync-works")
PROCESS_DMPS_JOBS: tuple[str, ...] = (
    "sync-dmps",
    "enrich-dmps",
    "dmp-works-search",
    "merge-related-works",
)


@app.command(name="ror")
def ror_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="AWS_ENV",
            help="Environment (e.g., dev, stg, prd)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="RUN_ID_ROR",
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
    """Submit ROR processing jobs.

    Args:
        env: Environment (e.g., dev, stg, prd).
        run_id: A unique ID to represent this run of the job.
        bucket_name: S3 bucket name for job I/O.
        download_url: The Zenodo download URL for the ROR data file.
        hash: The expected hash of the data file.
        start_job: The first job to run in the sequence.
    """
    from dmpworks.batch_submit.jobs import run_job_pipeline, submit_factory_job

    logging.basicConfig(level=logging.INFO)

    task_definitions = {
        "download": partial(
            submit_factory_job,
            factory_key=("ror", "download"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            download_url=download_url,
            file_hash=hash,
        )
    }

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=list(ROR_JOBS),
        start_task_name=start_job,
    )


@app.command(name="crossref-metadata")
def crossref_metadata_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="AWS_ENV",
            help="Environment (e.g., dev, stg, prd)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="RUN_ID_CROSSREF_METADATA",
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
    crossref_metadata_bucket_name: Annotated[
        str,
        Parameter(
            env_var="CROSSREF_METADATA_BUCKET_NAME",
            help="Name of the Crossref AWS S3 bucket.",
        ),
    ],
    dataset_subset: DatasetSubsetAWS = None,
    config: CrossrefMetadataTransformConfig | None = None,
    log_level: LogLevel = "INFO",
    start_job: Annotated[
        Literal[*CROSSREF_METADATA_JOBS],
        Parameter(
            env_var="CROSSREF_METADATA_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = CROSSREF_METADATA_JOBS[0],
):
    """Submit Crossref Metadata processing jobs.

    Args:
        env: Environment (e.g., dev, stg, prd).
        run_id: A unique ID to represent this run of the job.
        bucket_name: S3 bucket name for job I/O.
        file_name: The name of the Crossref metadata file to download.
        crossref_metadata_bucket_name: Name of the Crossref AWS S3 bucket.
        dataset_subset: Configuration for creating a subset of the dataset.
        config: Configuration for the transform job.
        log_level: The logging level for the transform job.
        start_job: The first job to run in the sequence.
    """
    from dmpworks.batch_submit.jobs import run_job_pipeline, submit_factory_job
    from dmpworks.cli_utils import config_to_kwargs

    logging.basicConfig(level=logging.INFO)
    use_subset = dataset_subset is not None and dataset_subset.enable
    config = CrossrefMetadataTransformConfig() if config is None else config

    task_definitions = {
        "download": partial(
            submit_factory_job,
            factory_key=("crossref-metadata", "download"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            file_name=file_name,
            crossref_metadata_bucket_name=crossref_metadata_bucket_name,
        ),
        "transform": partial(
            submit_factory_job,
            factory_key=("crossref-metadata", "transform"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            use_subset=use_subset,
            log_level=log_level,
            **config_to_kwargs(config),
        ),
    }

    # Add dataset subset task
    task_order = list(CROSSREF_METADATA_JOBS)
    if use_subset:
        task_definitions["dataset-subset"] = partial(
            submit_factory_job,
            factory_key=("crossref-metadata", "subset"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            dataset="crossref-metadata",
            **config_to_kwargs(dataset_subset),
        )
    else:
        task_order.remove("dataset-subset")

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=task_order,
        start_task_name=start_job,
    )


@app.command(name="datacite")
def datacite_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="AWS_ENV",
            help="Environment (e.g., dev, stg, prd)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="RUN_ID_DATACITE",
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
    dataset_subset: DatasetSubsetAWS = None,
    config: DataCiteTransformConfig | None = None,
    log_level: LogLevel = "INFO",
    start_job: Annotated[
        Literal[*DATACITE_JOBS],
        Parameter(
            env_var="DATACITE_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = DATACITE_JOBS[0],
):
    """Submit DataCite processing jobs.

    Args:
        env: Environment (e.g., dev, stg, prd).
        run_id: A unique ID to represent this run of the job.
        bucket_name: S3 bucket name for job I/O.
        datacite_bucket_name: Name of the DataCite AWS S3 bucket.
        dataset_subset: Configuration for creating a subset of the dataset.
        config: Configuration for the transform job.
        log_level: The logging level for the transform job.
        start_job: The first job to run in the sequence.
    """
    from dmpworks.batch_submit.jobs import run_job_pipeline, submit_factory_job
    from dmpworks.cli_utils import config_to_kwargs

    logging.basicConfig(level=logging.INFO)
    use_subset = dataset_subset is not None and dataset_subset.enable
    config = DataCiteTransformConfig() if config is None else config

    task_definitions = {
        "download": partial(
            submit_factory_job,
            factory_key=("datacite", "download"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            datacite_bucket_name=datacite_bucket_name,
        ),
        "transform": partial(
            submit_factory_job,
            factory_key=("datacite", "transform"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            use_subset=use_subset,
            log_level=log_level,
            **config_to_kwargs(config),
        ),
    }

    # Add dataset subset task
    task_order = list(DATACITE_JOBS)
    if use_subset:
        task_definitions["dataset-subset"] = partial(
            submit_factory_job,
            factory_key=("datacite", "subset"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            dataset="datacite",
            **config_to_kwargs(dataset_subset),
        )
    else:
        task_order.remove("dataset-subset")

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=task_order,
        start_task_name=start_job,
    )


@app.command(name="openalex-works")
def openalex_works_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="AWS_ENV",
            help="Environment (e.g., dev, stg, prd)",
        ),
    ],
    run_id: Annotated[
        str,
        Parameter(
            env_var="RUN_ID_OPENALEX_WORKS",
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
    dataset_subset: DatasetSubsetAWS = None,
    config: OpenAlexWorksTransformConfig | None = None,
    log_level: LogLevel = "INFO",
    start_job: Annotated[
        Literal[*OPENALEX_WORKS_JOBS],
        Parameter(
            env_var="OPENALEX_WORKS_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = OPENALEX_WORKS_JOBS[0],
):
    """Submit OpenAlex Works processing jobs.

    Args:
        env: Environment (e.g., dev, stg, prd).
        run_id: A unique ID to represent this run of the job.
        bucket_name: S3 bucket name.
        openalex_bucket_name: Name of the OpenAlex AWS S3 bucket.
        dataset_subset: Configuration for creating a subset of the dataset.
        config: Configuration for the transform job.
        log_level: The logging level for the transform job.
        start_job: The first job to run in the sequence.
    """
    from dmpworks.batch_submit.jobs import run_job_pipeline, submit_factory_job
    from dmpworks.cli_utils import config_to_kwargs

    logging.basicConfig(level=logging.INFO)
    use_subset = dataset_subset is not None and dataset_subset.enable
    config = OpenAlexWorksTransformConfig() if config is None else config

    task_definitions = {
        "download": partial(
            submit_factory_job,
            factory_key=("openalex-works", "download"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            openalex_bucket_name=openalex_bucket_name,
        ),
        "transform": partial(
            submit_factory_job,
            factory_key=("openalex-works", "transform"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            use_subset=use_subset,
            log_level=log_level,
            **config_to_kwargs(config),
        ),
    }
    # Add dataset subset task
    task_order = list(OPENALEX_WORKS_JOBS)
    if use_subset:
        task_definitions["dataset-subset"] = partial(
            submit_factory_job,
            factory_key=("openalex-works", "subset"),
            run_id=run_id,
            env=env,
            bucket_name=bucket_name,
            dataset="openalex-works",
            **config_to_kwargs(dataset_subset),
        )
    else:
        task_order.remove("dataset-subset")

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=task_order,
        start_task_name=start_job,
    )


@app.command(name="process-works")
def process_works_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(
            env_var="AWS_ENV",
            help="Environment (e.g., dev, stg, prd)",
        ),
    ],
    bucket_name: Annotated[
        str,
        Parameter(
            env_var="BUCKET_NAME",
            help="S3 bucket name for job I/O.",
        ),
    ],
    run_identifiers: RunIdentifiers,
    sqlmesh_config: SQLMeshConfig,
    os_client_config: OpenSearchClientConfig | None = None,
    os_sync_config: OpenSearchSyncConfig | None = None,
    works_index_name: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_WORKS_INDEX_NAME",
            help="The name of the OpenSearch works index.",
        ),
    ] = "works-index",
    start_job: Annotated[
        Literal[*PROCESS_WORKS_JOBS],
        Parameter(
            env_var="PROCESS_WORKS_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = PROCESS_WORKS_JOBS[0],
):
    """Submit Process Works jobs (SQLMesh and Sync Works).

    Args:
        env: Environment (e.g., dev, stg, prd).
        bucket_name: S3 bucket name for job I/O.
        run_identifiers: Unique identifiers for each data source.
        sqlmesh_config: The SQLMesh config.
        os_client_config: The OpenSearch client config.
        os_sync_config: The OpenSearch sync config.
        works_index_name: The name of the OpenSearch works index.
        start_job: The first job to run in the sequence.
    """
    from dmpworks.batch_submit.jobs import run_job_pipeline, submit_factory_job
    from dmpworks.cli_utils import config_to_kwargs

    logging.basicConfig(level=logging.INFO)

    if os_client_config is None:
        os_client_config = OpenSearchClientConfig()
    if os_sync_config is None:
        os_sync_config = OpenSearchSyncConfig()

    sqlmesh_kwargs = config_to_kwargs(run_identifiers, sqlmesh_config)
    os_kwargs = config_to_kwargs(os_client_config, os_sync_config)

    task_definitions = {
        "sqlmesh-transform": partial(
            submit_factory_job,
            factory_key=("process-works", "sqlmesh"),
            run_id=run_identifiers.run_id_sqlmesh,
            env=env,
            bucket_name=bucket_name,
            **sqlmesh_kwargs,
        ),
        "sync-works": partial(
            submit_factory_job,
            factory_key=("process-works", "sync-works"),
            run_id=run_identifiers.run_id_sqlmesh,
            env=env,
            bucket_name=bucket_name,
            sqlmesh_run_id=run_identifiers.run_id_sqlmesh,
            index_name=works_index_name,
            **os_kwargs,
        ),
    }

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=list(PROCESS_WORKS_JOBS),
        start_task_name=start_job,
    )


@app.command(name="process-dmps")
def process_dmps_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)"),
    ],
    bucket_name: Annotated[
        str,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name for job I/O."),
    ],
    run_id_dmps: Annotated[
        str,
        Parameter(
            env_var="RUN_ID_DMPS",
            help="A unique ID of the run containing the DMPs file.",
        ),
    ],
    dmps_index_name: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_DMPS_INDEX_NAME",
            help="The name of the OpenSearch DMPs index.",
        ),
    ] = "dmps-index",
    works_index_name: Annotated[
        str,
        Parameter(
            env_var="OPENSEARCH_WORKS_INDEX_NAME",
            help="The name of the OpenSearch works index.",
        ),
    ] = "works-index",
    os_client_config: OpenSearchClientConfig | None = None,
    os_sync_config: OpenSearchSyncConfig | None = None,
    dmp_subset: DMPSubsetAWS = None,
    dmp_works_search_config: DMPWorksSearchConfig | None = None,
    start_job: Annotated[
        Literal[*PROCESS_DMPS_JOBS],
        Parameter(
            env_var="PROCESS_DMPS_START_JOB",
            help="The first job to run in the sequence.",
        ),
    ] = PROCESS_DMPS_JOBS[0],
):
    """Submit Process DMPs jobs (Sync, Enrich, Search, Merge).

    Args:
        env: Environment (e.g., dev, stg, prd).
        bucket_name: S3 bucket name for job I/O.
        run_id_dmps: A unique ID of the run containing the DMPs file.
        dmps_index_name: The name of the OpenSearch DMPs index.
        works_index_name: The name of the OpenSearch works index.
        os_client_config: The OpenSearch client config.
        os_sync_config: The OpenSearch sync config.
        dmp_subset: Configuration for creating a subset of DMPs.
        dmp_works_search_config: DMP works search settings.
        start_job: The first job to run in the sequence.
    """
    from dmpworks.batch_submit.jobs import run_job_pipeline, submit_factory_job
    from dmpworks.cli_utils import config_to_kwargs

    logging.basicConfig(level=logging.INFO)

    if os_client_config is None:
        os_client_config = OpenSearchClientConfig()
    if os_sync_config is None:
        os_sync_config = OpenSearchSyncConfig()
    if dmp_works_search_config is None:
        dmp_works_search_config = DMPWorksSearchConfig()

    os_kwargs = config_to_kwargs(os_client_config, os_sync_config)
    subset_kwargs = config_to_kwargs(dmp_subset) if dmp_subset else {}
    search_kwargs = config_to_kwargs(dmp_works_search_config)

    task_definitions = {
        "sync-dmps": partial(
            submit_factory_job,
            factory_key=("process-dmps", "sync-dmps"),
            run_id=run_id_dmps,
            env=env,
            bucket_name=bucket_name,
            dmps_index_name=dmps_index_name,
            **os_kwargs,
            **subset_kwargs,
        ),
        "enrich-dmps": partial(
            submit_factory_job,
            factory_key=("process-dmps", "enrich-dmps"),
            run_id=run_id_dmps,
            env=env,
            bucket_name=bucket_name,
            dmps_index_name=dmps_index_name,
            **config_to_kwargs(os_client_config),
            **subset_kwargs,
        ),
        "dmp-works-search": partial(
            submit_factory_job,
            factory_key=("process-dmps", "dmp-works-search"),
            run_id=run_id_dmps,
            env=env,
            bucket_name=bucket_name,
            dmps_index_name=dmps_index_name,
            works_index_name=works_index_name,
            run_all_dmps=False,
            **config_to_kwargs(os_client_config),
            **search_kwargs,
            **subset_kwargs,
        ),
        "merge-related-works": partial(
            submit_factory_job,
            factory_key=("process-dmps", "merge-related-works"),
            run_id=run_id_dmps,
            env=env,
            bucket_name=bucket_name,
            search_run_id=run_id_dmps,
        ),
    }

    run_job_pipeline(
        task_definitions=task_definitions,
        task_order=list(PROCESS_DMPS_JOBS),
        start_task_name=start_job,
    )


if __name__ == "__main__":
    app()
