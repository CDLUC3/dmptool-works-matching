import logging
from typing import Annotated

from cyclopts import App, Parameter

from dmpworks.cli_utils import (
    CrossrefMetadataTransformConfig,
    DataCiteTransformConfig,
    DatasetSubsetAWS,
    DMPSubsetAWS,
    DMPWorksSearchConfig,
    LogLevel,
    MySQLConfig,
    OpenAlexWorksTransformConfig,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
    RunIdentifiers,
    SQLMeshConfig,
)

app = App(name="aws-batch", help="AWS Batch pipelines.")
datacite_app = App(name="datacite", help="DataCite AWS Batch pipeline.")
crossref_metadata_app = App(name="crossref-metadata", help="Crossref Metadata AWS Batch pipeline.")
ror_app = App(name="ror", help="ROR AWS Batch pipeline.")
data_citation_corpus_app = App(name="data-citation-corpus", help="Data Citation Corpus AWS Batch pipeline.")
sqlmesh_app = App(name="sqlmesh", help="SQLMesh AWS Batch pipeline.")
opensearch_app = App(name="opensearch", help="OpenSearch AWS Batch pipeline.")

app.command(ror_app)
app.command(crossref_metadata_app)
app.command(datacite_app)
app.command(data_citation_corpus_app)
app.command(sqlmesh_app)
app.command(opensearch_app)


@data_citation_corpus_app.command(name="download")
def data_citation_corpus_download_cmd(
    bucket_name: str,
    run_id: str,
    download_url: str,
    file_hash: str | None = None,
    log_level: LogLevel = "INFO",
):
    """Download Data Citation Corpus from Zenodo and upload to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        download_url: the Zenodo download URL for the Data Citation Corpus JSON zip file.
        file_hash: the MD5 sum of the file.
        log_level: Python log level.
    """
    from dmpworks.batch import data_citation_corpus
    from dmpworks.utils import setup_multiprocessing_logging

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    data_citation_corpus.download(
        bucket_name=bucket_name, run_id=run_id, download_url=download_url, file_hash=file_hash
    )


@datacite_app.command(name="download")
def datacite_download_cmd(
    bucket_name: str,
    run_id: str,
    datacite_bucket_name: str,
    log_level: LogLevel = "INFO",
):
    """Download DataCite from the DataCite S3 bucket and upload to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        datacite_bucket_name: the name of the DataCite AWS S3 bucket.
        log_level: Python log level.
    """
    from dmpworks.batch import datacite
    from dmpworks.utils import setup_multiprocessing_logging

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    datacite.download(
        bucket_name=bucket_name,
        run_id=run_id,
        datacite_bucket_name=datacite_bucket_name,
    )


@datacite_app.command(name="dataset-subset")
def datacite_dataset_subset_cmd(
    bucket_name: str,
    run_id: str,
    dataset_subset: DatasetSubsetAWS,
    prev_job_run_id: Annotated[
        str | None,
        Parameter(env_var="PREV_JOB_RUN_ID", help="Run ID of the prior download job."),
    ] = None,
    log_level: LogLevel = "INFO",
):
    """Create a subset of DataCite.

    Args:
        bucket_name: the name of the S3 bucket for JOB I/O.
        run_id: a unique ID to represent this run of the job.
        dataset_subset: settings for creating the subset of works
        prev_job_run_id: run ID of the prior download job to read source data from.
        log_level: Python log level.
    """
    from dmpworks.batch import datacite
    from dmpworks.utils import setup_multiprocessing_logging

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    datacite.dataset_subset(
        bucket_name=bucket_name,
        run_id=run_id,
        ds_config=dataset_subset,
        prev_run_id=prev_job_run_id,
    )


@datacite_app.command(name="transform")
def datacite_transform_cmd(
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    *,
    config: DataCiteTransformConfig | None = None,
    prev_job_run_id: Annotated[
        str | None,
        Parameter(env_var="PREV_JOB_RUN_ID", help="Run ID of the prior download job."),
    ] = None,
    log_level: LogLevel = "INFO",
):
    """Download DataCite from DMP Tool S3 bucket, transform to Parquet, and upload the result.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: optional configuration parameters.
        prev_job_run_id: run ID of the prior download job to read source data from.
        log_level: Python log level.
    """
    from dmpworks.batch import datacite
    from dmpworks.utils import setup_multiprocessing_logging

    config = DataCiteTransformConfig() if config is None else config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)
    datacite.transform(
        bucket_name=bucket_name,
        run_id=run_id,
        config=config,
        use_subset=use_subset,
        source_run_id=prev_job_run_id,
        log_level=level,
    )


# OpenAlex Works
openalex_works_app = App(name="openalex-works", help="OpenAlex Works AWS Batch pipeline.")
app.command(openalex_works_app)


@openalex_works_app.command(name="download")
def openalex_works_download_cmd(
    bucket_name: str,
    run_id: str,
    openalex_bucket_name: str,
    log_level: LogLevel = "INFO",
):
    """Download OpenAlex Works from the OpenAlex S3 bucket and upload to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        openalex_bucket_name: the name of the OpenAlex AWS S3 bucket.
        log_level: Python log level.
    """
    from dmpworks.batch import openalex_works
    from dmpworks.utils import setup_multiprocessing_logging

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    openalex_works.download(
        bucket_name=bucket_name,
        run_id=run_id,
        openalex_bucket_name=openalex_bucket_name,
    )


@openalex_works_app.command(name="dataset-subset")
def openalex_works_dataset_subset_cmd(
    bucket_name: str,
    run_id: str,
    dataset_subset: DatasetSubsetAWS = None,
    prev_job_run_id: Annotated[
        str | None,
        Parameter(env_var="PREV_JOB_RUN_ID", help="Run ID of the prior download job."),
    ] = None,
    log_level: LogLevel = "INFO",
):
    """Create a subset of OpenAlex Works.

    Args:
        bucket_name: the name of the S3 bucket for JOB I/O.
        run_id: a unique ID to represent this run of the job.
        dataset_subset: settings for creating the subset of works
        prev_job_run_id: run ID of the prior download job to read source data from.
        log_level: Python log level.
    """
    from dmpworks.batch import openalex_works
    from dmpworks.utils import setup_multiprocessing_logging

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    openalex_works.dataset_subset(
        bucket_name=bucket_name,
        run_id=run_id,
        ds_config=dataset_subset,
        prev_run_id=prev_job_run_id,
    )


@openalex_works_app.command(name="transform")
def openalex_works_transform_cmd(
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    *,
    config: OpenAlexWorksTransformConfig | None = None,
    prev_job_run_id: Annotated[
        str | None,
        Parameter(env_var="PREV_JOB_RUN_ID", help="Run ID of the prior download job."),
    ] = None,
    log_level: LogLevel = "INFO",
):
    """Download OpenAlex Works from DMP Tool S3 bucket, transform to Parquet, and upload the result.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: optional configuration parameters.
        prev_job_run_id: run ID of the prior download job to read source data from.
        log_level: Python log level.
    """
    from dmpworks.batch import openalex_works
    from dmpworks.utils import setup_multiprocessing_logging

    config = OpenAlexWorksTransformConfig() if config is None else config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)
    openalex_works.transform(
        bucket_name=bucket_name,
        run_id=run_id,
        config=config,
        use_subset=use_subset,
        source_run_id=prev_job_run_id,
        log_level=level,
    )


@crossref_metadata_app.command(name="download")
def crossref_metadata_download_cmd(
    bucket_name: str,
    run_id: str,
    file_name: str,
    crossref_bucket_name: str,
    log_level: LogLevel = "INFO",
):
    """Download Crossref Metadata from the Crossref Metadata requestor pays S3 bucket and upload to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: Unique ID to represent this run of the job.
        file_name: Name of the Crossref Metadata Public Datafile, e.g. April_2025_Public_Data_File_from_Crossref.tar.
        crossref_bucket_name: Name of the Crossref AWS S3 bucket.
        log_level: Python log level.
    """
    from dmpworks.batch import crossref_metadata
    from dmpworks.utils import setup_multiprocessing_logging

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    crossref_metadata.download(
        bucket_name=bucket_name,
        run_id=run_id,
        file_name=file_name,
        crossref_bucket_name=crossref_bucket_name,
    )


@crossref_metadata_app.command(name="dataset-subset")
def crossref_metadata_dataset_subset_cmd(
    bucket_name: str,
    run_id: str,
    dataset_subset: DatasetSubsetAWS = None,
    prev_job_run_id: Annotated[
        str | None,
        Parameter(env_var="PREV_JOB_RUN_ID", help="Run ID of the prior download job."),
    ] = None,
    log_level: LogLevel = "INFO",
):
    """Create a subset of Crossref Metadata.

    Args:
        bucket_name: the name of the S3 bucket for JOB I/O.
        run_id: a unique ID to represent this run of the job.
        dataset_subset: settings for creating the subset of works
        prev_job_run_id: run ID of the prior download job to read source data from.
        log_level: Python log level.
    """
    from dmpworks.batch import crossref_metadata
    from dmpworks.utils import setup_multiprocessing_logging

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    crossref_metadata.dataset_subset(
        bucket_name=bucket_name,
        run_id=run_id,
        ds_config=dataset_subset,
        prev_run_id=prev_job_run_id,
    )


@crossref_metadata_app.command(name="transform")
def crossref_metadata_transform_cmd(
    bucket_name: str,
    run_id: str,
    use_subset: bool = False,
    *,
    config: CrossrefMetadataTransformConfig | None = None,
    prev_job_run_id: Annotated[
        str | None,
        Parameter(env_var="PREV_JOB_RUN_ID", help="Run ID of the prior download job."),
    ] = None,
    log_level: LogLevel = "INFO",
):
    """Download Crossref Metadata from DMP Tool S3 bucket, transform to Parquet, and upload the result.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        use_subset: whether to use a subset of the dataset or the full dataset.
        config: optional configuration parameters.
        prev_job_run_id: run ID of the prior download job to read source data from.
        log_level: Python log level.
    """
    from dmpworks.batch import crossref_metadata
    from dmpworks.utils import setup_multiprocessing_logging

    config = CrossrefMetadataTransformConfig() if config is None else config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)
    crossref_metadata.transform(
        bucket_name=bucket_name,
        run_id=run_id,
        config=config,
        use_subset=use_subset,
        source_run_id=prev_job_run_id,
        log_level=level,
    )


@ror_app.command(name="download")
def ror_download_cmd(
    bucket_name: str,
    run_id: str,
    download_url: str,
    file_hash: str | None = None,
    log_level: LogLevel = "INFO",
):
    """Download ROR from the Zenodo and upload it to the DMP Tool S3 bucket.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        download_url: the Zenodo download URL for a specific ROR ID, e.g. https://zenodo.org/records/15731450/files/v1.67-2025-06-24-ror-data.zip?download=1.
        file_hash: the MD5 sum of the file.
        log_level: Python log level.
    """
    from dmpworks.batch import ror
    from dmpworks.utils import setup_multiprocessing_logging

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    ror.download(bucket_name=bucket_name, run_id=run_id, download_url=download_url, file_hash=file_hash)


@sqlmesh_app.command(name="plan")
def sqlmesh_plan_cmd(
    bucket_name: str,
    run_identifiers: RunIdentifiers,
    sqlmesh_config: SQLMeshConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Run the SQLMesh plan.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_identifiers: the release dates of each dataset.
        sqlmesh_config: SQLMesh configuration.
        log_level: Python log level.
    """
    from dmpworks.batch import sql
    from dmpworks.utils import setup_multiprocessing_logging

    sqlmesh_config = SQLMeshConfig() if sqlmesh_config is None else sqlmesh_config
    setup_multiprocessing_logging(logging.getLevelName(log_level))
    sql.plan(
        bucket_name=bucket_name,
        run_identifiers=run_identifiers,
        sqlmesh_config=sqlmesh_config,
    )


@opensearch_app.command(name="sync-works")
def opensearch_sync_works_cmd(
    bucket_name: str,
    index_name: str,
    run_identifiers: RunIdentifiers,
    client_config: OpenSearchClientConfig | None = None,
    sync_config: OpenSearchSyncConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Sync exported works in Parquet format with OpenSearch.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_identifiers: run identifiers including sqlmesh run ID.
        index_name: the OpenSearch index name.
        client_config: the OpenSearch client config settings.
        sync_config: the OpenSearch sync config settings.
        log_level: Python log level.
    """
    from dmpworks.batch import opensearch
    from dmpworks.utils import setup_multiprocessing_logging

    client_config = OpenSearchClientConfig() if client_config is None else client_config
    sync_config = OpenSearchSyncConfig() if sync_config is None else sync_config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)
    opensearch.sync_works_cmd(
        bucket_name=bucket_name,
        run_id=run_identifiers.run_id_sqlmesh,
        sqlmesh_run_id=run_identifiers.run_id_sqlmesh,
        index_name=index_name,
        client_config=client_config,
        sync_config=sync_config,
        log_level=log_level,
    )


@opensearch_app.command(name="sync-dmps")
def opensearch_sync_dmps_cmd(
    bucket_name: str,
    index_name: str,
    mysql_config: MySQLConfig,
    client_config: OpenSearchClientConfig | None = None,
    dmp_subset: DMPSubsetAWS | None = None,
    log_level: LogLevel = "INFO",
):
    """Sync DMPs from MySQL with OpenSearch DMPs index.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        index_name: the OpenSearch DMPs index name.
        mysql_config: MySQL connection configuration.
        client_config: the OpenSearch client config settings.
        dmp_subset: settings for including a subset of DMPs.
        log_level: Python log level.
    """
    from dmpworks.batch import opensearch

    client_config = OpenSearchClientConfig() if client_config is None else client_config
    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    opensearch.sync_dmps_cmd(
        bucket_name=bucket_name,
        index_name=index_name,
        client_config=client_config,
        mysql_config=mysql_config,
        dmp_subset=dmp_subset,
    )


@opensearch_app.command(name="enrich-dmps")
def opensearch_enrich_dmps_cmd(
    index_name: str,
    client_config: OpenSearchClientConfig | None = None,
    bucket_name: str | None = None,
    dmp_subset: DMPSubsetAWS | None = None,
    log_level: LogLevel = "INFO",
):
    """Enrich dmps in the OpenSearch DMPs index.

    Args:
        index_name: the OpenSearch index name.
        client_config: the OpenSearch client config settings.
        bucket_name: DMP Tool S3 bucket name (required when dmp_subset is provided).
        dmp_subset: settings for including a subset of DMPs.
        log_level: Python log level.
    """
    from dmpworks.batch import opensearch

    client_config = OpenSearchClientConfig() if client_config is None else client_config
    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    opensearch.enrich_dmps_cmd(
        index_name=index_name,
        client_config=client_config,
        bucket_name=bucket_name,
        dmp_subset=dmp_subset,
    )


@opensearch_app.command(name="dmp-works-search")
def opensearch_dmp_works_search_cmd(
    bucket_name: str,
    run_id: str,
    dmps_index_name: str,
    works_index_name: str,
    client_config: OpenSearchClientConfig | None = None,
    dmp_subset: DMPSubsetAWS | None = None,
    dmp_works_search_config: DMPWorksSearchConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Run the DMP Works Search process to find matching works for DMPs.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this run of the job.
        dmps_index_name: the name of the DMP index in OpenSearch.
        works_index_name: the name of the works index in OpenSearch.
        client_config: OpenSearch client settings.
        dmp_subset: settings for including a subset of DMPs.
        dmp_works_search_config: DMP works search settings.
        log_level: Python log level.
    """
    from dmpworks.batch import opensearch

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client_config = OpenSearchClientConfig() if client_config is None else client_config
    opensearch.dmp_works_search_cmd(
        bucket_name=bucket_name,
        run_id=run_id,
        dmps_index_name=dmps_index_name,
        works_index_name=works_index_name,
        client_config=client_config,
        dmp_subset=dmp_subset,
        dmp_works_search_config=dmp_works_search_config,
    )


@opensearch_app.command(name="merge-related-works")
def opensearch_merge_related_works_cmd(
    bucket_name: str,
    run_id: str,
    search_run_id: str,
    mysql_config: MySQLConfig,
    batch_size: int = 1000,
    log_level: LogLevel = "INFO",
):
    """Merge related works from S3 into the MySQL database.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_id: a unique ID to represent this merge run.
        search_run_id: the run ID of the dmp-works-search job whose output to read.
        mysql_config: MySQL connection configuration.
        batch_size: Number of records to process in a batch.
        log_level: Python log level.
    """
    from dmpworks.batch import opensearch

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    opensearch.merge_related_works_cmd(
        bucket_name=bucket_name,
        run_id=run_id,
        search_run_id=search_run_id,
        mysql_config=mysql_config,
        batch_size=batch_size,
    )


if __name__ == "__main__":
    app()
