import logging
import pathlib
from typing import Annotated

from cyclopts import App, Parameter, validators

from dmpworks.cli_utils import (
    Directory,
    DMPSubsetLocal,
    DMPWorksSearchConfig,
    LogLevel,
    MySQLConfig,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
    QueryBuilder,
    QueryFeature,
)
from dmpworks.opensearch.cli_roles import app as roles_app

app = App(name="opensearch", help="OpenSearch utilities.")
app.command(roles_app)


def load_dmp_subset_local(
    dmp_subset: DMPSubsetLocal | None,
) -> tuple[list | None, list[str] | None]:
    """Load institutions and DOIs from local files based on DMPSubsetLocal config.

    Args:
        dmp_subset: Local DMP subset configuration.

    Returns:
        tuple[list | None, list[str] | None]: Loaded institutions and DOIs, or None if not configured.
    """
    from dmpworks.dataset_subset import load_dois, load_institutions

    use_subset = dmp_subset is not None and dmp_subset.enable
    institutions = None
    dois = None
    if use_subset and dmp_subset.institutions_path is not None:
        institutions = load_institutions(dmp_subset.institutions_path)
    if use_subset and dmp_subset.dois_path is not None:
        dois = load_dois(dmp_subset.dois_path)
    return institutions, dois


@app.command(name="create-index")
def create_index_cmd(
    index_name: str,
    mapping_filename: str,
    client_config: OpenSearchClientConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Create an OpenSearch index.

    Args:
        index_name: Name of the OpenSearch index to create (e.g., works).
        mapping_filename: Name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json).
        client_config: OpenSearch client settings.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.index import create_index
    from dmpworks.opensearch.utils import make_opensearch_client

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client = make_opensearch_client(client_config)

    create_index(client, index_name, mapping_filename)


@app.command(name="update-mapping")
def update_mapping_cmd(
    index_name: str,
    mapping_filename: str,
    client_config: OpenSearchClientConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Update an OpenSearch index mapping.

    Args:
        index_name: Name of the OpenSearch index to update (e.g., works).
        mapping_filename: Name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json).
        client_config: OpenSearch client settings.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.index import update_mapping
    from dmpworks.opensearch.utils import make_opensearch_client

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client = make_opensearch_client(client_config)
    update_mapping(client, index_name, mapping_filename)


@app.command(name="sync-works")
def sync_works_cmd(
    index_name: str,
    works_index_export: Directory,
    doi_state_export: Directory,
    run_id: str,
    client_config: OpenSearchClientConfig | None = None,
    sync_config: OpenSearchSyncConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Sync the DMP Tool Works Index Table with OpenSearch.

    Args:
        index_name: Name of the OpenSearch index to sync to (e.g., works).
        works_index_export: Path to the DMP Tool Works index table export directory (e.g., /path/to/works_index_export).
        doi_state_export: Path to the DOI state export directory (e.g., /path/to/doi_state_export).
        run_id: The run date when the new works were generated.
        client_config: OpenSearch client settings.
        sync_config: OpenSearch sync settings.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.sync_works import sync_works

    if client_config is None:
        client_config = OpenSearchClientConfig()

    if sync_config is None:
        sync_config = OpenSearchSyncConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    sync_works(
        index_name=index_name,
        works_index_export=works_index_export,
        doi_state_export=doi_state_export,
        run_id=run_id,
        client_config=client_config,
        sync_config=sync_config,
        log_level=level,
    )


@app.command(name="sync-dmps")
def sync_dmps_cmd(
    index_name: str,
    mysql_config: MySQLConfig,
    opensearch_config: OpenSearchClientConfig | None = None,
    chunk_size: int = 1000,
    dmp_subset: DMPSubsetLocal | None = None,
    log_level: LogLevel = "INFO",
):
    """Sync DMPs from MySQL with OpenSearch DMPs index.

    Args:
        index_name: Name of the OpenSearch index to sync to (e.g., dmps).
        mysql_config: MySQL config.
        opensearch_config: OpenSearch client settings.
        chunk_size: OpenSearch bulk indexing chunk size.
        dmp_subset: Settings for including a subset of DMPs.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.sync_dmps import sync_dmps

    if opensearch_config is None:
        opensearch_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    institutions, dois = load_dmp_subset_local(dmp_subset)
    sync_dmps(
        index_name,
        mysql_config,
        opensearch_config=opensearch_config,
        chunk_size=chunk_size,
        institutions=institutions,
        dois=dois,
    )


@app.command(name="enrich-dmps")
def enrich_dmps_cmd(
    dmps_index_name: str,
    client_config: OpenSearchClientConfig | None = None,
    dmp_subset: DMPSubsetLocal | None = None,
    log_level: LogLevel = "INFO",
):
    """Enrich DMPs in OpenSearch with publications found on funder award pages.

    Args:
        dmps_index_name: Name of the DMP index to update.
        client_config: OpenSearch client settings.
        dmp_subset: Settings for including a subset of DMPs.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.enrich_dmps import enrich_dmps

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    institutions, dois = load_dmp_subset_local(dmp_subset)
    enrich_dmps(
        dmps_index_name,
        client_config,
        institutions=institutions,
        dois=dois,
    )


@app.command(name="dmp-works-search")
def dmp_works_search_cmd(
    dmps_index_name: str,
    works_index_name: str,
    out_dir: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=True,
                file_okay=False,
                exists=False,
            )
        ),
    ],
    client_config: OpenSearchClientConfig | None = None,
    dmp_subset: DMPSubsetLocal | None = None,
    search_config: DMPWorksSearchConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Run the DMP works search, returning candidate matches for each DMP.

    Args:
        dmps_index_name: Name of the DMP index in OpenSearch.
        works_index_name: Name of the works index in OpenSearch.
        out_dir: The output directory where search result .jsonl.gz files will be written.
        client_config: OpenSearch client settings.
        dmp_subset: Settings for including a subset of DMPs.
        search_config: DMP works search settings.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.dmp_works_search import dmp_works_search

    if client_config is None:
        client_config = OpenSearchClientConfig()
    if search_config is None:
        search_config = DMPWorksSearchConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    institutions, dois = load_dmp_subset_local(dmp_subset)

    out_dir.mkdir(parents=True, exist_ok=True)
    dmp_works_search(
        dmps_index_name,
        works_index_name,
        out_dir,
        client_config,
        query_builder_name=search_config.query_builder_name,
        rerank_model_name=search_config.rerank_model_name,
        scroll_time=search_config.scroll_time,
        batch_size=search_config.batch_size,
        max_results=search_config.max_results,
        project_end_buffer_years=search_config.project_end_buffer_years,
        parallel_search=search_config.parallel_search,
        include_named_queries_score=search_config.include_named_queries_score,
        max_concurrent_searches=search_config.max_concurrent_searches,
        max_concurrent_shard_requests=search_config.max_concurrent_shard_requests,
        institutions=institutions,
        dois=dois,
        dmps_start_date=search_config.dmps_start_date,
        dmps_end_date=search_config.dmps_end_date,
        dmp_modification_window_days=(
            search_config.dmp_modification_window_days if search_config.apply_modification_window else None
        ),
        inner_hits_size=search_config.inner_hits_size,
        records_per_file=search_config.records_per_file,
    )


@app.command(name="rank-metrics")
def rank_metrics_cmd(
    ground_truth_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            )
        ),
    ],
    dmps_index_name: str,
    works_index_name: str,
    output_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=False,
            )
        ),
    ],
    query_builder_name: QueryBuilder = "build_dmp_works_search_baseline_query",
    rerank_model_name: str | None = None,
    client_config: OpenSearchClientConfig | None = None,
    scroll_time: str = "360m",
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    include_named_queries_score: bool = True,
    inner_hits_size: int = 50,
    ks: Annotated[list[int] | None, Parameter(consume_multiple=True)] = None,
    inject_published_outputs_file: Annotated[
        pathlib.Path | None,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            )
        ),
    ] = None,
    true_positive_published_outputs_file: Annotated[
        pathlib.Path | None,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=False,
            )
        ),
    ] = None,
    disable_features: Annotated[list[QueryFeature] | None, Parameter(consume_multiple=True)] = None,
    log_level: LogLevel = "INFO",
):
    """Compute ranking metrics for baseline or re-ranked search results.

    Args:
        ground_truth_file: Path to the ground truth CSV file containing DMP to work judgement labels.
        dmps_index_name: The DMPs index name.
        works_index_name: The works index name.
        output_file: Path to the file where the computed metrics will be saved.
        query_builder_name: Name of the baseline query to use.
        rerank_model_name: Name of the model to use for re-ranking. Omit to test baseline search.
        client_config: OpenSearch client settings.
        scroll_time: Length of time the OpenSearch scroll context remains active while iterating over DMPs.
        batch_size: Number of DMPs processed per batch when executing searches.
        max_results: The maximum number of works to return for each DMP.
        project_end_buffer_years: Number of years added to the project end date when searching for works.
        include_named_queries_score: Whether to include named query scores in the search response.
        inner_hits_size: Maximum number of inner hits returned for each matched work.
        ks: The top K breakpoints to compute for each metric.
        inject_published_outputs_file: Path to a CSV (dmp_doi,work_doi columns) defining the published_outputs used during search. When set, this file is the full anchor spec: DMPs listed get injected anchors, DMPs not listed get an empty anchor set. Used to drive the DMP relations feature from a controlled anchor set during benchmarking.
        true_positive_published_outputs_file: When set, write a CSV of dmp_doi,work_doi pairs for returned works that are also in the ground truth. The output can be fed back in via inject_published_outputs_file on a subsequent run.
        disable_features: Features to disable in the baseline query for ablation studies. All features are enabled by default. Valid values: funded_dois, authors, institutions, funders, awards, content, relations.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.query_builder import QueryFeatures
    from dmpworks.opensearch.rank_metrics import related_works_calculate_metrics

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    features = QueryFeatures(**dict.fromkeys(disable_features, False)) if disable_features else QueryFeatures()

    related_works_calculate_metrics(
        ground_truth_file,
        dmps_index_name,
        works_index_name,
        output_file,
        client_config,
        query_builder_name=query_builder_name,
        rerank_model_name=rerank_model_name,
        scroll_time=scroll_time,
        project_end_buffer_years=project_end_buffer_years,
        include_named_queries_score=include_named_queries_score,
        inner_hits_size=inner_hits_size,
        batch_size=batch_size,
        max_results=max_results,
        ks=ks,
        inject_published_outputs_file=inject_published_outputs_file,
        true_positive_published_outputs_file=true_positive_published_outputs_file,
        features=features,
    )


@app.command(name="create-featureset")
def create_featureset_cmd(
    featureset_name: str,
    client_config: OpenSearchClientConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Create an OpenSearch Learning to Rank feature set.

    Args:
        featureset_name: The OpenSearch LTR feature set name.
        client_config: OpenSearch client settings.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.learning_to_rank import create_featureset
    from dmpworks.opensearch.utils import make_opensearch_client

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    client = make_opensearch_client(client_config)
    create_featureset(client, featureset_name)


@app.command(name="upload-ranklib-model")
def upload_ranklib_model_cmd(
    featureset_name: str,
    model_name: str,
    ranklib_model_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            )
        ),
    ],
    ranklib_features_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            )
        ),
    ],
    training_dataset_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            )
        ),
    ],
    client_config: OpenSearchClientConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Upload a RankLib model to OpenSearch.

    Args:
        featureset_name: Name of the featureset.
        model_name: The name to give the model.
        ranklib_model_file: Path to the RankLib model file to upload.
        ranklib_features_file: Path to the RankLib features file to determine what features to add normalisers for.
        training_dataset_file: Path to the training dataset used to train the RankLib model. Used to calculate the mean and standard deviation to supply normalisation data.
        client_config: The OpenSearch client config.
        log_level: The Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.learning_to_rank import upload_ranklib_model
    from dmpworks.opensearch.utils import make_opensearch_client

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    client = make_opensearch_client(client_config)
    upload_ranklib_model(
        client,
        featureset_name,
        model_name,
        ranklib_model_file,
        ranklib_features_file,
        training_dataset_file,
    )


@app.command(name="generate-training-dataset")
def generate_training_dataset_cmd(
    ground_truth_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            )
        ),
    ],
    dmps_index_name: str,
    works_index_name: str,
    output_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=False,
            )
        ),
    ],
    featureset_name: str,
    query_builder_name: QueryBuilder = "build_dmp_works_search_baseline_query",
    client_config: OpenSearchClientConfig | None = None,
    scroll_time: str = "360m",
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    include_named_queries_score: bool = True,
    inner_hits_size: int = 50,
    log_level: LogLevel = "INFO",
):
    """Generate a RankLib training dataset from search results and ground truth.

    Uses the baseline DMP works search, a featureset and a ground truth file.

    Args:
        ground_truth_file: Path to the ground truth data file
        dmps_index_name: The OpenSearch DMP index name.
        works_index_name: The OpenSearch works index name.
        output_file: Path to the output file where the generated RankLib training dataset will be written.
        featureset_name: The featureset name.
        query_builder_name: Name of the query builder to use.
        client_config: OpenSearch client settings.
        scroll_time: Length of time the OpenSearch scroll context remains active while iterating over DMPs.
        batch_size: Number of DMPs processed per batch when executing searches.
        max_results: The maximum number of works to include for each DMP.
        project_end_buffer_years: Number of years added to the project end date when searching for works.
        include_named_queries_score: Whether to include named query scores in the search response.
        inner_hits_size: Maximum number of inner hits returned for each matched work.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.learning_to_rank import generate_training_dataset

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    generate_training_dataset(
        ground_truth_file,
        dmps_index_name,
        works_index_name,
        featureset_name,
        output_file,
        client_config,
        query_builder_name=query_builder_name,
        scroll_time=scroll_time,
        project_end_buffer_years=project_end_buffer_years,
        include_named_queries_score=include_named_queries_score,
        inner_hits_size=inner_hits_size,
        batch_size=batch_size,
        max_results=max_results,
    )


if __name__ == "__main__":
    app()
