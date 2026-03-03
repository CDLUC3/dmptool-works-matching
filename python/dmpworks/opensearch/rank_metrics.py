from collections import defaultdict
from collections.abc import Iterable
import csv
import itertools
import logging
import pathlib
from typing import Any

from ranx import Qrels, Run, evaluate

from dmpworks.model.related_work_model import RelatedWork
from dmpworks.opensearch.dmp_search import fetch_dmps
from dmpworks.opensearch.dmp_works_search import search_dmp_works
from dmpworks.opensearch.query_builder import get_query_builder
from dmpworks.opensearch.utils import OpenSearchClientConfig, QueryBuilder, make_opensearch_client


def load_qrels_dict(file_path: pathlib.Path) -> dict:
    """Load query relevance judgments dict.

    Args:
        file_path: The path to the qrels file.

    Returns:
        dict: A dictionary of query relevance judgments.
    """
    qrels_dict = defaultdict(dict)
    with file_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = row["status"]
            if status == "ACCEPTED":
                dmp_doi = row["dmpDoi"]
                work_doi = row["workDoi"]
                qrels_dict[dmp_doi][work_doi] = 1

    return qrels_dict


def load_run_dict(related_works: Iterable[RelatedWork], dmp_dois: set[str] | None = None) -> dict:
    """Load the Run dict which stores the relevance scores estimated by the model under evaluation.

    Args:
        related_works: An iterable of related works.
        dmp_dois: A set of DMP DOIs to filter by.

    Returns:
        dict: A dictionary of run scores.
    """
    run_dict = defaultdict(dict)
    for related_work in related_works:
        if dmp_dois is None or related_work.dmp_doi in dmp_dois:
            run_dict[related_work.dmp_doi][related_work.work.doi] = related_work.score / related_work.score_max

    return run_dict


def get_dmp_dois(qrels_dict: dict) -> set[str]:
    """Get the set of DMP DOIs from the qrels dictionary.

    Args:
        qrels_dict: The qrels dictionary.

    Returns:
        set[str]: A set of DMP DOIs.
    """
    return set(qrels_dict.keys())


def related_works_calculate_metrics(
    ground_truth_file: pathlib.Path,
    dmps_index_name: str,
    works_index_name: str,
    output_file: pathlib.Path,
    client_config: OpenSearchClientConfig,
    query_builder_name: QueryBuilder = "build_dmp_works_search_baseline_query",
    rerank_model_name: str | None = None,
    scroll_time: str = "360m",
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    include_named_queries_score: bool = True,
    inner_hits_size: int = 50,
    ks: list[int] | None = None,
):
    """Calculate rank metrics for related works search.

    Args:
        ground_truth_file: The path to the ground truth file.
        dmps_index_name: The name of the DMPs index.
        works_index_name: The name of the works index.
        output_file: The path to the output file.
        client_config: The OpenSearch client configuration.
        query_builder_name: The name of the query builder to use.
        rerank_model_name: The name of the rerank model to use.
        scroll_time: The scroll time for the search context.
        batch_size: The number of DMPs to process per batch.
        max_results: The maximum number of results to return per DMP.
        project_end_buffer_years: The number of years to buffer the project end date.
        include_named_queries_score: Whether to include named queries scores.
        inner_hits_size: The size of inner hits to return for nested fields.
        ks: A list of k values for metrics calculation (e.g., [10, 20, 100]).
    """
    logging.info("Computing rank metrics...")
    client = make_opensearch_client(client_config)
    related_works_all = []
    dmps_metrics = []
    ks = [10, 20, 100] if ks is None else ks
    fieldnames = [
        "dmp_doi",
        "dmp_title",
        "n_outputs",
        *itertools.chain.from_iterable([[f"map@{k}", f"ndcg@{k}", f"precision@{k}", f"recall@{k}"] for k in ks]),
    ]

    # Load ground truth file and get DMP DOIs
    qrels_dict_all = load_qrels_dict(ground_truth_file)
    dmp_dois = list(get_dmp_dois(qrels_dict_all))
    query_builder = get_query_builder(query_builder_name)

    with fetch_dmps(
        client=client,
        dmps_index_name=dmps_index_name,
        scroll_time=scroll_time,
        page_size=batch_size,
        dois=dmp_dois,
        inner_hits_size=inner_hits_size,
    ) as results:
        for dmp in results.dmps:
            logging.info(f"DMP: {dmp.doi}")
            # Candidate search
            related_works = search_dmp_works(
                client,
                works_index_name,
                dmp,
                query_builder,
                rerank_model_name=rerank_model_name,
                max_results=max_results,
                project_end_buffer_years=project_end_buffer_years,
                include_named_queries_score=include_named_queries_score,
                inner_hits_size=inner_hits_size,
            )
            related_works_all += related_works

            # Calculate metrics
            qrels_dmp = Qrels.from_dict({dmp.doi: qrels_dict_all[dmp.doi]})
            run_dmp = Run.from_dict(load_run_dict(related_works))
            row = {"dmp_doi": dmp.doi, "dmp_title": dmp.title, "n_outputs": len(qrels_dict_all[dmp.doi])}
            for k in ks:
                row.update(evaluate(qrels_dmp, run_dmp, [f"map@{k}", f"ndcg@{k}", f"precision@{k}", f"recall@{k}"]))
            dmps_metrics.append(row)

    # Calculate global metrics
    qrels_all = Qrels.from_dict(qrels_dict_all)
    run_all = Run.from_dict(load_run_dict(related_works_all))
    metrics_all: dict[str, Any] = {"dmp_doi": "all"}
    for k in ks:
        metrics_all.update(evaluate(qrels_all, run_all, [f"map@{k}", f"ndcg@{k}", f"precision@{k}", f"recall@{k}"]))

    # Save metrics
    logging.info(f"Saving metrics to: {output_file}")
    with output_file.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(metrics_all)
        writer.writerows(dmps_metrics)
