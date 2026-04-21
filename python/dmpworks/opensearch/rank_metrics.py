from collections import defaultdict
from collections.abc import Iterable
import csv
import functools
import itertools
import logging
import pathlib
from typing import Any

from ranx import Qrels, Run, evaluate

from dmpworks.cli_utils import OpenSearchClientConfig, QueryBuilder
from dmpworks.model.dmp_model import ResearchOutput
from dmpworks.model.related_work_model import RelatedWork
from dmpworks.opensearch.dmp_search import fetch_dmps
from dmpworks.opensearch.dmp_works_search import search_dmp_works
from dmpworks.opensearch.query_builder import QueryFeatures, get_query_builder
from dmpworks.opensearch.utils import make_opensearch_client

DMP_DOI_COLUMN = "dmp_doi"
WORK_DOI_COLUMN = "work_doi"


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
                dmp_doi = row[DMP_DOI_COLUMN]
                work_doi = row[WORK_DOI_COLUMN]
                qrels_dict[dmp_doi][work_doi] = 1

    return qrels_dict


def load_published_outputs_file(file_path: pathlib.Path) -> dict[str, list[str]]:
    """Load a CSV of (dmp_doi, work_doi) pairs into a per-DMP list of work DOIs.

    Args:
        file_path: The path to the CSV file with ``dmp_doi,work_doi`` columns.

    Returns:
        dict[str, list[str]]: A dictionary mapping each DMP DOI to the list of associated work DOIs.
    """
    outputs: dict[str, list[str]] = defaultdict(list)
    with file_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            dmp_doi = row[DMP_DOI_COLUMN]
            work_doi = row[WORK_DOI_COLUMN]
            outputs[dmp_doi].append(work_doi)
    return outputs


def save_published_outputs_file(file_path: pathlib.Path, pairs: list[tuple[str, str]]) -> None:
    """Save a list of (dmp_doi, work_doi) pairs as a CSV file.

    Args:
        file_path: The destination path for the CSV file.
        pairs: A list of (dmp_doi, work_doi) tuples to write.
    """
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[DMP_DOI_COLUMN, WORK_DOI_COLUMN])
        writer.writeheader()
        for dmp_doi, work_doi in pairs:
            writer.writerow({DMP_DOI_COLUMN: dmp_doi, WORK_DOI_COLUMN: work_doi})


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
    inject_published_outputs_file: pathlib.Path | None = None,
    true_positive_published_outputs_file: pathlib.Path | None = None,
    features: QueryFeatures | None = None,
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
        inject_published_outputs_file: Path to a CSV (``dmp_doi,work_doi`` columns) whose rows
            define the published_outputs used during search. When set, this file is the full
            anchor spec: DMPs listed in it get their injected anchors, DMPs not listed get an
            empty anchor set. Used to drive the DMP relations feature from a controlled anchor
            set during benchmarking.
        true_positive_published_outputs_file: When set, write a CSV of ``dmp_doi,work_doi`` pairs
            for each returned work that is also in the ground truth. The output file format matches
            ``inject_published_outputs_file`` and can be fed back in on a subsequent run.
        features: Per-feature toggles for the baseline query. Defaults to all-on.
    """
    logging.info("Computing rank metrics...")
    features = features if features is not None else QueryFeatures()
    if features.disabled_names():
        logging.info(f"Feature ablation — disabled: {features.disabled_names()}")
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
    query_builder = functools.partial(get_query_builder(query_builder_name), features=features)

    # Load injected published outputs (if provided) and prepare TP collection
    inject_map: dict[str, list[str]] | None = None
    if inject_published_outputs_file is not None:
        inject_map = load_published_outputs_file(inject_published_outputs_file)
        logging.info(
            f"Loaded injected published outputs for {len(inject_map)} DMPs from {inject_published_outputs_file}"
        )
    true_positive_pairs: list[tuple[str, str]] = []

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

            # Inject file = full anchor spec: listed DMPs use injected anchors,
            # un-listed DMPs are cleared so the relations feature has nothing to match.
            if inject_map is not None:
                dmp.published_outputs = [ResearchOutput(doi=d) for d in inject_map.get(dmp.doi, [])]

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

            # Collect true positives for optional export
            if true_positive_published_outputs_file is not None:
                qrel_work_dois = qrels_dict_all.get(dmp.doi, {})
                true_positive_pairs.extend(
                    (dmp.doi, rw.work.doi) for rw in related_works if rw.work.doi in qrel_work_dois
                )

            # Calculate metrics
            qrels_dmp = Qrels.from_dict({dmp.doi: qrels_dict_all[dmp.doi]})
            run_dmp = Run.from_dict(load_run_dict(related_works))
            row = {"dmp_doi": dmp.doi, "dmp_title": dmp.title, "n_outputs": len(qrels_dict_all[dmp.doi])}
            for k in ks:
                row.update(
                    evaluate(
                        qrels_dmp,
                        run_dmp,
                        [f"map@{k}", f"ndcg@{k}", f"precision@{k}", f"recall@{k}"],
                        make_comparable=True,
                    )
                )
            dmps_metrics.append(row)

    # Calculate global metrics
    qrels_all = Qrels.from_dict(qrels_dict_all)
    run_all = Run.from_dict(load_run_dict(related_works_all))
    missing_from_run = set(qrels_all.keys()) - set(run_all.keys())
    if missing_from_run:
        logging.warning(
            f"{len(missing_from_run)} DMP(s) in the ground truth had no returned works "
            f"(recall=0 for these): {sorted(missing_from_run)}"
        )
    metrics_all: dict[str, Any] = {"dmp_doi": "all"}
    for k in ks:
        metrics_all.update(
            evaluate(
                qrels_all,
                run_all,
                [f"map@{k}", f"ndcg@{k}", f"precision@{k}", f"recall@{k}"],
                make_comparable=True,
            )
        )

    # Save metrics
    logging.info(f"Saving metrics to: {output_file}")
    with output_file.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(metrics_all)
        writer.writerows(dmps_metrics)

    # Save true positive published outputs
    if true_positive_published_outputs_file is not None:
        logging.info(
            f"Saving {len(true_positive_pairs)} true positive (dmp_doi, work_doi) pairs to: "
            f"{true_positive_published_outputs_file}"
        )
        save_published_outputs_file(true_positive_published_outputs_file, true_positive_pairs)
