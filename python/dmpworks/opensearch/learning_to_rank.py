import json
import logging
import pathlib
from typing import Any

import numpy as np
from opensearchpy import OpenSearch
from scipy.sparse import csr_matrix

from dmpworks.model.dmp_model import DMPModel
from dmpworks.model.related_work_model import RelatedWorkTrainingRow
from dmpworks.opensearch.dmp_search import fetch_dmps
from dmpworks.opensearch.dmp_works_search import search_dmp_works
from dmpworks.opensearch.query_builder import build_sltr_query, get_query_builder
from dmpworks.opensearch.rank_metrics import (
    get_dmp_dois,
    load_qrels_dict,
)
from dmpworks.opensearch.utils import OpenSearchClientConfig, QueryBuilder, make_opensearch_client

TO_JSON_SECTION_NAME = "TO_JSON_SECTION"


def build_featureset() -> dict:
    """Build the featureset for Learning to Rank.

    Returns:
        dict: The featureset definition.
    """
    return {
        "featureset": {
            "features": [
                {
                    "name": "mlt_content",
                    "params": ["content"],
                    "template_language": "mustache",
                    "template": {
                        "more_like_this": {
                            "_name": "content",
                            "fields": ["title", "abstract_text"],
                            "like": "{{content}}",
                            "min_term_freq": 1,
                        }
                    },
                },
                {
                    "name": "funded_doi_matched",
                    "params": ["funded_dois"],
                    "template_language": "mustache",
                    "template": template_str(
                        {
                            "constant_score": {
                                "boost": 1,
                                "filter": {"ids": {"values": TO_JSON_SECTION_NAME}},
                            }
                        },
                        "{{#toJson}}funded_dois{{/toJson}}",
                    ),
                },
                # Awards
                const_count_feature("dmp_award_count", "dmp_award_count"),
                {
                    "name": "award_match_count",
                    "params": ["award_groups"],
                    "template_language": "mustache",
                    "template": template_str(
                        {"bool": {"should": TO_JSON_SECTION_NAME, "minimum_should_match": 1}},
                        "{{#toJson}}award_groups{{/toJson}}",
                    ),
                },
                # Authors
                const_count_feature(
                    "dmp_author_count",
                    "dmp_author_count",
                ),
                identifier_feature(
                    "author_orcid_match_count",
                    "author_orcids",
                    "authors",
                    "authors.orcid",
                ),
                name_feature(
                    "author_surname_match_count",
                    "author_surname_queries",
                    "authors",
                ),
                # Institutions
                const_count_feature(
                    "dmp_institution_count",
                    "dmp_institution_count",
                ),
                identifier_feature(
                    "institution_ror_match_count",
                    "institution_rors",
                    "institutions",
                    "institutions.ror",
                ),
                name_feature(
                    "institution_name_match_count",
                    "institution_name_queries",
                    "institutions",
                ),
                # Funders
                const_count_feature(
                    "dmp_funder_count",
                    "dmp_funder_count",
                ),
                identifier_feature(
                    "funder_ror_match_count",
                    "funder_rors",
                    "funders",
                    "funders.ror",
                ),
                name_feature(
                    "funder_name_match_count",
                    "funder_name_queries",
                    "funders",
                ),
                # Relations
                identifier_feature(
                    "intra_work_doi_count",
                    "published_output_dois",
                    "relations.intra_work_dois",
                    "relations.intra_work_dois.doi",
                ),
                identifier_feature(
                    "possible_shared_project_doi_count",
                    "published_output_dois",
                    "relations.possible_shared_project_dois",
                    "relations.possible_shared_project_dois.doi",
                ),
                identifier_feature(
                    "dataset_citation_doi_count",
                    "published_output_dois",
                    "relations.dataset_citation_dois",
                    "relations.dataset_citation_dois.doi",
                ),
            ]
        },
    }


def identifier_feature(feature_name: str, value_param: str, path: str, field: str) -> dict:
    """Create a feature definition for matching identifiers in a nested field.

    Args:
        feature_name: The name of the feature.
        value_param: The name of the parameter containing the identifier values.
        path: The nested path to search.
        field: The field to match against.

    Returns:
        dict: The feature definition.
    """
    return {
        "name": feature_name,
        "params": [value_param],
        "template_language": "mustache",
        "template": template_str(
            {
                "nested": {
                    "path": path,
                    "score_mode": "sum",
                    "query": {
                        "constant_score": {
                            "boost": 1,
                            "filter": {"terms": {field: TO_JSON_SECTION_NAME}},
                        }
                    },
                }
            },
            "{{#toJson}}" + value_param + "{{/toJson}}",
        ),
    }


def name_feature(feature_name: str, value_param: str, path: str) -> dict:
    """Create a feature definition for matching names in a nested field.

    Args:
        feature_name: The name of the feature.
        value_param: The name of the parameter containing the name queries.
        path: The nested path to search.

    Returns:
        dict: The feature definition.
    """
    return {
        "name": feature_name,
        "params": [value_param],
        "template_language": "mustache",
        "template": template_str(
            {
                "nested": {
                    "path": path,
                    "score_mode": "sum",
                    "query": {
                        "dis_max": {
                            "tie_breaker": 0,
                            "queries": TO_JSON_SECTION_NAME,
                        }
                    },
                }
            },
            "{{#toJson}}" + value_param + "{{/toJson}}",
        ),
    }


def const_count_feature(feature_name: str, value_param: str) -> dict:
    """Create a feature definition that returns a constant value passed as a parameter.

    Args:
        feature_name: The name of the feature.
        value_param: The name of the parameter containing the constant value.

    Returns:
        dict: The feature definition.
    """
    return {
        "name": feature_name,
        "params": [value_param],
        "template_language": "mustache",
        "template": template_str(
            {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {"source": "params.c", "params": {"c": TO_JSON_SECTION_NAME}},
                }
            },
            "{{#toJson}}" + value_param + "{{/toJson}}",
        ),
    }


def template_str(
    obj: dict[str, Any], to_json_section: str | None = None, to_json_section_name: str = TO_JSON_SECTION_NAME
) -> str:
    """Convert a dictionary to a JSON string, optionally replacing a placeholder with a template variable.

    Args:
        obj: The dictionary to convert.
        to_json_section: The string to replace the placeholder with.
        to_json_section_name: The placeholder string to look for.

    Returns:
        str: The JSON string.
    """
    json_str = json.dumps(obj, separators=(",", ":"))
    if to_json_section is not None:
        json_str = json_str.replace(f'"{to_json_section_name}"', to_json_section)
    return json_str


def fetch_candidate_features(
    client: OpenSearch,
    index_name: str,
    dmp: DMPModel,
    work_ids: list[str],
    featureset_name: str,
    max_results: int = 100,
) -> list[RelatedWorkTrainingRow]:
    """Fetch feature values for candidate works using SLTR query.

    Args:
        client: The OpenSearch client.
        index_name: The name of the works index.
        dmp: The DMP model.
        work_ids: A list of work DOIs to fetch features for.
        featureset_name: The name of the featureset to use.
        max_results: The maximum number of results to return.

    Returns:
        list[RelatedWorkTrainingRow]: A list of training rows containing feature values.
    """
    body = build_sltr_query(dmp, work_ids, featureset_name, max_results=max_results)
    response = client.search(
        body=body,
        index=index_name,
    )
    hits = response.get("hits", {}).get("hits", [])
    rw_features = []
    for hit in hits:
        ltrlog = hit.get("fields", {}).get("_ltrlog")
        source = {"dmp_doi": dmp.doi, "work_doi": hit.get("_id"), "work_title": hit.get("_source", {}).get("title")}
        feature_names = []
        if len(ltrlog):
            for feature in ltrlog[0].get("features", []):
                feature_name = feature.get("name")
                feature_names.append(feature_name)
                source[feature_name] = feature.get("value")
            rw_features.append(RelatedWorkTrainingRow.model_validate(source, by_name=True, by_alias=False))
    return rw_features


def generate_training_dataset(
    ground_truth_file: pathlib.Path,
    dmps_index_name: str,
    works_index_name: str,
    featureset_name: str,
    output_file: pathlib.Path,
    client_config: OpenSearchClientConfig,
    query_builder_name: QueryBuilder = "build_dmp_works_search_baseline_query",
    scroll_time: str = "360m",
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    include_named_queries_score: bool = True,
    inner_hits_size: int = 50,
):
    """Generate a training dataset for Learning to Rank.

    Args:
        ground_truth_file: The path to the ground truth file.
        dmps_index_name: The name of the DMPs index.
        works_index_name: The name of the works index.
        featureset_name: The name of the featureset to use.
        output_file: The path to the output file.
        client_config: The OpenSearch client configuration.
        query_builder_name: The name of the query builder to use.
        scroll_time: The scroll time for the search context.
        batch_size: The number of DMPs to process per batch.
        max_results: The maximum number of results to return per DMP.
        project_end_buffer_years: The number of years to buffer the project end date.
        include_named_queries_score: Whether to include named queries scores.
        inner_hits_size: The size of inner hits to return for nested fields.
    """
    logging.info("Generating training dataset...")
    client = make_opensearch_client(client_config)

    # Load ground truth file and get DMP DOIs
    qrels_dict_all = load_qrels_dict(ground_truth_file)
    dmp_dois = list(get_dmp_dois(qrels_dict_all))
    query_builder = get_query_builder(query_builder_name)

    # For each DMP, perform candidate search
    with (
        fetch_dmps(
            client=client,
            dmps_index_name=dmps_index_name,
            scroll_time=scroll_time,
            page_size=batch_size,
            dois=dmp_dois,
            inner_hits_size=inner_hits_size,
        ) as results,
        output_file.open(mode="w") as f_out,
    ):
        for dmp in results.dmps:
            logging.info(f"DMP: {dmp.doi}")

            # Candidate search
            related_works = search_dmp_works(
                client,
                works_index_name,
                dmp,
                query_builder,
                max_results=max_results,
                project_end_buffer_years=project_end_buffer_years,
                include_named_queries_score=include_named_queries_score,
                inner_hits_size=inner_hits_size,
            )

            # Perform SLTR on candidates, convert to training records and save
            work_ids = [rw.work.doi for rw in related_works]
            training_rows = fetch_candidate_features(
                client, works_index_name, dmp, work_ids, featureset_name, max_results=max_results
            )
            for row in training_rows:
                row.judgement = qrels_dict_all.get(row.dmp_doi, {}).get(row.work_doi, 0)
                f_out.write(row.to_ranklib() + "\n")


def create_featureset(
    client: OpenSearch,
    featureset_name: str,
):
    """Create a featureset in OpenSearch.

    Args:
        client: The OpenSearch client.
        featureset_name: The name of the featureset to create.
    """
    logging.info(f"Creating featureset: {featureset_name}")

    featureset = build_featureset()
    response = client.transport.perform_request(
        method="POST",
        url=f"/_ltr/_featureset/{featureset_name}",
        body=featureset,
    )
    logging.info(response)


def upload_ranklib_model(
    client: OpenSearch,
    featureset_name: str,
    model_name: str,
    ranklib_model_file: pathlib.Path,
    ranklib_features_file: pathlib.Path,
    training_dataset_file: pathlib.Path,
):
    """Upload a RankLib model to OpenSearch.

    Args:
        client: The OpenSearch client.
        featureset_name: The name of the featureset.
        model_name: The name of the model.
        ranklib_model_file: The path to the RankLib model file.
        ranklib_features_file: The path to the RankLib features file.
        training_dataset_file: The path to the training dataset file.
    """
    logging.info(f"Uploading RankLib model for featureset={featureset_name} with model_name={model_name}")

    with ranklib_model_file.open(encoding="utf-8") as f_in:
        ranklib_definition = f_in.read()

    feature_normalizers = build_feature_normalizers(
        client,
        featureset_name,
        ranklib_features_file,
        training_dataset_file,
    )

    body = {
        "model": {
            "name": model_name,
            "model": {
                "type": "model/ranklib",
                "definition": ranklib_definition,
                "feature_normalizers": feature_normalizers,
            },
        }
    }

    response = client.transport.perform_request(
        "POST",
        url=f"/_ltr/_featureset/{featureset_name}/_createmodel",
        body=body,
    )
    logging.info(response)


def load_ranklib_training_file(ranklib_training_file: pathlib.Path) -> tuple[np.array, np.array, list[str]]:
    """Load a RankLib training file into numpy arrays.

    Args:
        ranklib_training_file: The path to the RankLib training file.

    Returns:
        tuple: A tuple containing the feature matrix (X), labels (y), and query IDs (qids).
    """
    qids = []
    labels = []
    rows, cols, vals = [], [], []
    current_row = 0
    max_col = 0

    with ranklib_training_file.open(encoding="utf-8") as f_in:
        for ln in f_in:
            # Remove comments from line
            line = ln.split("#")[0].strip()
            if not line:
                continue

            # Parse the training data
            parts = line.split(" ")
            label = int(parts[0])
            labels.append(label)
            qid = parts[1].split(":")[1]
            qids.append(qid)
            for feature in parts[2:]:
                feature_id, feature_val = feature.split(":")
                feature_id = int(feature_id) - 1
                feature_val = float(feature_val)
                rows.append(current_row)
                cols.append(feature_id)
                vals.append(feature_val)
                max_col = max(max_col, feature_id)
            current_row += 1

    X = csr_matrix((vals, (rows, cols)), shape=(current_row, max_col + 1)).toarray()  # noqa: N806
    y = np.array(labels)

    return X, y, qids


def build_feature_normalizers(
    client: OpenSearch, featureset_name: str, ranklib_features_file: pathlib.Path, ranklib_training_file: pathlib.Path
) -> dict:
    """Build feature normalizers based on training data statistics.

    Args:
        client: The OpenSearch client.
        featureset_name: The name of the featureset.
        ranklib_features_file: The path to the RankLib features file.
        ranklib_training_file: The path to the RankLib training file.

    Returns:
        dict: A dictionary of feature normalizers.
    """
    # Load features
    feature_ids = set()
    with ranklib_features_file.open(encoding="utf-8") as f_in:
        for feature_id in f_in:
            feature_ids.add(int(feature_id))

    # Get features
    response = client.transport.perform_request("GET", f"/_ltr/_featureset/{featureset_name}")
    features = response.get("_source", {}).get("featureset", {}).get("features", [])
    idx_to_name = {i + 1: feature["name"] for i, feature in enumerate(features)}

    # Create feature normalizers
    feature_normalizers = {}
    X, y, qids = load_ranklib_training_file(ranklib_training_file)  # noqa: N806
    means = np.mean(X, axis=0)
    stds = np.std(X, axis=0)

    for i, (mean, std_dev) in enumerate(zip(means, stds, strict=False)):
        feature_id = i + 1
        if feature_id in feature_ids:
            # Convert feature_id to feature name
            feature_name = idx_to_name[feature_id]
            feature_normalizers[feature_name] = {
                "standard": {
                    "mean": round(float(mean), 7),
                    "standard_deviation": round(float(np.maximum(np.mean(std_dev), 1e-7)), 7),
                }
            }
            logging.info(f"Feature {feature_id}: mean={mean:.4f}, std={std_dev:.4f}")

    return feature_normalizers
