import json
import pathlib
from typing import Any, Optional

from opensearchpy import OpenSearch

from dmpworks.dmsp.rank_metrics import load_qrels_dict
from dmpworks.model.dmp_model import DMPModel
from dmpworks.model.related_work_model import RelatedWork, RelatedWorkJudgement
from dmpworks.opensearch.dmp_search import fetch_dmps
from dmpworks.opensearch.dmp_works_search import search_dmp_works
from dmpworks.opensearch.utils import make_opensearch_client, OpenSearchClientConfig
from dmpworks.opensearch.query_builder import build_sltr_query


TO_JSON_SECTION_NAME = "TO_JSON_SECTION"


def build_featureset() -> dict:
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
                    "params": ["awards"],
                    "template_language": "mustache",
                    "template": template_str(
                        {
                            "function_score": {
                                "query": {"match_all": {}},
                                "boost_mode": "replace",
                                "score_mode": "sum",
                                "functions": TO_JSON_SECTION_NAME,
                            }
                        },
                        "{{#toJson}}awards{{/toJson}}",
                    ),
                },
                # Authors
                const_count_feature("dmp_author_count", "dmp_author_count"),
                identifier_feature("author_orcid_match_count", "author_orcids", "authors", "authors.orcid"),
                name_feature("author_surname_match_count", "author_surname_queries", "authors"),
                # Institutions
                const_count_feature("dmp_institution_count", "dmp_institution_count"),
                identifier_feature(
                    "institution_ror_match_count", "institution_rors", "institutions", "institutions.ror"
                ),
                name_feature("institution_name_match_count", "institution_name_queries", "institutions"),
                # Funders
                const_count_feature("dmp_funder_count", "dmp_funder_count"),
                identifier_feature("funder_ror_match_count", "funder_rors", "funders", "funders.ror"),
                name_feature("funder_name_match_count", "funder_name_queries", "funders"),
            ]
        },
    }


def identifier_feature(feature_name: str, value_param: str, path: str, field: str) -> dict:
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
    obj: dict[str, Any], to_json_section: Optional[str] = None, to_json_section_name: str = TO_JSON_SECTION_NAME
) -> str:
    json_str = json.dumps(obj, separators=(",", ":"))
    if to_json_section is not None:
        json_str = json_str.replace(f"\"{to_json_section_name}\"", to_json_section)
    return json_str


def generate_training_dataset(
    ground_truth_file: pathlib.Path,
    dmps_index_name: str,
    works_index_name: str,
    featureset_name: str,
    client_config: OpenSearchClientConfig,
    scroll_time: str = "360m",
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    include_named_queries_score: bool = True,
    inner_hits_size: int = 50,
):
    client = make_opensearch_client(client_config)

    records = []

    # Load ground truth file and get DMP DOIs
    qrels_dict = load_qrels_dict(ground_truth_file)
    dmp_dois = list(qrels_dict.keys())

    # For each DMP, perform candidate search
    with fetch_dmps(
        client=client,
        dmps_index_name=dmps_index_name,
        scroll_time=scroll_time,
        page_size=batch_size,
        dois=dmp_dois,
        inner_hits_size=inner_hits_size,
    ) as results:
        for dmp in results.dmps:
            # Candidate search
            related_works = search_dmp_works(
                client,
                works_index_name,
                dmp,
                max_results=max_results,
                project_end_buffer_years=project_end_buffer_years,
                include_named_queries_score=include_named_queries_score,
                inner_hits_size=inner_hits_size,
            )

            # Perform SLTR on candidates and get features
            work_ids = [rw.work.doi for rw in related_works]
            features = fetch_candidate_features(client, works_index_name, dmp, work_ids, featureset_name)

    # for dmp_doi, works in qrels_dict.items():

    # Load ground truth file
    # For each DMP  perform candidate search
    # TODO: for each work, check if in ground truth, label 1 if so and 0 if not.
    # TODO: run sltr query for each DMP, supplying all work DOIs, this generates features
    # TODO: build training file and save

    pass


def fetch_candidate_features(
    client: OpenSearch,
    index_name: str,
    dmp: DMPModel,
    work_ids: list[str],
    featureset_name: str,
) -> list[RelatedWorkJudgement]:
    body = build_sltr_query(dmp, work_ids, featureset_name)
    response = client.search(
        body=body,
        index=index_name,
    )
    hits = response.get("hits", {}).get("hits", [])
    rw_features = []
    for hit in hits:
        ltrlog = hit.get("fields", {}).get("_ltrlog")
        source = {
            "dmp_doi": dmp.doi,
            "work_doi": hit.get("_id"),
        }
        if len(ltrlog):
            for feature in ltrlog[0].get("features", []):
                source[feature.get("name")] = feature.get("value")
            rw_features.append(RelatedWorkJudgement.model_validate(source, by_name=True, by_alias=False))

    return rw_features


if __name__ == "__main__":
    # print(json.dumps(build_featureset()))

    generate_training_dataset(
        pathlib.Path("/path/to/ground-truth-2025-12-04.csv"),
        "dmps-index",
        "works-index",
        "dmpworks",
        OpenSearchClientConfig(),
    )
