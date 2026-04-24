from collections.abc import Callable
import copy
from dataclasses import dataclass
import logging

import pendulum

from dmpworks.model.common import Institution
from dmpworks.model.dmp_model import Award, DMPModel, FundingItem

log = logging.getLogger(__name__)

MIN_START_DATE = pendulum.date(1990, 1, 1)


@dataclass(frozen=True)
class QueryFeatures:
    """Toggles for features emitted by the baseline query. All on by default.

    Disabling a feature causes its clause to be omitted from the query. Used
    for ablation studies run via the rank-metrics CLI; production search runs
    with defaults.
    """

    funded_dois: bool = True
    authors: bool = True
    institutions: bool = True
    funders: bool = True
    awards: bool = True
    content: bool = True
    relations: bool = True

    def disabled_names(self) -> list[str]:
        """Return the names of disabled features, sorted."""
        return sorted(f for f, v in self.__dict__.items() if not v)


def get_query_builder(name: str) -> Callable[[DMPModel, int, int, int], dict]:
    """Get a query builder function by name.

    Args:
        name: The name of the query builder function.

    Returns:
        Callable[[DMPModel, int, int, int], dict]: The query builder function.

    Raises:
        ValueError: If the query builder name is unknown.
    """
    query_builders = {
        "build_dmp_works_search_baseline_query": build_dmp_works_search_baseline_query,
    }
    if name not in query_builders:
        raise ValueError(f"Unknown query builder name: {name}")
    return query_builders[name]


def build_dmps_query(
    *,
    dois: list[str] | None = None,
    institutions: list[Institution] | None = None,
    start_date: pendulum.Date | None = None,
    end_date: pendulum.Date | None = None,
    modified_since: pendulum.Date | None = None,
    inner_hits_size: int = 50,
) -> dict:
    """Build a query for searching DMPs.

    Args:
        dois: A list of DOIs to filter by.
        institutions: A list of institutions to filter by.
        start_date: The start date for the project start range filter.
        end_date: The end date for the project start range filter.
        modified_since: Only return DMPs with a modified date on or after this date.
        inner_hits_size: The size of inner hits to return for nested fields.

    Returns:
        dict: The OpenSearch query.
    """
    should = []

    # Filter by DOIs
    if dois:
        should.append(
            {
                "ids": {"values": dois},
            }
        )

    # Filter by institutions
    if institutions:
        query = build_entity_query(
            "institutions",
            "institutions.ror",
            "institutions.name",
            institutions,
            lambda inst: inst.ror,
            lambda inst: inst.name,
            inner_hits_size=inner_hits_size,
            name_slop=3,
        )
        should.append(query)

    filters = []
    project_start_dict = {}
    if start_date is not None:
        project_start_dict["gte"] = start_date.format("YYYY-MM-DD")
    if end_date is not None:
        project_start_dict["lte"] = end_date.format("YYYY-MM-DD")

    if len(project_start_dict) > 0:
        filters.append(
            {
                "range": {
                    "project_start": project_start_dict,
                }
            }
        )

    if modified_since is not None:
        filters.append({"range": {"modified": {"gte": modified_since.format("YYYY-MM-DD")}}})

    # Build final query
    query = {"query": {}}
    bool_components = {}

    if should:
        bool_components["should"] = should
        bool_components["minimum_should_match"] = 1

    if filters:
        bool_components["filter"] = filters

    if bool_components:
        query["query"]["bool"] = bool_components
    else:
        query["query"]["match_all"] = {}

    query["sort"] = [{"_id": "asc"}]

    return query


def make_content(dmp: DMPModel) -> str | None:
    """Create a content string from DMP title and abstract.

    Args:
        dmp: The DMP model.

    Returns:
        Optional[str]: The content string, or None if no text is available.
    """
    has_text = dmp.title is not None or dmp.abstract_text is not None
    if has_text:
        return " ".join([text for text in [dmp.title, dmp.abstract_text] if text is not None and text != ""])

    return None


def build_dmp_works_search_baseline_query(
    dmp: DMPModel,
    max_results: int,
    project_end_buffer_years: int,
    inner_hits_size: int,
    *,
    features: QueryFeatures | None = None,
) -> dict:
    """Baseline DMP works search query using manually tuned weights.

    Args:
        dmp: The DMP model.
        max_results: The maximum number of results to return.
        project_end_buffer_years: The number of years to buffer the project end date.
        inner_hits_size: The size of inner hits to return for nested fields.
        features: Per-feature toggles. Disabled features omit their clause from the query.
            Defaults to all features on.

    Returns:
        dict: The OpenSearch query. When every must-clause feature is disabled or produces
        no clauses, OpenSearch returns zero hits for this DMP (an empty ``bool.should`` with
        ``minimum_should_match: 1`` matches nothing).
    """
    features = features if features is not None else QueryFeatures()
    must = []
    should = []

    # Funded DOIs
    if features.funded_dois and dmp.funded_dois:
        must.append(
            {
                "constant_score": {
                    "_name": "funded_dois",
                    "boost": 15,
                    "filter": {
                        "ids": {"values": dmp.funded_dois},
                    },
                }
            }
        )

    # Authors
    if features.authors:
        authors = build_entity_query(
            "authors",
            "authors.orcid",
            "authors.full",
            dmp.authors,
            lambda author: author.orcid,
            lambda author: author.surname,
            inner_hits_size=inner_hits_size,
            name_slop=None,
        )
        if authors is not None:
            must.append(authors)

    # Institutions
    if features.institutions:
        institutions = build_entity_query(
            "institutions",
            "institutions.ror",
            "institutions.name",
            dmp.institutions,
            lambda inst: inst.ror,
            lambda inst: inst.name,
            inner_hits_size=inner_hits_size,
            name_slop=3,
        )
        if institutions is not None:
            should.append(institutions)

    # Funders
    if features.funders:
        funders = build_entity_query(
            "funders",
            "funders.ror",
            "funders.name",
            dmp.funding,
            lambda fund: fund.funder.ror,
            lambda fund: fund.funder.name,
            inner_hits_size=inner_hits_size,
            name_slop=3,
        )
        if funders is not None:
            should.append(funders)

    # Awards (fall back to raw funding identifiers when no parsed awards available)
    if features.awards:
        if dmp.external_data.awards:
            awards = build_awards_query(
                "awards",
                dmp.external_data.awards,
                inner_hits_size=inner_hits_size,
            )
        else:
            awards = build_raw_awards_query(
                "awards",
                dmp.funding,
                inner_hits_size=inner_hits_size,
            )
        if awards is not None:
            must.append(awards)

    # Title and abstract
    content = make_content(dmp) if features.content else None
    if content:
        should.append(
            {
                "more_like_this": {
                    "_name": "content",
                    "fields": ["title", "abstract_text"],
                    "like": content,
                    "min_term_freq": 1,
                }
            }
        )

    # Relations
    # Intra work DOIs are the same core work, so they get a high rank.
    # These are appended to must because if one of these matches it is almost
    # certainly a match.
    if features.relations:
        published_outputs = dmp.published_outputs if dmp.published_outputs is not None else []
        intra_work_dois = build_relations_query(
            "relations.intra_work_dois",
            "relations.intra_work_dois.doi",
            [work.doi for work in published_outputs],
            boost=10.0,
        )
        if intra_work_dois is not None:
            must.append(intra_work_dois)

        # Inter work DOIs with relation types that can be used for linking works
        # published as a part of the same project. E.g. a supplement rather than
        # a citation.
        possible_shared_project_dois = build_relations_query(
            "relations.possible_shared_project_dois",
            "relations.possible_shared_project_dois.doi",
            [work.doi for work in published_outputs],
            boost=5.0,
        )
        if possible_shared_project_dois is not None:
            must.append(possible_shared_project_dois)

        # Dataset citations, these are any kind of citation of a dataset, but still
        # could be useful information, so have ranked lower than the above two.
        dataset_citation_dois = build_relations_query(
            "relations.dataset_citation_dois",
            "relations.dataset_citation_dois.doi",
            [work.doi for work in published_outputs],
            boost=2.5,
        )
        if dataset_citation_dois is not None:
            must.append(dataset_citation_dois)

    if not must:
        log.warning(
            f"No must-clause features produced a clause for DMP {dmp.doi}. "
            f"Disabled features: {features.disabled_names()}. DMP will return zero results."
        )

    # Final query and filter based on date range
    # also remove DMPs from search results (OUTPUT_MANAGEMENT_PLAN)
    filters = [
        {
            "bool": {
                "must_not": {
                    "term": {
                        "work_type": "OUTPUT_MANAGEMENT_PLAN",
                    }
                }
            },
        }
    ]

    # Setup date filter
    start_date = dmp.project_start if dmp.project_start is not None else MIN_START_DATE
    date_range = {
        "gte": start_date.format("YYYY-MM-DD"),
    }
    if dmp.project_end is not None:
        date_range["lte"] = dmp.project_end.add(years=project_end_buffer_years).format("YYYY-MM-DD")
    filters.append(
        {
            "range": {
                "publication_date": date_range,
            },
        }
    )

    query = {
        "size": max_results,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": must,
                            "minimum_should_match": 1,
                        }
                    }
                ],
                "should": should,
                "filter": filters,
            },
        },
    }

    if content:
        query["highlight"] = {
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
            "order": "score",
            "require_field_match": True,
            "fields": {
                "title": {
                    "type": "fvh",
                    "number_of_fragments": 0,
                    "fragment_size": 0,
                    "no_match_size": 500,
                },
                "abstract_text": {
                    "type": "fvh",
                    "fragment_size": 160,
                    "number_of_fragments": 2,
                    "no_match_size": 160,
                },
            },
            "highlight_query": {
                "more_like_this": {
                    "fields": ["title", "abstract_text"],
                    "like": content,
                    "min_term_freq": 1,
                }
            },
        }

    # print(json.dumps(query))

    return query


def build_awards_query(
    path: str,
    awards: list[Award],
    inner_hits_size: int = 50,
) -> dict | None:
    """Build a nested OpenSearch query for matching award identifiers.

    Uses `dis_max` so that multiple variants of the same award contribute
    only a single score (boost 10) rather than accumulating scores.

    Args:
        path: Nested document path for awards.
        awards: List of award objects containing identifier variants.
        inner_hits_size: Maximum number of matching nested documents to return.

    Returns:
        Nested query dict if awards are provided, otherwise None.
    """
    award_queries = []
    for award in awards:
        queries = [
            {
                "constant_score": {
                    "_name": f"awards.award_id.{award_id}",
                    "filter": {"term": {"awards.award_id": award_id}},
                    "boost": 10,
                }
            }
            for award_id in award.award_id.all_variants
        ]
        award_queries.append(
            {
                "dis_max": {
                    "tie_breaker": 0,
                    "queries": queries,
                }
            }
        )

    if len(award_queries) > 0:
        return {
            "nested": {
                "path": path,
                "query": {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": award_queries,
                    }
                },
                "inner_hits": {
                    "name": path,
                    "size": inner_hits_size,
                },
            },
        }

    return None


def build_raw_awards_query(
    path: str,
    funding: list[FundingItem],
    inner_hits_size: int = 50,
) -> dict | None:
    """Build a nested OpenSearch query matching raw funding identifiers.

    Used as a fallback when no parsed awards are available (e.g. for funders
    that are not yet supported by an AwardID parser). Matches the raw values
    of funding_opportunity_id, award_id, and funder_project_number directly
    against the works index `awards.award_id` field.

    Args:
        path: Nested document path for awards.
        funding: List of FundingItem objects containing raw identifiers.
        inner_hits_size: Maximum number of matching nested documents to return.

    Returns:
        Nested query dict if any funding item has raw identifiers, otherwise None.
    """
    funder_queries = []
    for fund in funding:
        raw_ids = {raw for raw in (fund.funding_opportunity_id, fund.award_id, fund.funder_project_number) if raw}
        if not raw_ids:
            continue
        queries = [
            {
                "constant_score": {
                    "_name": f"awards.award_id.{raw_id}",
                    "filter": {"term": {"awards.award_id": raw_id}},
                    "boost": 10,
                }
            }
            for raw_id in raw_ids
        ]
        funder_queries.append(
            {
                "dis_max": {
                    "tie_breaker": 0,
                    "queries": queries,
                }
            }
        )

    if len(funder_queries) > 0:
        return {
            "nested": {
                "path": path,
                "query": {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": funder_queries,
                    }
                },
                "inner_hits": {
                    "name": path,
                    "size": inner_hits_size,
                },
            },
        }

    return None


def build_relations_query(
    path: str,
    doi_field: str,
    dois: list[str],
    inner_hits_size: int = 50,
    boost: float = 1.0,
) -> dict | None:
    """Build a nested query for matching related DOIs.

    Args:
        path: The nested path to search.
        doi_field: The field containing the DOI.
        dois: A list of DOIs to match.
        inner_hits_size: The size of inner hits to return.
        boost: The boost factor for the query.

    Returns:
        Optional[dict]: The nested query, or None if no DOIs are provided.
    """
    should_queries: list[dict] = [
        {
            "constant_score": {
                "_name": f"{doi_field}.{doi}",
                "filter": {"term": {doi_field: doi}},
                "boost": boost,
            }
        }
        for doi in dois
    ]

    if not should_queries:
        return None

    return {
        "nested": {
            "path": path,
            "query": {
                "bool": {
                    "minimum_should_match": 1,
                    "should": should_queries,
                }
            },
            "inner_hits": {
                "name": path,
                "size": inner_hits_size,
            },
        }
    }


def build_entity_query(
    path: str,
    id_field: str,
    name_field: str,
    items: list,
    id_accessor: Callable,
    name_accessor: Callable,
    inner_hits_size: int = 50,
    name_slop: int | None = None,
) -> dict | None:
    """Build a nested query for matching entities (authors, institutions, funders).

    Args:
        path: The nested path to search.
        id_field: The field containing the entity ID.
        name_field: The field containing the entity name.
        items: A list of items to match.
        id_accessor: A function to access the ID from an item.
        name_accessor: A function to access the name from an item.
        inner_hits_size: The size of inner hits to return.
        name_slop: The slop for the name match phrase query.

    Returns:
        Optional[dict]: The nested query, or None if no items match.
    """
    should_queries = []

    for item in items:
        entity_queries = []
        entity_id = id_accessor(item)
        entity_name = name_accessor(item)

        if entity_id is not None:
            entity_queries.append(
                {
                    "constant_score": {
                        "_name": f"{id_field}.{entity_id}",
                        "filter": {"term": {id_field: entity_id}},
                        "boost": 2,
                    }
                }
            )

        if entity_name is not None:
            name_query = {
                "constant_score": {
                    "_name": f"{name_field}.{entity_name}",
                    "filter": {"match_phrase": {name_field: {"query": entity_name}}},
                    "boost": 1,
                }
            }
            if name_slop is not None:
                name_query["constant_score"]["filter"]["match_phrase"][name_field]["slop"] = name_slop
            entity_queries.append(name_query)

        if len(entity_queries) > 1:
            should_queries.append(
                {
                    "dis_max": {
                        "tie_breaker": 0,
                        "queries": entity_queries,
                    },
                }
            )
        elif len(entity_queries) == 1:
            should_queries.append(entity_queries[0])

    if should_queries:
        return {
            "nested": {
                "path": path,
                "query": {
                    "bool": {
                        "minimum_should_match": 1,
                        "should": should_queries,
                    }
                },
                "inner_hits": {
                    "name": path,
                    "size": inner_hits_size,
                },
            },
        }

    return None


def build_ltr_features(dmp: DMPModel):
    """Build features for Learning to Rank.

    Args:
        dmp: The DMP model.

    Returns:
        dict: A dictionary of features.
    """
    published_outputs = dmp.published_outputs if dmp.published_outputs is not None else []

    # Awards: use parsed external awards when available, otherwise fall back to
    # raw funding identifiers so unsupported funders still contribute features.
    if dmp.external_data.awards:
        dmp_award_count = len(dmp.external_data.awards)
        award_groups = build_sltr_awards_query(dmp.external_data.awards)
    else:
        funding_with_raw = [
            fund
            for fund in dmp.funding
            if any([fund.funding_opportunity_id, fund.award_id, fund.funder_project_number])
        ]
        dmp_award_count = len(funding_with_raw)
        award_groups = build_sltr_raw_awards_query(funding_with_raw)

    return {
        "content": [make_content(dmp)],
        "funded_dois": dmp.funded_dois,
        # Awards
        "dmp_award_count": dmp_award_count,
        "award_groups": award_groups,
        # Authors
        "dmp_author_count": len(dmp.authors),
        "author_orcids": list(
            {author.orcid for author in dmp.authors if author.orcid is not None},
        ),
        "author_surname_queries": build_sltr_name_queries(
            "authors.full",
            {author.surname for author in dmp.authors if author.surname is not None},
        ),
        # Institutions
        "dmp_institution_count": len(dmp.institutions),
        "institution_rors": list(
            {inst.ror for inst in dmp.institutions if inst.ror is not None},
        ),
        "institution_name_queries": build_sltr_name_queries(
            "institutions.name",
            {inst.name for inst in dmp.institutions if inst.name is not None},
            name_slop=3,
        ),
        # Funders
        "dmp_funder_count": len(dmp.funding),
        "funder_rors": list(
            {fund.funder.ror for fund in dmp.funding if fund.funder is not None and fund.funder.ror is not None}
        ),
        "funder_name_queries": build_sltr_name_queries(
            "funders.name",
            {fund.funder.name for fund in dmp.funding if fund.funder is not None and fund.funder.name is not None},
            name_slop=3,
        ),
        # Relations
        "published_output_dois": list({output.doi for output in published_outputs if output.doi is not None}),
    }


def build_sltr_query(dmp: DMPModel, work_ids: list[str], featureset_name: str, max_results: int = 100):
    """Build a SLTR query for logging features.

    Args:
        dmp: The DMP model.
        work_ids: A list of work DOIs to filter by.
        featureset_name: The name of the featureset.
        max_results: The maximum number of results to return.

    Returns:
        dict: The OpenSearch query.
    """
    params = build_ltr_features(dmp)

    return {
        "size": max_results,
        "query": {
            "bool": {
                "filter": [
                    {"ids": {"values": work_ids}},
                    {
                        "sltr": {
                            "_name": "logged_featureset",
                            "featureset": featureset_name,
                            "params": params,
                        }
                    },
                ]
            }
        },
        "ext": {
            "ltr_log": {
                "log_specs": {
                    "name": "features",
                    "named_query": "logged_featureset",
                    "missing_as_zero": True,
                }
            }
        },
    }


def build_sltr_awards_query(awards: list[Award]) -> list[dict]:
    """Build SLTR query components for awards.

    Args:
        awards: A list of awards.

    Returns:
        list[dict]: A list of query components.
    """
    if len(awards) == 0:
        return [{"match_none": {}}]

    queries = []
    for award in awards:
        query = {
            "constant_score": {
                "boost": 1,
                "filter": {
                    "nested": {"path": "awards", "query": {"terms": {"awards.award_id": award.award_id.all_variants}}}
                },
            }
        }
        queries.append(query)
    return queries


def build_sltr_raw_awards_query(funding: list[FundingItem]) -> list[dict]:
    """Build SLTR query components for raw funding identifiers.

    Fallback variant of build_sltr_awards_query used when no parsed awards are
    available. Each funder contributes one nested terms query against
    awards.award_id with its non-null raw identifiers.

    Args:
        funding: A list of FundingItem objects.

    Returns:
        list[dict]: A list of query components.
    """
    queries = []
    for fund in funding:
        raw_ids = [raw for raw in (fund.funding_opportunity_id, fund.award_id, fund.funder_project_number) if raw]
        if not raw_ids:
            continue
        queries.append(
            {
                "constant_score": {
                    "boost": 1,
                    "filter": {"nested": {"path": "awards", "query": {"terms": {"awards.award_id": raw_ids}}}},
                }
            }
        )
    if not queries:
        return [{"match_none": {}}]
    return queries


def build_sltr_name_queries(name_field: str, names: set[str], name_slop: int | None = None) -> list[dict]:
    """Build SLTR query components for names.

    Args:
        name_field: The field to match against.
        names: A set of names to match.
        name_slop: The slop for the match phrase query.

    Returns:
        list[dict]: A list of query components.
    """
    if len(names) == 0:
        return [{"match_none": {}}]

    queries = []
    for name in names:
        query = {
            "constant_score": {
                "boost": 1,
                "filter": {
                    "match_phrase": {
                        name_field: {
                            "query": name,
                        }
                    }
                },
            }
        }
        if name_slop is not None:
            query["constant_score"]["filter"]["match_phrase"][name_field]["slop"] = name_slop
        queries.append(query)
    return queries


def build_dmp_works_search_rerank_query(
    dmp: DMPModel,
    base_query: dict,
    max_results: int,
    model_name: str,
) -> dict:
    """Build a rerank query using a Learning to Rank model.

    Args:
        dmp: The DMP model.
        base_query: The base query to rescore.
        max_results: The window size for rescoring.
        model_name: The name of the LTR model.

    Returns:
        dict: The OpenSearch query with rescoring.
    """
    ltr_query = copy.deepcopy(base_query)
    ltr_features = build_ltr_features(dmp)
    ltr_query["rescore"] = {
        "window_size": max_results,
        "query": {
            "rescore_query": {
                "sltr": {
                    "model": model_name,
                    "params": ltr_features,
                }
            },
            "query_weight": 0.001,
            "rescore_query_weight": 1.0,
        },
    }
    return ltr_query
