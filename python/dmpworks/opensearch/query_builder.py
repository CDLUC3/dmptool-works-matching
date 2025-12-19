from typing import Callable, Optional

import pendulum

from dmpworks.model.common import Institution
from dmpworks.model.dmp_model import Award, DMPModel


def build_dmps_query(
    *,
    dois: Optional[list[str]] = None,
    institutions: Optional[list[Institution]] = None,
    start_date: Optional[pendulum.Date] = None,
    end_date: Optional[pendulum.Date] = None,
    inner_hits_size: int = 50,
) -> dict:
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
            lambda inst: getattr(inst, "ror"),
            lambda inst: getattr(inst, "name"),
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

    if len(project_start_dict):
        filters.append(
            {
                "range": {
                    "project_start": project_start_dict,
                }
            }
        )

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

    return query


def make_content(dmp: DMPModel) -> Optional[str]:
    has_text = dmp.title is not None or dmp.abstract is not None
    if has_text:
        return " ".join([text for text in [dmp.title, dmp.abstract] if text is not None and text != ""])

    return None


def build_dmp_works_search_query_v1(
    dmp: DMPModel, max_results: int, project_end_buffer_years: int, inner_hits_size: int
) -> dict:
    must = []
    should = []

    # Funded DOIs
    if dmp.funded_dois:
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

    # Awards
    awards = build_awards_query(
        "awards",
        dmp.external_data.awards,
        inner_hits_size=inner_hits_size,
    )
    if awards is not None:
        must.append(awards)

    # Title and abstract
    content = make_content(dmp)
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
    if dmp.project_start is not None and dmp.project_start >= pendulum.date(1990, 1, 1):
        gte = dmp.project_start.format("YYYY-MM-DD")
        lte = dmp.project_end.add(years=project_end_buffer_years).format("YYYY-MM-DD")
        filters.append(
            {
                "range": {
                    "publication_date": {
                        "gte": gte,
                        "lte": lte,
                    },
                }
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


def build_dmp_works_search_query_v2(
    dmp: DMPModel, max_results: int, project_end_buffer_years: int, inner_hits_size: int
) -> dict:
    must = []
    should = []

    #
    # should.append(
    #     {
    #         "multi_match": {
    #             "type": "most_fields",
    #             "query": f"{dmp.title} {dmp.abstract}",
    #             "fields": ["title.shingles^1", "abstract.shingles^1"],
    #             "operator": "or",
    #             # "fuzziness": "AUTO",
    #         }
    #     }
    # )

    # Funded DOIs
    if dmp.funded_dois:
        should.append(
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
        should.append(authors)

    # Institutions
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

    # Awards
    awards = build_awards_query(
        "awards",
        dmp.external_data.awards,
        inner_hits_size=inner_hits_size,
    )
    if awards is not None:
        should.append(awards)

    # Title and abstract
    content = make_content(dmp)
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
    if dmp.project_start is not None and dmp.project_start >= pendulum.date(1990, 1, 1):
        gte = dmp.project_start.format("YYYY-MM-DD")
        lte = dmp.project_end.add(years=project_end_buffer_years).format("YYYY-MM-DD")
        filters.append(
            {
                "range": {
                    "publication_date": {
                        "gte": gte,
                        "lte": lte,
                    },
                }
            }
        )

    query = {
        "size": max_results,
        "query": {
            "bool": {
                # "must": [
                #     {
                #         "bool": {
                #             "should": must,
                #             "minimum_should_match": 1,
                #         }
                #     }
                # ],
                # "must": [
                #     {
                #         "bool": {
                #             "should": should,
                #             "minimum_should_match": 1,
                #         }
                #     }
                # ],
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
) -> Optional[dict]:
    """The dis_max ensures that all the variants of a single award only contribute
    a maximum score of 10."""

    award_queries = []
    for award in awards:
        queries = []
        for award_id in award.award_id.all_variants:
            queries.append(
                {
                    "constant_score": {
                        "_name": f"awards.award_id.{award_id}",
                        "filter": {"term": {"awards.award_id": award_id}},
                        "boost": 10,
                    }
                }
            )
        award_queries.append(
            {
                "dis_max": {
                    "tie_breaker": 0,
                    "queries": queries,
                }
            }
        )

    if len(award_queries):
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


def build_entity_query(
    path: str,
    id_field: str,
    name_field: str,
    items: list,
    id_accessor: Callable,
    name_accessor: Callable,
    inner_hits_size: int = 50,
    name_slop: Optional[int] = None,
) -> Optional[dict]:
    should_queries = []

    for idx, item in enumerate(items):
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


def build_sltr_query(dmp: DMPModel, work_ids: list[str], featureset_name: str, max_results: int = 100):
    params = {
        "content": [make_content(dmp)],
        "funded_dois": dmp.funded_dois,
        # Awards
        "dmp_award_count": len(dmp.external_data.awards),
        "award_groups": build_sltr_awards_query(dmp.external_data.awards),
        # Authors
        "dmp_author_count": len(dmp.authors),
        "author_orcids": list({author.orcid for author in dmp.authors if author.orcid is not None}),
        "author_surname_queries": build_sltr_name_queries(
            "authors.full",
            {author.surname for author in dmp.authors if author.surname is not None},
        ),
        # Institutions
        "dmp_institution_count": len(dmp.institutions),
        "institution_rors": list({inst.ror for inst in dmp.institutions if inst.ror is not None}),
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
    }

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


def build_sltr_name_queries(name_field: str, names: set[str], name_slop: Optional[int] = None) -> list[dict]:
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
