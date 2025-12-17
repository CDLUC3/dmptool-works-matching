import logging
import math
import pathlib
from typing import Optional

import jsonlines
import pendulum
from opensearchpy import OpenSearch
from tqdm import tqdm

from dmpworks.model.common import Institution
from dmpworks.model.dmp_model import DMPModel
from dmpworks.model.related_work_model import ContentMatch, DoiMatch, DoiMatchSource, ItemMatch, RelatedWork
from dmpworks.model.work_model import WorkModel
from dmpworks.opensearch.dmp_search import fetch_dmps
from dmpworks.opensearch.utils import make_opensearch_client, OpenSearchClientConfig
from dmpworks.utils import timed
from opensearch.query_builder import build_dmp_works_search_query

log = logging.getLogger(__name__)


@timed
def dmp_works_search(
    dmps_index_name: str,
    works_index_name: str,
    out_file: pathlib.Path,
    client_config: OpenSearchClientConfig,
    scroll_time: str = "360m",
    batch_size: int = 100,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    parallel_search: bool = True,
    include_named_queries_score: bool = False,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
    institutions: list[Institution] = None,
    dois: list[str] = None,
    start_date: Optional[pendulum.Date] = None,
    end_date: Optional[pendulum.Date] = None,
    inner_hits_size: int = 50,
):
    client = make_opensearch_client(client_config)

    if parallel_search and include_named_queries_score:
        log.warning("Unable to use include_named_queries_score with msearch, query scores will not be returned.")

    def write_works(works: list[RelatedWork], count: int):
        for work in works:
            writer.write(
                # Convert fields to CamelCase for database
                work.model_dump(
                    by_alias=True,
                    mode="json",
                )
            )
        pbar.update(count)

    with tqdm(total=0, desc="Find DMP work matches with OpenSearch", unit="doc") as pbar:
        with fetch_dmps(
            client=client,
            dmps_index_name=dmps_index_name,
            scroll_time=scroll_time,
            page_size=batch_size,
            dois=dois,
            institutions=institutions,
            start_date=start_date,
            end_date=end_date,
            inner_hits_size=inner_hits_size,
        ) as results:
            pbar.total = results.total_dmps

            with jsonlines.open(out_file, mode='w') as writer:
                batch = []
                for dmp in results.dmps:
                    if not parallel_search or include_named_queries_score:
                        works = search_dmp_works(
                            client,
                            works_index_name,
                            dmp,
                            max_results=max_results,
                            project_end_buffer_years=project_end_buffer_years,
                            include_named_queries_score=include_named_queries_score,
                            inner_hits_size=inner_hits_size,
                        )
                        write_works(works, 1)
                    else:
                        batch.append(dmp)
                        if len(batch) >= batch_size:
                            works = msearch_dmp_works(
                                client,
                                works_index_name,
                                batch,
                                max_results=max_results,
                                project_end_buffer_years=project_end_buffer_years,
                                max_concurrent_searches=max_concurrent_searches,
                                max_concurrent_shard_requests=max_concurrent_shard_requests,
                                inner_hits_size=inner_hits_size,
                            )
                            write_works(works, len(batch))
                            batch = []

                if parallel_search and batch:
                    works = msearch_dmp_works(
                        client,
                        works_index_name,
                        batch,
                        max_results=max_results,
                        project_end_buffer_years=project_end_buffer_years,
                        max_concurrent_searches=max_concurrent_searches,
                        max_concurrent_shard_requests=max_concurrent_shard_requests,
                        inner_hits_size=inner_hits_size,
                    )
                    write_works(works, len(batch))


def msearch_dmp_works(
    client: OpenSearch,
    index_name: str,
    dmps: list[DMPModel],
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    max_concurrent_searches: int = 125,
    max_concurrent_shard_requests: int = 12,
    inner_hits_size: int = 50,
) -> list[RelatedWork]:
    # Execute searches
    body = []
    for dmp in dmps:
        body.append({})
        body.append(build_dmp_works_search_query(dmp, max_results, project_end_buffer_years, inner_hits_size))

    responses = client.msearch(
        body=body,
        index=index_name,
        max_concurrent_searches=max_concurrent_searches,
        max_concurrent_shard_requests=max_concurrent_shard_requests,
    )

    # Collate results
    results = []
    for i, response in enumerate(responses["responses"]):
        dmp = dmps[i]
        hits = response.get("hits", {}).get("hits", [])
        max_score = response.get("hits", {}).get("max_score")
        results.extend(collate_results(dmp, hits, max_score))

    return results


def search_dmp_works(
    client: OpenSearch,
    index_name: str,
    dmp: DMPModel,
    max_results: int = 100,
    project_end_buffer_years: int = 3,
    include_named_queries_score: bool = False,
    inner_hits_size: int = 50,
) -> list[RelatedWork]:
    body = build_dmp_works_search_query(dmp, max_results, project_end_buffer_years, inner_hits_size)
    response = client.search(
        body=body,
        index=index_name,
        include_named_queries_score=include_named_queries_score,
    )
    hits = response.get("hits", {}).get("hits", [])
    max_score = response.get("hits", {}).get("max_score")
    return collate_results(dmp, hits, max_score)


def parse_matched_queries(matched_queries: list[str] | dict[str, float]) -> dict[str, float]:
    if isinstance(matched_queries, list):
        return {key: math.nan for key in matched_queries}
    return matched_queries


def collate_results(dmp: DMPModel, hits: list[dict], max_score: float) -> list[RelatedWork]:
    results: list[RelatedWork] = []
    for hit in hits:
        work_doi = hit.get("_id")
        score: float = hit.get("_score", 0.0)
        work = WorkModel.model_validate(hit.get("_source", {}), by_name=True, by_alias=False)
        matched_queries = parse_matched_queries(hit.get("matched_queries", []))
        highlights = hit.get("highlight", {})

        # Construct DOI match
        doi_found = "funded_dois" in matched_queries
        sources = []
        if doi_found:
            for award in dmp.external_data.awards:
                if work_doi in award.funded_dois:
                    parent = award.award_id
                    awards = [parent] + award.award_id.related_awards
                    for child in awards:
                        if child.award_url() is not None:
                            parent_award_id = parent.identifier_string() if parent != child else None
                            sources.append(
                                DoiMatchSource(
                                    parent_award_id=parent_award_id,
                                    award_id=child.identifier_string(),
                                    award_url=child.award_url(),
                                )
                            )
        doi_match = DoiMatch(
            found=doi_found,
            score=matched_queries.get("funded_dois", 0.0),
            sources=sources,
        )

        # Construct content match (based on title and abstract)
        title_highlights = highlights.get("title", [])
        abstract_highlights = highlights.get("abstract_text", [])
        content_score = matched_queries.get("content", 0.0)
        content_matched = "content" in matched_queries
        content_match = ContentMatch(
            score=content_score,
            title_highlight=title_highlights[0] if title_highlights and content_matched else None,
            abstract_highlights=abstract_highlights if content_matched else [],
        )

        # Construct matches based on inner hits
        inner_hits = hit.get("inner_hits", {})
        author_matches = to_item_matches(inner_hits, "authors")
        institution_matches = to_item_matches(inner_hits, "institutions")
        funder_matches = to_item_matches(inner_hits, "funders")
        award_matches = to_item_matches(inner_hits, "awards")

        results.append(
            RelatedWork(
                dmp_doi=dmp.doi,
                work=work,
                score=score,
                score_max=max_score,
                doi_match=doi_match,
                content_match=content_match,
                author_matches=author_matches,
                institution_matches=institution_matches,
                funder_matches=funder_matches,
                award_matches=award_matches,
            )
        )
    return results


def to_item_matches(inner_hits: dict, hit_name: str) -> list[ItemMatch]:
    matches = []
    hits = inner_hits.get(hit_name, {}).get("hits", {}).get("hits", [])
    for hit in hits:
        offset = hit.get("_nested", {}).get("offset")
        score = hit.get("_score")
        matched_queries = parse_matched_queries(hit.get("matched_queries", []))
        fields = [field for field in matched_queries.keys()]
        fields.sort()  # Sort A-Z
        matches.append(
            ItemMatch(
                index=offset,
                score=score,
                fields=fields,
            )
        )
    return matches
