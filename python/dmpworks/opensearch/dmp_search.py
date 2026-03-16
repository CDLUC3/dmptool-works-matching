from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dmpworks.model.dmp_model import DMPModel
from dmpworks.opensearch.query_builder import build_dmps_query

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

    from opensearchpy import OpenSearch
    import pendulum

    from dmpworks.model.common import Institution


@contextmanager
def fetch_dmps(
    *,
    client: OpenSearch,
    dmps_index_name: str,
    scroll_time: str,
    page_size: int,
    dois: list[str] | None = None,
    institutions: list[Institution] | None = None,
    start_date: pendulum.Date | None = None,
    end_date: pendulum.Date | None = None,
    modified_since: pendulum.Date | None = None,
    inner_hits_size: int = 50,
) -> Generator[ScrollDmps, None, None]:
    """A context manager for fetching DMPs from OpenSearch.

    Args:
        client: The OpenSearch client.
        dmps_index_name: The name of the DMPs index.
        scroll_time: The scroll time for the search context (e.g., "360m").
        page_size: The number of results to return per page.
        dois: A list of DOIs to filter by.
        institutions: A list of institutions to filter by.
        start_date: Return DMPs with project start dates on or after this date.
        end_date: Return DMPs with project start dates on before this date.
        modified_since: Only return DMPs with a modified date on or after this date.
        inner_hits_size: The size of inner hits to return for nested fields.

    Yields:
        ScrollDmps: A container with the total number of DMPs and an iterator over the DMPs.
    """
    query = build_dmps_query(
        dois=dois,
        institutions=institutions,
        start_date=start_date,
        end_date=end_date,
        modified_since=modified_since,
        inner_hits_size=inner_hits_size,
    )

    with yield_dmps(
        client,
        dmps_index_name,
        query,
        page_size=page_size,
        scroll_time=scroll_time,
    ) as results:
        yield results


@dataclass(kw_only=True)
class ScrollDmps:
    """A container for scrolled DMP results.

    Attributes:
        total_dmps: The total number of DMPs found.
        dmps: An iterator over the DMPs.
    """

    total_dmps: str
    dmps: Iterator[DMPModel]


@contextmanager
def yield_dmps(
    client: OpenSearch,
    index_name: str,
    query: dict,
    page_size: int = 500,
    scroll_time: str = "360m",
) -> Generator[ScrollDmps, None, None]:
    """A context manager that yields DMPs from an OpenSearch index using the scroll API.

    This is a lower-level function that `fetch_dmps` is built on.

    Args:
        client: The OpenSearch client.
        index_name: The name of the index to search.
        query: The search query body.
        page_size: The number of results to return per page.
        scroll_time: The scroll time for the search context.

    Yields:
        ScrollDmps: A container with the total number of DMPs and an iterator over the DMPs.
    """
    scroll_id: str | None = None

    try:
        response = client.search(
            index=index_name,
            body=query,
            scroll=scroll_time,
            size=page_size,
            track_total_hits=True,
        )
        scroll_id = response["_scroll_id"]
        total_hits = response.get("hits", {}).get("total", {}).get("value", 0)
        hits = response.get("hits", {}).get("hits", [])

        def dmp_generator():
            nonlocal scroll_id, hits, response

            while hits:
                for doc in hits:
                    source = doc["_source"]
                    yield DMPModel.model_validate(source)

                # Get next batch
                response = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
                scroll_id = response["_scroll_id"]
                hits = response.get("hits", {}).get("hits", [])

        yield ScrollDmps(total_dmps=total_hits, dmps=dmp_generator())
    finally:
        if scroll_id is not None:
            client.clear_scroll(scroll_id=scroll_id)
