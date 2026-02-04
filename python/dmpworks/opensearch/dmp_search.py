from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Iterator, Optional

import pendulum
from opensearchpy import OpenSearch

from dmpworks.model.common import Institution
from dmpworks.model.dmp_model import DMPModel
from dmpworks.opensearch.query_builder import build_dmps_query


@contextmanager
def fetch_dmps(
    *,
    client: OpenSearch,
    dmps_index_name: str,
    scroll_time: str,
    page_size: int,
    dois: Optional[list[str]] = None,
    institutions: Optional[list[Institution]] = None,
    start_date: Optional[pendulum.Date] = None,
    end_date: Optional[pendulum.Date] = None,
    inner_hits_size: int = 50,
) -> Generator[ScrollDmps, None, None]:
    query = build_dmps_query(
        dois=dois,
        institutions=institutions,
        start_date=start_date,
        end_date=end_date,
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
    scroll_id: Optional[str] = None

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
                    source = doc['_source']
                    yield DMPModel.model_validate(source)

                # Get next batch
                response = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
                scroll_id = response["_scroll_id"]
                hits = response.get("hits", {}).get("hits", [])

        yield ScrollDmps(total_dmps=total_hits, dmps=dmp_generator())
    finally:
        if scroll_id is not None:
            client.clear_scroll(scroll_id=scroll_id)
