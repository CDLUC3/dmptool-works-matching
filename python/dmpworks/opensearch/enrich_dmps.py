from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pendulum
from tqdm import tqdm

from dmpworks.funders.parser import fetch_funded_dois, parse_award_text
from dmpworks.model.dmp_model import Award, ExternalData
from dmpworks.opensearch.dmp_search import yield_dmps
from dmpworks.opensearch.utils import OpenSearchClientConfig, make_opensearch_client

if TYPE_CHECKING:
    from dmpworks.model.common import Institution

log = logging.getLogger(__name__)


def build_enrich_query(
    institutions: list[Institution] | None = None,
    dois: list[str] | None = None,
) -> dict:
    """Build the OpenSearch query for selecting DMPs to enrich.

    Wraps the existing modification-date script filter with optional institution
    and DOI subset filters.

    Args:
        institutions: When supplied only enriches DMPs from these institutions.
        dois: When supplied only enriches DMPs with these DOIs.

    Returns:
        dict: The OpenSearch query body.
    """
    script_filter = {
        "script": {
            "script": {
                "lang": "painless",
                "source": """
                       try {
                           def modifiedExists = doc['modified'].size() > 0;
                           def externalUpdatedExists = doc['external_data.updated'].size() > 0;

                           if (!modifiedExists && !externalUpdatedExists) {
                               return true;
                           } else if (!externalUpdatedExists) {
                               return true;
                           } else if (!modifiedExists) {
                               return false;
                           }

                           long modifiedMillis = doc['modified'].value.toInstant().toEpochMilli();
                           long externalUpdatedMillis = doc['external_data.updated'].value.toInstant().toEpochMilli();

                           return modifiedMillis >= externalUpdatedMillis;

                       } catch (Exception e) {
                           return false;
                       }
                   """,
            }
        }
    }

    subset_filters = []
    if dois:
        subset_filters.append({"ids": {"values": dois}})
    if institutions:
        institution_queries = []
        ror_ids = [inst.ror for inst in institutions if inst.ror]
        if ror_ids:
            institution_queries.append({"terms": {"institutions.ror": ror_ids}})
        names = [inst.name for inst in institutions if inst.name]
        institution_queries.extend(
            {"match_phrase": {"institutions.name": {"query": name, "slop": 3}}} for name in names
        )
        if institution_queries:
            subset_filters.append(
                {
                    "nested": {
                        "path": "institutions",
                        "query": {"bool": {"should": institution_queries, "minimum_should_match": 1}},
                    }
                }
            )

    if subset_filters:
        return {
            "query": {
                "bool": {
                    "must": [script_filter],
                    "filter": {"bool": {"should": subset_filters, "minimum_should_match": 1}},
                }
            }
        }

    return {"query": script_filter}


def enrich_dmps(
    index_name: str,
    client_config: OpenSearchClientConfig,
    page_size: int = 500,
    scroll_time: str = "360m",
    email: str | None = None,
    institutions: list[Institution] | None = None,
    dois: list[str] | None = None,
):
    """Enrich DMPs with additional metadata from external sources.

    Args:
        index_name: The name of the DMPs index.
        client_config: The OpenSearch client configuration.
        page_size: The number of DMPs to process per batch.
        scroll_time: The scroll time for the search context.
        email: The email address to use for external API requests.
        institutions: When supplied only enriches DMPs from these institutions.
        dois: When supplied only enriches DMPs with these DOIs.
    """
    client = make_opensearch_client(client_config)
    query = build_enrich_query(institutions=institutions, dois=dois)

    with (
        tqdm(
            total=0,
            desc="Enrich DMPs in OpenSearch",
            unit="doc",
        ) as pbar,
        yield_dmps(
            client,
            index_name,
            query,
            page_size=page_size,
            scroll_time=scroll_time,
        ) as results,
    ):
        pbar.total = results.total_dmps

        for dmp in results.dmps:
            log.debug(f"Fetch additional metadata for DMP: {dmp.doi}")
            awards = []
            for fund in dmp.funding:
                # Parse Award IDs, which can be found in both funding_opportunity_id
                # and award_id
                award_ids = parse_award_text(fund.funder.ror, fund.funding_opportunity_id)
                award_ids.extend(parse_award_text(fund.funder.ror, fund.award_id))
                award_ids = set(award_ids)

                # Fetch additional data for each award ID
                for award_id in award_ids:
                    funded_dois = fetch_funded_dois(award_id, email=email)
                    awards.append(Award(funder=fund.funder, award_id=award_id, funded_dois=funded_dois))

            log.debug(f"Save additional metadata for DMP: {dmp.doi}")
            external_data = ExternalData(updated=pendulum.now(tz="UTC"), awards=awards).model_dump()
            response = client.update(
                index=index_name,
                id=dmp.doi,
                body={"doc": {"external_data": external_data}},
            )
            result = response.get("result")
            log.debug(f"Result of saving DMP metadata: {dmp.doi} {result}")

            pbar.update(1)
