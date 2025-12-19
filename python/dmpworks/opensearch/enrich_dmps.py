import logging
from typing import Optional

import pendulum
from tqdm import tqdm

from dmpworks.funders.parser import fetch_funded_dois, parse_award_text
from dmpworks.model.dmp_model import Award, ExternalData
from dmpworks.opensearch.utils import make_opensearch_client, OpenSearchClientConfig
from dmpworks.opensearch.dmp_search import yield_dmps

log = logging.getLogger(__name__)


def enrich_dmps(
    index_name: str,
    client_config: OpenSearchClientConfig,
    page_size: int = 500,
    scroll_time: str = "360m",
    email: Optional[str] = None,
):
    client = make_opensearch_client(client_config)
    query = {
        "query": {
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
    }

    with tqdm(
        total=0,
        desc="Enrich DMPs in OpenSearch",
        unit="doc",
    ) as pbar:
        with yield_dmps(
            client,
            index_name,
            query,
            page_size=page_size,
            scroll_time=scroll_time,
        ) as results:
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
                        dois = fetch_funded_dois(award_id, email=email)
                        awards.append(Award(funder=fund.funder, award_id=award_id, funded_dois=dois))

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
