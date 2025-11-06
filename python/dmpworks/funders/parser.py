from __future__ import annotations

import logging
import re
from typing import Optional, Type

from dmpworks.funders.award_id import AwardID
from dmpworks.funders.nih_award_id import NIHAwardID
from dmpworks.funders.nih_funder_api import nih_fetch_award_publication_dois
from dmpworks.funders.nsf_award_id import NSFAwardID
from dmpworks.funders.nsf_funder_api import nsf_fetch_award_publication_dois

log = logging.getLogger(__name__)


def parse_award_text(funder_id: str, text: str) -> list[AwardID]:
    award_ids = set()
    parser_index: dict[str, Type[AwardID]] = {}
    for id_type in [NIHAwardID, NSFAwardID]:
        for ror_id in id_type.parent_ror_ids:
            parser_index[ror_id] = id_type
    parser = parser_index.get(funder_id)
    if parser:
        if text is not None:
            # Handle cases where multiple awards specified, for example:
            # U19 AI111143; U19 AI111143
            # Lead 2126792, 2126793, 2126794, 2126795, 2126796, 2126797, 2126798, 2126799
            # Then parse each part
            parts = re.split(r"[;,]", text)
            for part in parts:
                award_id = parser.parse(part)
                if award_id is not None:
                    award_ids.add(award_id)
    return list(award_ids)


def fetch_funded_dois(
    award_id: AwardID,
    email: Optional[str] = None,
) -> list[str]:
    log.debug(f"Fetching data for award {award_id.text}")

    log.debug(f"Fetching additional metadata for award {award_id.text}")
    award_id.fetch_additional_metadata()

    canonical_id = award_id.identifier_string()
    log.debug(f"Canonical ID {canonical_id}")

    log.debug(f"Fetching funded works for award {canonical_id}")
    works = []
    # Fetch NIH Award info
    # We only need to get publications for the first award, as all related
    # awards point to the same set of publications
    if isinstance(award_id, NIHAwardID):
        log.debug(f"NIHAwardID fetch works via application IDs")
        awards = [award_id]
        awards.extend(award_id.related_awards)
        for award in awards:
            if award.appl_id is None:
                log.debug(f"Skipping fetching works for {canonical_id} as appl_id is None")
                continue

            log.debug(f"Fetch works for {canonical_id} with appl_id={award.appl_id}")
            results = nih_fetch_award_publication_dois(
                award.appl_id,
                pubmed_api_email=email,
            )
            log.debug(f"Discovered {len(results)} associated works")
            works.extend(results)
            break

    # Fetch NSF award info
    elif isinstance(award_id, NSFAwardID):
        log.debug(f"NSFAwardID fetch works for {canonical_id} with award_id={award_id.award_id}")
        results = nsf_fetch_award_publication_dois(award_id.award_id, email=email)
        log.debug(f"Discovered {len(results)} associated works")
        works.extend(results)

    # Parse and set DOIs for funded works
    funded_works = set()
    for work in works:
        doi = work.get("doi")
        if doi is not None:
            funded_works.add(doi)

    return list(funded_works)
