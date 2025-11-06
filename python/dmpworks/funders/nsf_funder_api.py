from __future__ import annotations

import logging
import re
from typing import Optional

import requests
from bs4 import BeautifulSoup
from fold_to_ascii import fold
from rapidfuzz import fuzz

from dmpworks.transforms import clean_string, extract_doi
from dmpworks.utils import retry_session

log = logging.getLogger(__name__)


def nsf_fetch_award_publication_dois(
    award_id: str,
    crossref_threshold: float = 95,
    datacite_threshold: float = 99,
    email: Optional[str] = None,
) -> list[dict]:
    """Fetch publications associated with an NSF award ID.

    :param award_id: the NSF award ID.
    :param crossref_threshold: the minimum title matching threshold when no DOI is specified and Crossref Metadata is queried.
    :param datacite_threshold: the minimum title matching threshold when no DOI is specified and DataCite is queried.
    :param email: email to supply to Crossref Metadata API.
    :return: a list of works.
    """

    base_url = "https://www.research.gov/awardapi-service/v1/awards.json"
    params = {"id": award_id, "printFields": "publicationResearch"}

    try:
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Collect references
        references = []
        awards = data.get("response", {}).get("award", [])
        for award in awards:
            publication_research = award.get("publicationResearch", [])
            references.extend(publication_research)

        # Parse references
        references = [parse_reference(ref) for ref in references]

        # Attempt to find missing DOIs from Crossref Metadata and DataCite
        for ref in references:
            doi = ref.get("doi")
            title = ref.get("title")
            journal = ref.get("journal")

            # Try Crossref Metadata first
            if doi is None:
                doi = find_crossref_doi(
                    title,
                    journal,
                    threshold=crossref_threshold,
                    email=email,
                )

            # If DOI is still None, then try DataCite
            if doi is None:
                doi = find_datacite_doi(title, threshold=datacite_threshold)

            ref["doi"] = doi

        return references

    except requests.exceptions.RequestException as e:
        log.error(f"nsf_fetch_award_publication_dois: an error occurred while fetching data: {e}")
        raise


def find_crossref_doi(
    title: str,
    journal: str,
    threshold: float = 95,
    email: Optional[str] = None,
) -> str | None:
    """Given an academic work title and a journal name, fetch a list of candidate DOIs from Crossref Metadata
    and accept the title with a similarity greater than or equal to the threshold.

    :param title: the title.
    :param journal: the journal name.
    :param threshold: the minimum threshold for accepting a match.
    :param email: email address.
    :return: the DOI or None.
    """

    base_url = "https://api.crossref.org/works"
    params = {"query.title": title, "query.container-title": journal}
    if email is not None:
        params["mailto"] = email

    try:
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()

        data = response.json()
        items = data.get("message", {}).get("items", [])

        for item in items:
            # Get title for item
            item_title = item.get("title")
            item_title = item_title[0] if isinstance(item_title, list) and item_title else ""

            # Accept title if similarity >= threshold
            if fuzz.ratio(title, item_title, processor=preprocess_text) >= threshold:
                return clean_string(item.get("DOI"))

        return None
    except requests.exceptions.RequestException as e:
        log.error(f"find_crossref_doi: an error occurred while fetching data: {e}")
        raise


def find_datacite_doi(title: str, threshold: float = 95) -> str | None:
    """Given an academic work title, fetch a list of candidate DOIs from DataCite
    and accept the title with a similarity greater than or equal to the threshold.

    :param title: the title.
    :param threshold: the minimum threshold for accepting a match.
    :return: the DOI or None.
    """

    base_url = "https://api.datacite.org/dois"
    title_quoted = title.replace('"', '\\"')
    params = {"query": f'titles.title:"{title_quoted}"', "sort": "relevance"}
    try:
        response = retry_session().get(base_url, params=params)
        response.raise_for_status()

        data = response.json()
        items = data.get("data")
        for item in items:
            doi = item.get("id")
            attributes = item.get("attributes", {})

            # Get title for item
            item_titles = attributes.get("titles", [])  # Example: titles [{'title': 'COKI Open Access Dataset'}]
            item_title = item_titles[0].get("title") if item_titles else ""

            # Accept title if similarity >= threshold
            if fuzz.ratio(title, item_title, processor=preprocess_text) >= threshold:
                # Check to see if there is a root version of record
                # Example: relatedIdentifiers [{'relatedIdentifier': '10.5281/zenodo.6399462', 'relatedIdentifierType': 'DOI', 'relationType': 'IsVersionOf'}, {'relatedIdentifier': 'https://zenodo.org/communities/coki', 'relatedIdentifierType': 'URL', 'relationType': 'IsPartOf'}]
                related_identifiers = attributes.get("relatedIdentifiers")
                for related in related_identifiers:
                    related_identifier = related.get("relatedIdentifier")
                    if related.get("relationType") == "IsVersionOf" and related_identifier:
                        doi = related_identifier
                        break

                return clean_string(doi)

        return None
    except requests.exceptions.RequestException as e:
        log.error(f"find_datacite_doi: an error occurred while fetching data: {e}")
        raise


def preprocess_text(text) -> str:
    """Pre-process text before similarity matching. Converts to lower case, folds non-ASCII characters into ASCII
    characters, removes punctuation and normalises spaces.

    :param text: the text to preprocess.
    :return: the preprocessed text.
    """

    text = text.lower()  # Convert to lowercase
    text = fold(text)  # Fold non-ASCII characters into ASCII
    text = re.sub(
        r"[^\w\s]", "", text
    )  # Remove punctuation: replaces any character that is not a word or whitespace with ""
    text = re.sub(
        r"\s+", " ", text
    ).strip()  # Normalise spaces by replacing whitespace character with one or more occurrences with a single space
    return text


def parse_reference(reference: str) -> dict:
    """Parse an NSF award publication reference.

    :param reference: the reference string.
    :return: a dictionary with doi, journal, year, title and reference set.
    """

    # Split on ~
    parts = reference.split("~")

    # Journal: always first
    journal = parts[0].strip() if len(parts) > 0 else None

    # Year: always second
    year = parts[1].strip() if len(parts) > 1 else None

    # Parse DOI and title in reverse
    doi = None
    title = None
    parts.reverse()
    for i, part in enumerate(parts):
        # Title is always preceded by a date or the N character
        if title is None and i >= 1 and part.strip().lower() != "n":
            title = part

        # Check every part for a DOI
        if doi is None:
            doi = extract_doi(part)

        # Break early if both found
        if title is not None and doi is not None:
            break

    return dict(
        doi=doi,
        journal=journal,
        year=year,
        title=title,
        reference=reference,
    )


def nsf_fetch_org_id(award_id: str):
    base_url = "https://www.nsf.gov/awardsearch/showAward"
    params = {"AWD_ID": award_id}
    org_id = None
    try:
        # Fetch page
        response = retry_session(raise_on_status=False).get(base_url, params=params)

        # Currently returning 500 error if the Award ID doesn't exist (instead
        # of 404).
        if response.status_code == 500:
            return None

        response.raise_for_status()

        # Parse page
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table row containing "NSF Org:"
        nsf_org_row = soup.find("td", text="NSF Org:")

        if nsf_org_row:
            # Find the next sibling <td> that contains the actual NSF Org ID
            nsf_org_td = nsf_org_row.find_next_sibling("td")

            if nsf_org_td:
                # Extract the NSF Org ID from the <a> tag or direct text
                nsf_org_id = nsf_org_td.find("a").text if nsf_org_td.find("a") else nsf_org_td.text
                org_id = nsf_org_id.strip().upper()
                log.info(f"nsf_fetch_org_id: found NSF Org ID {org_id} for Award ID {award_id}")
            else:
                log.info(f"nsf_fetch_org_id: no NSF Org ID found for Award ID {award_id}")

    except requests.exceptions.RequestException as e:
        log.error(f"nsf_fetch_org_id: an error occurred while fetching data: {e}")
        raise

    return org_id
