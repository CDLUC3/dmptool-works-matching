from __future__ import annotations

import logging
import re
from typing import Optional

import requests
from fold_to_ascii import fold
from rapidfuzz import fuzz

from dmpworks.utils import retry_session
from dmpworks.transform.simdjson_transforms import extract_doi

log = logging.getLogger(__name__)


def nsf_fetch_award_publication_dois(
    award_id: str,
    crossref_threshold: float = 95,
    datacite_threshold: float = 99,
    email: Optional[str] = None,
) -> list[dict]:
    """Fetch publications associated with an NSF award ID.

    Args:
        award_id: the NSF award ID.
        crossref_threshold: the minimum title matching threshold when no DOI is specified and Crossref Metadata is queried.
        datacite_threshold: the minimum title matching threshold when no DOI is specified and DataCite is queried.
        email: email to supply to Crossref Metadata API.

    Returns:
        A list of works.
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
    """Find a matching Crossref DOI for a title and journal using fuzzy similarity.

    Queries the Crossref API for candidate works and returns the DOI of the
    first title whose similarity score meets or exceeds the threshold.

    Args:
        title: The work title.
        journal: The journal name.
        threshold: Minimum similarity score required to accept a match.
        email: Optional email address for Crossref API etiquette.

    Returns:
        The matched DOI, or None if no match is found.
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
                return extract_doi(item.get("DOI"))

        return None
    except requests.exceptions.RequestException as e:
        log.error(f"find_crossref_doi: an error occurred while fetching data: {e}")
        raise


def find_datacite_doi(title: str, threshold: float = 95) -> str | None:
    """Find a matching DataCite DOI for a work title using fuzzy similarity.

    Queries the DataCite API for candidate titles and returns the DOI of the
    first match whose similarity score meets or exceeds the threshold.

    Args:
        title: The work title.
        threshold: Minimum similarity score required to accept a match.

    Returns:
        The matched DOI, or None if no match is found.
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

                return extract_doi(doi)

        return None
    except requests.exceptions.RequestException as e:
        log.error(f"find_datacite_doi: an error occurred while fetching data: {e}")
        raise


def preprocess_text(text) -> str:
    """Preprocess text for similarity matching.

    Converts to lowercase, folds non-ASCII characters to ASCII, removes
    punctuation, and normalizes whitespace.

    Args:
        text: The text to preprocess.

    Returns:
        The preprocessed text.
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

    Args:
        reference: the reference string.

    Returns:
        A dictionary with doi, journal, year, title and reference set.
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
    """Fetch the NSF organization ID for a given award ID.

    Args:
        award_id: The NSF award ID.

    Returns:
        str: The organization ID if found, otherwise None.

    Raises:
        requests.exceptions.RequestException: If the API request fails.
    """
    base_url = "https://www.research.gov/awardapi-service/v1/awards.json"
    params = {"id": award_id}
    org_id = None
    try:
        response = retry_session().get(base_url, params=params)
        data = response.json()
        awards = data.get("response", {}).get("award", [])
        for award in awards:
            # TODO: what if one of these lacked leading zeros?
            if award.get("id") == award_id:
                div_abbr = award.get("divAbbr", "").strip()
                org_id = div_abbr if div_abbr != "" else None
                break

    except requests.exceptions.RequestException as e:
        log.error(f"nsf_fetch_org_id: an error occurred while fetching data: {e}")
        raise

    return org_id
