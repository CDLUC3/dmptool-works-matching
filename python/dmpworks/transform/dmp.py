import json
import logging
import re
from typing import Optional

from dmpworks.rust import parse_name, ParsedName, strip_markup

from dmpworks.model.dmp_model import DMPModel
from dmpworks.transform.simdjson_transforms import (
    clean_string,
    extract_doi,
    extract_orcid,
    extract_ror,
    parse_author_name,
    parse_iso8601_calendar_date,
    replace_with_null,
)

log = logging.getLogger(__name__)


AWARD_IDS_EXCLUDE = {
    "",
    "-",
    "0",
    "0000",
    "001",
    "1",
    "12",
    "123",
    "1234",
    "12345",
    "123456",
    "12345678",
    "123456789",
    "123457",
    "None",
    "abc123",
    "abcdef",
    "em elaboração",
    "independent departmental funds",
    "internally funded",
    "n/a",
    "na",
    "na.com",
    "nil",
    "no aplica",
    "no grat numbered yet",
    "no numbered yet",
    "not applicable",
    "not assigned",
    "not yet assigned",
    "pending",
    "sem numero",
    "sem numeros",
    "tbd",
    "unspecified",
    "xxxxxxxxxxxxxxxxx",
}


def transform_dmp(obj: dict) -> Optional[DMPModel]:
    doi = parse_doi(obj.get("doi"))

    # Convert JSON lists into Python lists of dicts
    obj["authors"] = json.loads(obj["authors"])
    obj["institutions"] = json.loads(obj["institutions"])
    obj["funding"] = json.loads(obj["funding"])
    obj["published_outputs"] = json.loads(obj["published_outputs"])

    created = obj.get("created")
    registered = obj.get("registered")
    modified = obj.get("modified")
    title = strip_markup(obj.get("title"))
    abstract_text = strip_markup(obj.get("abstract_text"))
    project_start = parse_iso8601_calendar_date(obj.get("project_start"))
    project_end = parse_iso8601_calendar_date(obj.get("project_end"))
    institutions = parse_institutions(obj.get("institutions"))
    authors = parse_authors(obj.get("authors"))
    funding = parse_funding(obj.get("funding"))
    published_outputs = parse_published_outputs(obj.get("published_outputs"))

    dmp_dict = {
        "doi": doi,
        "created": created,
        "registered": registered,
        "modified": modified,
        "title": title,
        "abstract_text": abstract_text,
        "project_start": project_start,
        "project_end": project_end,
        "institutions": institutions,
        "authors": authors,
        "funding": funding,
        "published_outputs": published_outputs,
    }

    return DMPModel.model_validate(
        dmp_dict,
        by_name=True,
        by_alias=False,
    )


def parse_doi(obj: Optional[str]) -> Optional[str]:
    if obj is None:
        return None

    # Try to extract DOI
    doi = extract_doi(obj)
    if doi is not None:
        return doi

    # Fallback  to strip protocol, domain, and any leading slashes
    cleaned = re.sub(r'^https?://(?:doi\.org/)?', '', clean_string(obj, lower=True))

    # Add DOI prefix if doesn't exist
    if not cleaned.startswith('10.'):
        cleaned = f'10.48321/{cleaned}'

    # Try to extract DOI
    return extract_doi(cleaned)


def parse_institutions(objects: list[dict]) -> list[dict]:
    institutions = []
    seen = set()

    # Sort by name
    objects = sorted(objects, key=lambda x: (x.get("name") is None, x.get("name") or ""))

    for obj in objects:
        name = clean_string(obj.get("name"), lower=False)
        ror = extract_ror(obj.get("affiliation_id"))
        if any([name, ror]):
            inst = {
                "name": name,
                "ror": ror,
            }
            key = frozenset(inst.items())
            if key not in seen:
                seen.add(key)
                institutions.append(inst)

    return institutions


def parse_authors(objects: list[dict]) -> list[dict]:
    authors = []
    seen = set()

    # Sort by is primary contact then created date
    objects = sorted(
        objects, key=lambda x: (not x.get("is_primary_contact"), x.get("created") is None, x.get("created") or "")
    )

    for obj in objects:
        orcid = extract_orcid(obj.get("orcid"))
        given = clean_string(obj.get("given_name"), lower=False)
        family = clean_string(obj.get("surname"), lower=False)
        first_initial, given_name, middle_initials, middle_names, surname, full = parse_author_name(given, family)
        if any([orcid, first_initial, given_name, middle_initials, middle_names, surname, full]):
            author = {
                "orcid": orcid,
                "first_initial": first_initial,
                "given_name": given_name,
                "middle_initials": middle_initials,
                "middle_names": middle_names,
                "surname": surname,
                "full": full,
            }
            if any([orcid, first_initial, given_name, middle_initials, middle_names, surname, full]):
                key = frozenset(author.items())
                if key not in seen:
                    seen.add(key)
                    authors.append(author)

    return authors


def parse_funding(objects: list[dict]) -> list[dict]:
    funding = []
    seen = set()

    # Sort by created date
    objects = sorted(objects, key=lambda x: (x.get("created") is None, x.get("created") or ""))

    for obj in objects:
        funder_name = clean_string(obj.get("funder_name"), lower=False)
        funder_ror = extract_ror(obj.get("funder_id"))
        status = obj.get("status")
        funding_opportunity_id = replace_with_null(obj.get("funder_opportunity_id"), AWARD_IDS_EXCLUDE)
        award_id = replace_with_null(obj.get("grant_id"), AWARD_IDS_EXCLUDE)
        funder = {
            "funder": {
                "name": funder_name,
                "ror": funder_ror,
            },
            "status": status,
            "funding_opportunity_id": funding_opportunity_id,
            "award_id": award_id,
        }

        if any([funder_name, funder_ror, funding_opportunity_id, award_id]):
            key = (funder_name, funder_ror, status, funding_opportunity_id, award_id)
            if key not in seen:
                seen.add(key)
                funding.append(funder)

    return funding


def parse_published_outputs(objects: list[dict]) -> list[dict]:
    outputs = []
    for obj in objects:
        doi = extract_doi(obj.get("doi"))
        if doi is not None:
            outputs.append({"doi": doi})
    outputs.sort(key=lambda x: x.get("doi"))
    return outputs
