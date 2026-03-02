import logging
import pathlib
from typing import Optional

import pyarrow as pa
import simdjson
from dmpworks.rust import parse_name, ParsedName, strip_markup

from dmpworks.transform.pipeline import process_files
from dmpworks.transform.simdjson_transforms import (
    clean_string,
    extract_doi,
    extract_orcid,
    normalise_identifier,
    parse_iso8601_calendar_date,
    parse_iso8601_datetime,
    to_optional_string,
    parse_author_name,
)
from dmpworks.transform.utils_file import setup_multiprocessing_logging, yield_objects_from_jsonl

logger = logging.getLogger(__name__)


DATACITE_SCHEMA = pa.schema(
    [
        pa.field("doi", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=True),
        pa.field("abstract", pa.string(), nullable=True),
        pa.field("work_type", pa.string(), nullable=True),
        pa.field("publication_date", pa.date32(), nullable=True),  # TODO CHECK
        pa.field("updated_date", pa.timestamp("us"), nullable=True),  # TODO CHECK
        pa.field("publication_venue", pa.string(), nullable=True),
        pa.field(
            "authors",
            pa.list_(
                pa.struct(
                    [
                        pa.field("orcid", pa.string(), nullable=True),
                        pa.field("first_initial", pa.string(), nullable=True),
                        pa.field("given_name", pa.string(), nullable=True),
                        pa.field("middle_initials", pa.string(), nullable=True),
                        pa.field("middle_names", pa.string(), nullable=True),
                        pa.field("surname", pa.string(), nullable=True),
                        pa.field("full", pa.string(), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
        pa.field(
            "institutions",
            pa.list_(
                pa.struct(
                    [
                        pa.field("affiliation_identifier", pa.string(), nullable=True),
                        pa.field("affiliation_identifier_scheme", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("scheme_uri", pa.string(), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
        pa.field(
            "funders",
            pa.list_(
                pa.struct(
                    [
                        pa.field("funder_identifier", pa.string(), nullable=True),
                        pa.field("funder_identifier_type", pa.string(), nullable=True),
                        pa.field("funder_name", pa.string(), nullable=True),
                        pa.field("award_number", pa.string(), nullable=True),
                        pa.field("award_uri", pa.string(), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
        pa.field(
            "relations",
            pa.list_(
                pa.struct(
                    [
                        pa.field("relation_type", pa.string(), nullable=True),
                        pa.field("related_identifier", pa.string(), nullable=True),
                        pa.field("related_identifier_type", pa.string(), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
    ]
)


def parse_datacite_record(obj: simdjson.Object) -> dict | None:
    doi = extract_doi(obj.get("id"))
    attrs = obj.get("attributes")
    title = parse_title(attrs.get("titles", []))
    abstract = parse_abstract(attrs.get("descriptions", []))
    work_type = attrs.get("types", {}).get("resourceTypeGeneral")
    publication_date = parse_iso8601_calendar_date(attrs.get("created"))
    updated_date = parse_iso8601_datetime(attrs.get("updated"))

    publisher = attrs.get("publisher")
    publication_venue = publisher.get("name") if publisher is not None else None
    authors, institutions = parse_authors_and_institutions(attrs.get("creators", []))
    funders = parse_funders(attrs.get("fundingReferences", []))
    relations = parse_relations(attrs.get("relatedIdentifiers", []))

    return dict(
        doi=doi,
        title=title,
        abstract=abstract,
        work_type=work_type,
        publication_date=publication_date,
        updated_date=updated_date,
        publication_venue=publication_venue,
        authors=authors,
        institutions=institutions,
        funders=funders,
        relations=relations,
    )


def parse_title(title_array: Optional[simdjson.Array]) -> Optional[str]:
    for obj in title_array:
        value = obj.get("title")
        if value is not None:
            title = strip_markup(str(value))
            if title is not None:
                return title
    return None


def parse_abstract(description_array: Optional[simdjson.Array]) -> Optional[str]:
    for obj in description_array:
        value = obj.get("description")
        if value is not None:
            abstract = strip_markup(str(value), null_if_equals=[":unav", "Cover title."])
            if abstract is not None:
                return abstract
    return None


def ensure_array_of_objects(obj: object) -> simdjson.Array | list[simdjson.Object]:
    """Some DataCite fields that should be arrays can sometimes be objects and need
    to be converted into arrays of objects with this function."""
    if isinstance(obj, simdjson.Object):
        return [obj]
    elif isinstance(obj, simdjson.Array):
        return obj
    else:
        return []


def parse_orcid(name_identifier_array: simdjson.Array) -> Optional[str]:
    for obj in name_identifier_array:
        # Try to extract ORCID ID
        # Return first valid ORCID ID
        name_identifier = obj.get("nameIdentifier")
        orcid = extract_orcid(name_identifier)
        if orcid is not None:
            return orcid

    return None


def parse_authors_and_institutions(
    creator_array: simdjson.Array | list[simdjson.Object],
) -> tuple[list[dict], list[dict]]:
    authors = []
    authors_seen = set()
    institutions = []
    institutions_seen = set()
    for creator_obj in creator_array:
        name_type = to_optional_string(creator_obj.get("nameType"))
        if name_type == "Personal":
            # Parse authors
            name_identifiers = ensure_array_of_objects(creator_obj.get("nameIdentifiers", []))
            orcid = parse_orcid(name_identifiers)
            first_initial, given_name, middle_initials, middle_names, surname, full = parse_author_name(
                to_optional_string(creator_obj.get("givenName")),
                to_optional_string(creator_obj.get("familyName")),
                full_name=to_optional_string(creator_obj.get("name")),
            )
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
                key = frozenset(author.items())
                if key not in authors_seen:
                    authors_seen.add(key)
                    authors.append(author)

            # Parse institutions
            affiliation_array = ensure_array_of_objects(creator_obj.get("affiliation", []))
            for aff_obj in affiliation_array:
                affiliation_identifier = normalise_identifier(aff_obj.get("affiliationIdentifier"))
                affiliation_identifier_scheme = to_optional_string(aff_obj.get("affiliationIdentifierScheme"))
                name = to_optional_string(aff_obj.get("name"))
                scheme_uri = to_optional_string(aff_obj.get("schemeUri"))
                if any([affiliation_identifier, affiliation_identifier_scheme, name, scheme_uri]):
                    inst = {
                        "affiliation_identifier": affiliation_identifier,
                        "affiliation_identifier_scheme": affiliation_identifier_scheme,
                        "name": name,
                        "scheme_uri": scheme_uri,
                    }
                    key = frozenset(inst.items())
                    if key not in institutions_seen:
                        institutions_seen.add(key)
                        institutions.append(inst)

    return authors, institutions


def parse_funders(funding_reference_array: simdjson.Array | list[simdjson.Object]) -> list[dict]:
    funders = []
    for obj in funding_reference_array:
        funder_identifier = normalise_identifier(obj.get("funderIdentifier"))  # TODO: check ISNI
        funder_identifier_type = to_optional_string(obj.get("funderIdentifierType"))
        funder_name = to_optional_string(obj.get("funderName"))
        award_uri = to_optional_string(obj.get("awardUri"))
        award_numbers = to_optional_string(obj.get("award_number"))
        award_numbers = [] if award_numbers is None else award_numbers.split(",")
        for award_number in award_numbers:
            award_number_clean = clean_string(award_number, lower=False)
            if any([funder_identifier, funder_identifier_type, funder_name, award_number_clean, award_uri]):
                funders.append(
                    {
                        "funder_identifier": funder_identifier,
                        "funder_identifier_type": funder_identifier_type,
                        "funder_name": funder_name,
                        "award_number": award_number_clean,
                        "award_uri": award_uri,
                    }
                )
    return funders


def parse_relations(related_identifier_array: simdjson.Array | list[simdjson.Object]) -> list[dict]:
    relations = []

    for obj in related_identifier_array:
        relation_type = to_optional_string(obj.get("relationType"))
        related_identifier = to_optional_string(obj.get("relatedIdentifier"))
        related_identifier_type = to_optional_string(obj.get("relatedIdentifierType"))

        # Remove url prefix from DOIs
        doi = extract_doi(related_identifier)
        if doi is not None:
            related_identifier = doi
            related_identifier_type = "DOI"
        else:
            related_identifier = normalise_identifier(related_identifier)

        if any([relation_type, related_identifier, related_identifier_type]):
            relations.append(
                {
                    "relation_type": relation_type,
                    "related_identifier": related_identifier,
                    "related_identifier_type": related_identifier_type,
                }
            )
    return relations


def transform_datacite(
    *,
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    batch_size: int,
    row_group_size: int,
    row_groups_per_file: int,
    max_workers: int,
    log_level: int = logging.INFO,
):
    setup_multiprocessing_logging(log_level)
    files = list(in_dir.glob("**/*.jsonl.gz"))
    process_files(
        files=files,
        output_dir=out_dir,
        batch_size=batch_size,
        row_group_size=row_group_size,
        row_groups_per_file=row_groups_per_file,
        schema=DATACITE_SCHEMA,
        read_func=yield_objects_from_jsonl,
        transform_func=parse_datacite_record,
        max_workers=max_workers,
        file_prefix="datacite_",
        tqdm_description="Transforming DataCite",
        log_level=log_level,
    )
