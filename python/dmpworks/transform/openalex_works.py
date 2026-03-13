import functools
import logging
import pathlib

import pyarrow as pa
import simdjson

from dmpworks.rust import parse_name, revert_inverted_index, strip_markup
from dmpworks.transform.pipeline import process_files
from dmpworks.transform.simdjson_transforms import (
    clean_string,
    extract_doi,
    extract_orcid,
    normalise_identifier,
    parse_iso8601_calendar_date,
    parse_iso8601_datetime,
    to_optional_string,
)
from dmpworks.utils import setup_multiprocessing_logging, yield_objects_from_jsonl

logger = logging.getLogger(__name__)

OPENALEX_WORKS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("doi", pa.string(), nullable=False),
        pa.field("is_xpac", pa.bool_(), nullable=False),
        pa.field(
            "ids",
            pa.struct(
                [
                    pa.field("doi", pa.string(), nullable=True),
                    pa.field("mag", pa.string(), nullable=True),
                    pa.field("openalex", pa.string(), nullable=True),
                    pa.field("pmid", pa.string(), nullable=True),
                    pa.field("pmcid", pa.string(), nullable=True),
                ]
            ),
            nullable=False,
        ),
        pa.field("title", pa.string(), nullable=True),
        pa.field("abstract", pa.string(), nullable=True),
        pa.field("work_type", pa.string(), nullable=True),
        pa.field("publication_date", pa.date32(), nullable=True),
        pa.field("updated_date", pa.timestamp("us"), nullable=True),
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
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("ror", pa.string(), nullable=True),
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
                        pa.field("id", pa.string(), nullable=True),
                        pa.field("display_name", pa.string(), nullable=True),
                        pa.field("ror", pa.string(), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
        pa.field(
            "awards",
            pa.list_(
                pa.struct(
                    [
                        pa.field("id", pa.string(), nullable=True),
                        pa.field("display_name", pa.string(), nullable=True),
                        pa.field("funder_award_id", pa.string(), nullable=True),
                        pa.field("funder_id", pa.string(), nullable=True),
                        pa.field("funder_display_name", pa.string(), nullable=True),
                        pa.field("doi", pa.string(), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
    ]
)


def parse_openalex_works_record(obj: simdjson.Object, *, include_xpac: bool = False) -> dict | None:
    """Parse an OpenAlex Works record from a simdjson object.

    Args:
        obj: The simdjson object representing an OpenAlex Works record.
        include_xpac: If True, include works flagged as xpac (is_xpac=true).

    Returns:
        Optional[dict]: A dictionary containing the parsed record, or None if parsing fails.
    """
    doi = extract_doi(obj.get("doi"))
    is_xpac = obj.get("is_xpac")

    # Break early if no DOI, or work is xpac and we're not including xpac works
    if doi is None or (is_xpac and not include_xpac):
        return None

    work_id = normalise_identifier(obj.get("id"))
    ids = parse_ids(obj.get("ids"))
    title = parse_title(obj.get("title"))
    abstract = parse_abstract(obj.get("abstract_inverted_index"))
    work_type = to_optional_string(obj.get("type"))
    publication_date = parse_iso8601_calendar_date(obj.get("publication_date"))
    updated_date = parse_iso8601_datetime(obj.get("updated_date"))
    publication_venue = parse_publication_venue(obj.get("primary_location"))
    authors, institutions = parse_authors_and_institutions(obj.get("authorships"))
    funders = parse_funders(obj.get("funders"))
    awards = parse_awards(obj.get("awards"))

    return {
        "id": work_id,
        "doi": doi,
        "is_xpac": is_xpac,
        "ids": ids,
        "title": title,
        "abstract": abstract,
        "work_type": work_type,
        "publication_date": publication_date,
        "updated_date": updated_date,
        "publication_venue": publication_venue,
        "authors": authors,
        "institutions": institutions,
        "funders": funders,
        "awards": awards,
    }


def parse_ids(ids_obj: simdjson.Object | None) -> dict:
    """Parse IDs from an OpenAlex IDs object.

    Args:
        ids_obj: The simdjson object containing IDs.

    Returns:
        dict: A dictionary of parsed IDs.
    """
    ids = {"doi": extract_doi(ids_obj.get("doi"))}
    for key in ["mag", "openalex", "pmid", "pmcid"]:
        value = ids_obj.get(key)
        ids[key] = normalise_identifier(value)
    return ids


def parse_title(text: str | None) -> str | None:
    """Parse and clean the title.

    Args:
        text: The raw title string.

    Returns:
        Optional[str]: The cleaned title, or None.
    """
    if text is not None:
        return strip_markup(str(text))
    return None


def parse_abstract(inverted_index_obj: simdjson.Object | None) -> str | None:
    """Reconstruct the abstract from an inverted index.

    Args:
        inverted_index_obj: The simdjson object representing the inverted index.

    Returns:
        Optional[str]: The reconstructed abstract, or None.
    """
    if inverted_index_obj is None:
        return None
    inverted_index_bytes = inverted_index_obj.mini
    return revert_inverted_index(
        inverted_index_bytes,
        null_if_equals=[
            "a",
            ":unav",
            "No abstract.",
            "Abstract",
            "abstract",
            "peer reviewed",
            "Peer Reviewed",
            "No abstract available.",
            "No description provided.",
            "No abstract available",
            "This article has no abstract.",
            "Abstract not Available.",
            "status: Published",
            "Version of Record",
            "[TODO] Add abstract here.",
            "No abstract",
            "No Abstract.",
            "No abstract is available for this article.",
            "Cover title.",
            "(Abstract to follow)",
            "No contiene resumen",
            "not provided.",
            "No abstract received.",
            "n/a",
            "N/A",
            ".",
            "-",
        ],
    )


def parse_publication_venue(primary_location_obj: simdjson.Object | None) -> str | None:
    """Parse the publication venue from the primary location object.

    Args:
        primary_location_obj: The simdjson object representing the primary location.

    Returns:
        Optional[str]: The publication venue name, or None.
    """
    if primary_location_obj is None:
        return None

    source = primary_location_obj.get("source")
    if source is None:
        return None

    return to_optional_string(source.get("display_name"))


def parse_authors_and_institutions(
    authorships_array: simdjson.Array | list[simdjson.Object],
) -> tuple[list[dict], list[dict]]:
    """Parse authors and institutions from the authorships array.

    Args:
        authorships_array: The simdjson array of authorships.

    Returns:
        tuple[list[dict], list[dict]]: A tuple containing a list of authors and a list of institutions.
    """
    authors = []
    authors_seen = set()
    institutions = []
    institutions_seen = set()

    for obj in authorships_array:
        # Parse author
        author = obj.get("author")
        author_orcid = extract_orcid(author.get("orcid"))
        author_full_name = to_optional_string(author.get("display_name"))
        first_initial, given_name, middle_initials, middle_names, surname, full = parse_name(raw_full=author_full_name)
        if any([author_orcid, first_initial, given_name, middle_initials, middle_names, surname, full]):
            author = {
                "orcid": author_orcid,
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
        author_institutions = obj.get("institutions", [])
        for inst_obj in author_institutions:
            inst_name = to_optional_string(inst_obj.get("display_name"))
            inst_ror = normalise_identifier(inst_obj.get("ror"))
            if any([inst_name, inst_ror]):
                inst = {
                    "name": inst_name,
                    "ror": inst_ror,
                }
                key = frozenset(inst.items())
                if key not in institutions_seen:
                    institutions_seen.add(key)
                    institutions.append(inst)

    return authors, institutions


def parse_funders(funders_array: simdjson.Array | list[simdjson.Object]) -> list[dict]:
    """Parse funders from the funders array.

    Args:
        funders_array: The simdjson array of funders.

    Returns:
        list[dict]: A list of parsed funders.
    """
    funders = []
    for obj in funders_array:
        funder_id = normalise_identifier(obj.get("id"))
        display_name = clean_string(obj.get("display_name"), lower=False)
        ror = normalise_identifier(obj.get("ror"))
        if any([funder_id, display_name, ror]):
            funders.append(
                {
                    "id": funder_id,
                    "display_name": display_name,
                    "ror": ror,
                }
            )
    return funders


def parse_awards(awards_array: simdjson.Array | list[simdjson.Object]) -> list[dict]:
    """Parse awards from the awards array.

    Args:
        awards_array: The simdjson array of awards.

    Returns:
        list[dict]: A list of parsed awards.
    """
    awards = []
    for obj in awards_array:
        award_id = normalise_identifier(obj.get("id"))
        display_name = clean_string(obj.get("display_name"), lower=False)
        funder_id = normalise_identifier(obj.get("funder_id"))
        funder_display_name = clean_string(obj.get("funder_display_name"), lower=False)
        doi = extract_doi(obj.get("doi"))

        raw_awards_value = to_optional_string(obj.get("funder_award_id"))
        raw_awards = raw_awards_value.split(",") if raw_awards_value is not None else []
        for raw_award in raw_awards:
            funder_award_id = clean_string(raw_award, lower=False)
            if any([award_id, display_name, funder_award_id, funder_id, funder_display_name, doi]):
                awards.append(
                    {
                        "id": award_id,
                        "display_name": display_name,
                        "funder_award_id": funder_award_id,
                        "funder_id": funder_id,
                        "funder_display_name": funder_display_name,
                        "doi": doi,
                    }
                )
    return awards


def transform_openalex_works(
    *,
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    batch_size: int,
    row_group_size: int,
    row_groups_per_file: int,
    max_workers: int,
    include_xpac: bool = False,
    log_level: int = logging.INFO,
):
    """Transform OpenAlex Works JSONL files to Parquet format.

    Args:
        in_dir: Input directory containing OpenAlex Works JSONL files.
        out_dir: Output directory for Parquet files.
        batch_size: Number of files to process in a batch.
        row_group_size: Number of rows per row group in Parquet files.
        row_groups_per_file: Number of row groups per Parquet file.
        max_workers: Maximum number of worker processes.
        include_xpac: If True, include works flagged as xpac (is_xpac=true).
        log_level: Logging level.
    """
    setup_multiprocessing_logging(log_level)
    files = list(in_dir.glob("**/*.gz"))
    process_files(
        files=files,
        output_dir=out_dir,
        batch_size=batch_size,
        row_group_size=row_group_size,
        row_groups_per_file=row_groups_per_file,
        schema=OPENALEX_WORKS_SCHEMA,
        read_func=yield_objects_from_jsonl,
        transform_func=functools.partial(parse_openalex_works_record, include_xpac=include_xpac),
        max_workers=max_workers,
        file_prefix="openalex_works_",
        tqdm_description="Transforming OpenAlex Works",
        log_level=log_level,
    )
