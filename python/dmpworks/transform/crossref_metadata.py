import logging
import pathlib
from typing import Optional

import pyarrow as pa
import simdjson
from dmpworks.rust import strip_markup

from dmpworks.transform.pipeline import process_files
from dmpworks.transform.simdjson_transforms import (
    clean_string,
    extract_doi,
    normalise_identifier,
    parse_iso8601_datetime,
    to_optional_string,
)
from dmpworks.transform.utils_file import setup_multiprocessing_logging, yield_objects_from_jsonl

logger = logging.getLogger(__name__)


CROSSREF_METADATA_SCHEMA = pa.schema(
    [
        pa.field("doi", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=True),
        pa.field("abstract", pa.string(), nullable=True),
        pa.field("updated_date", pa.timestamp("us"), nullable=True),
        pa.field(
            "funders",
            pa.list_(
                pa.struct(
                    [
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("funder_doi", pa.string(), nullable=True),
                        pa.field("award", pa.string(), nullable=True),
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
                        pa.field("relation_id", pa.string(), nullable=True),
                        pa.field("id_type", pa.string(), nullable=True),
                        pa.field("asserted_by", pa.string(), nullable=True),
                    ]
                )
            ),
            nullable=False,
        ),
    ]
)


def parse_crossref_metadata_record(obj: simdjson.Object) -> dict | None:
    doi = extract_doi(obj.get("DOI"))
    title = parse_title(obj.get("title", []))
    abstract = parse_abstract(obj.get("abstract"))
    updated_date = parse_updated_date(obj.get("deposited"))
    funders = parse_funders(obj.get("funder", []))
    relations = parse_relations(obj.get("relation", {}))

    return dict(
        doi=doi,
        title=title,
        abstract=abstract,
        updated_date=updated_date,
        funders=funders,
        relations=relations,
    )


def parse_title(title_array: Optional[simdjson.Array]) -> Optional[str]:
    for obj in title_array:
        if obj is not None:
            title = strip_markup(str(obj))
            if title is not None:
                return title
    return None


def parse_abstract(text: Optional[str]) -> Optional[str]:
    if text is not None:
        return strip_markup(str(text))
    return None


def parse_updated_date(date_time_obj: simdjson.Object):
    # https://github.com/crossref/rest-api-doc/blob/master/api_format.md see deposited
    date_time = date_time_obj.get("date-time")
    return parse_iso8601_datetime(date_time)


def parse_funders(funder_array: simdjson.Array) -> list[dict]:
    funders = []
    for obj in funder_array:
        funder_doi = extract_doi(obj.get("DOI"))
        funder_name = to_optional_string(obj.get("name"))
        raw_awards = [
            part for raw_award in obj.get("award", []) if raw_award is not None for part in str(raw_award).split(",")
        ]
        for raw_award in raw_awards:
            award = clean_string(raw_award, lower=False)
            if any([funder_doi, funder_name, award]):
                funders.append(
                    {
                        "name": funder_name,
                        "funder_doi": funder_doi,
                        "award": award,
                    }
                )
    return funders


def parse_relations(relation_obj: simdjson.Object) -> list[dict]:
    relations = []

    for relation_type, sub_relation_array in relation_obj.items():
        for obj in sub_relation_array:
            relation_id = normalise_identifier(obj.get("id"))
            id_type = to_optional_string(obj.get("id-type"))
            asserted_by = to_optional_string(obj.get("asserted-by"))

            if any([relation_type, relation_id, id_type, asserted_by]):
                relations.append(
                    {
                        "relation_type": relation_type,
                        "relation_id": relation_id,
                        "id_type": id_type,
                        "asserted_by": asserted_by,
                    }
                )

    return relations


def transform_crossref_metadata(
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
        schema=CROSSREF_METADATA_SCHEMA,
        read_func=yield_objects_from_jsonl,
        transform_func=parse_crossref_metadata_record,
        max_workers=max_workers,
        file_prefix="crossref_metadata_",
        tqdm_description="Transforming Crossref Metadata",
        log_level=log_level,
    )
