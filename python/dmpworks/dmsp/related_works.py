"""Functions to extract related works and work versions from match data Parquet files."""

import json
import logging
import pathlib

import pyarrow as pa
from tqdm import tqdm

from dmpworks.dmsp.utils import serialise_json
from dmpworks.utils import ParquetBatchWriter, read_parquet_files

log = logging.getLogger(__name__)

WORK_VERSIONS_SCHEMA = pa.schema(
    [
        pa.field("doi", pa.string()),
        pa.field("hash", pa.string()),
        pa.field("workType", pa.string()),
        pa.field("publicationDate", pa.string()),
        pa.field("title", pa.string()),
        pa.field("abstractText", pa.string()),
        pa.field("authors", pa.string()),
        pa.field("institutions", pa.string()),
        pa.field("funders", pa.string()),
        pa.field("awards", pa.string()),
        pa.field("publicationVenue", pa.string()),
        pa.field("sourceName", pa.string()),
        pa.field("sourceUrl", pa.string()),
    ]
)

RELATED_WORKS_SCHEMA = pa.schema(
    [
        pa.field("planId", pa.string()),
        pa.field("dmpDoi", pa.string()),
        pa.field("workDoi", pa.string()),
        pa.field("hash", pa.string()),
        pa.field("sourceType", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("scoreMax", pa.float64()),
        pa.field("doiMatch", pa.string()),
        pa.field("contentMatch", pa.string()),
        pa.field("authorMatches", pa.string()),
        pa.field("institutionMatches", pa.string()),
        pa.field("funderMatches", pa.string()),
        pa.field("awardMatches", pa.string()),
    ]
)


def json_work_to_work_version(work: dict) -> dict:
    """Convert a JSON work dictionary to a work version dictionary.

    Args:
        work: Dictionary containing work data.

    Returns:
        A dictionary representing the work version.
    """
    return {
        "doi": work["doi"],
        "hash": work["hash"],
        "workType": work["workType"],
        "publicationDate": work["publicationDate"],
        "title": work["title"],
        "abstractText": work["abstractText"],
        "authors": serialise_json(work["authors"]),
        "institutions": serialise_json(work["institutions"]),
        "funders": serialise_json(work["funders"]),
        "awards": serialise_json(work["awards"]),
        "publicationVenue": work["publicationVenue"],
        "sourceName": work["source"]["name"],
        "sourceUrl": work["source"]["url"],
    }


def create_work_versions(
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    *,
    row_group_size: int = 50_000,
    row_groups_per_file: int = 4,
):
    """Extract unique work versions from input Parquet directory and write to output directory.

    Args:
        input_dir: Directory containing input Parquet match data files.
        output_dir: Directory to write work version Parquet files into.
        row_group_size: Number of rows per Parquet row group.
        row_groups_per_file: Number of row groups per output file before rotating.
    """
    seen = set()
    with ParquetBatchWriter(
        output_dir=output_dir,
        schema=WORK_VERSIONS_SCHEMA,
        row_group_size=row_group_size,
        row_groups_per_file=row_groups_per_file,
    ) as writer:
        for row in tqdm(read_parquet_files([input_dir]), desc="Extracting work versions: ", unit="docs"):
            work = json.loads(row["work"])
            doi = work["doi"]
            if doi not in seen:
                writer.write_rows([json_work_to_work_version(work)])
                seen.add(doi)
    log.info("Extracted %d unique work versions.", len(seen))


def create_related_works(
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    *,
    row_group_size: int = 50_000,
    row_groups_per_file: int = 4,
):
    """Extract related works from input Parquet directory and write to output directory.

    Args:
        input_dir: Directory containing input Parquet match data files.
        output_dir: Directory to write related work Parquet files into.
        row_group_size: Number of rows per Parquet row group.
        row_groups_per_file: Number of row groups per output file before rotating.
    """
    count = 0
    with ParquetBatchWriter(
        output_dir=output_dir,
        schema=RELATED_WORKS_SCHEMA,
        row_group_size=row_group_size,
        row_groups_per_file=row_groups_per_file,
    ) as writer:
        for row in tqdm(read_parquet_files([input_dir]), desc="Extracting related works: ", unit="docs"):
            work = json.loads(row["work"])
            writer.write_rows(
                [
                    {
                        "planId": None,
                        "dmpDoi": row["dmpDoi"],
                        "workDoi": work["doi"],
                        "hash": work["hash"],
                        "sourceType": "SYSTEM_MATCHED",
                        "score": row["score"],
                        "scoreMax": row["scoreMax"],
                        "doiMatch": row["doiMatch"],
                        "contentMatch": row["contentMatch"],
                        "authorMatches": row["authorMatches"],
                        "institutionMatches": row["institutionMatches"],
                        "funderMatches": row["funderMatches"],
                        "awardMatches": row["awardMatches"],
                    }
                ]
            )
            count += 1
    log.info("Extracted %d related works.", count)
