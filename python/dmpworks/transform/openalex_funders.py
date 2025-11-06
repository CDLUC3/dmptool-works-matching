import logging
import os
import pathlib

import polars as pl
from dmpworks.transform.openalex_works import normalise_ids
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import normalise_identifier
from dmpworks.transform.utils_file import read_jsonls
from polars._typing import SchemaDefinition

logger = logging.getLogger(__name__)

FUNDERS_SCHEMA: SchemaDefinition = {
    "id": pl.String,  # https://docs.openalex.org/api-entities/funders/funder-object#id
    "display_name": pl.String,  # https://docs.openalex.org/api-entities/funders/funder-object#display_name
    "ids": pl.Struct(  # https://docs.openalex.org/api-entities/funders/funder-object#ids
        {
            "crossref": pl.String,
            "doi": pl.String,
            "openalex": pl.String,
            "ror": pl.String,
            "wikidata": pl.String,
        }
    ),
}


def transform_funders(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    funders = lz.select(
        id=normalise_identifier(pl.col("id")),
        display_name=pl.col("display_name"),
        ids=normalise_ids(pl.col("ids"), ["crossref", "doi", "openalex", "ror", "wikidata"]),
        # TODO: remove 'entity/' from start of wikidata
    )

    return [("openalex_funders", funders)]


def transform_openalex_funders(
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    batch_size: int = os.cpu_count(),
    extract_workers: int = 1,
    transform_workers: int = 1,
    cleanup_workers: int = 1,
    extract_queue_size: int = 0,
    transform_queue_size: int = 2,
    cleanup_queue_size: int = 0,
    max_file_processes: int = os.cpu_count(),
    n_batches: int = None,
    low_memory: bool = False,
):
    process_files_parallel(
        # Non customizable parameters, specific to Crossref Metadata
        schema=FUNDERS_SCHEMA,
        transform_func=transform_funders,
        file_glob="**/*.gz",
        read_func=read_jsonls,
        # Customisable parameters
        in_dir=in_dir,
        out_dir=out_dir,
        batch_size=batch_size,
        extract_workers=extract_workers,
        transform_workers=transform_workers,
        cleanup_workers=cleanup_workers,
        extract_queue_size=extract_queue_size,
        transform_queue_size=transform_queue_size,
        cleanup_queue_size=cleanup_queue_size,
        max_file_processes=max_file_processes,
        n_batches=n_batches,
        low_memory=low_memory,
    )
