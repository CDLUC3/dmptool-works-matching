import logging
import os
import pathlib

import dmpworks.polars_expr_plugin as pe
import polars as pl
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import clean_string, extract_orcid, normalise_identifier, replace_with_null
from dmpworks.transform.utils_file import read_jsonls
from polars._typing import SchemaDefinition

log = logging.getLogger(__name__)


AWARD_IDS_EXCLUDE = [
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
]

SCHEMA: SchemaDefinition = {
    "dmp_id": pl.String,
    "created": pl.Date,
    "registered": pl.Date,
    "modified": pl.Date,
    "title": pl.String,
    "description": pl.String,
    "project_start": pl.Date,
    "project_end": pl.Date,
    "institutions": pl.List(
        pl.Struct(
            {
                "name": pl.String,
                "ror": pl.String,
            }
        )
    ),
    "authors": pl.List(
        pl.Struct(
            {
                "name": pl.String,
                "orcid": pl.String,
            }
        )
    ),
    "funding": pl.List(
        pl.Struct(
            {
                "funder": pl.Struct(
                    {
                        "name": pl.String,
                        "id": pl.String,
                    }
                ),
                "funding_opportunity_id": pl.String,
                "status": pl.String,
                "grant_id": pl.String,
            }
        )
    ),
}


def dmp_id_to_doi(expr: pl.Expr) -> pl.Expr:
    return (
        pl.when(expr.is_not_null())
        .then(expr.cast(pl.String).str.to_lowercase().str.strip_chars().str.replace("doi.org/", ""))
        .otherwise(None)
    )


def clean_name(expr: pl.Expr) -> pl.Expr:
    cleaned = expr.str.strip_chars()

    return (
        pl.when(expr.is_null())
        .then(None)
        .when(cleaned == "")
        .then(None)
        .when(cleaned.str.contains("@"))
        .then(None)
        .when(cleaned.str.contains(r"^(https?://|www\.)"))
        .then(None)
        .otherwise(cleaned)
    )


def transform(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    dmps = lz.select(
        # dmp_id: strip doi.org/ prefix
        doi=dmp_id_to_doi(pl.col("dmp_id")),
        created=pl.col("created"),
        registered=pl.col("registered"),
        modified=pl.col("modified"),
        # title: strip markup
        title=clean_string(pe.strip_markup(pl.col("title"))),
        # description: strip markup
        abstract=clean_string(pe.strip_markup(pl.col("description"))),
        project_start=pl.col("project_start"),
        project_end=pl.col("project_end"),
        # institutions: strip name and ror
        institutions=pl.col("institutions")
        .list.eval(
            pl.struct(
                name=clean_string(pl.element().struct.field("name")),
                ror=clean_string(pl.element().struct.field("ror")),
            )
        )
        .list.eval(
            pl.element().filter(
                pl.any_horizontal([pl.element().struct.field(field).is_not_null() for field in ["name", "ror"]])
            )
        )
        .list.drop_nulls(),
        # authors: remove empty strings, split into name parts, extract ORCID IDs
        authors=pl.col("authors")
        .list.eval(
            pl.struct(
                [
                    extract_orcid(pl.element().struct.field("orcid")).alias("orcid"),
                    pe.parse_name(pl.element().struct.field("name")).struct.unnest(),
                ]
            )
        )
        .list.eval(
            pl.element().filter(
                pl.any_horizontal(
                    [
                        pl.element().struct.field(field).is_not_null()
                        for field in [
                            "orcid",
                            "first_initial",
                            "given_name",
                            "middle_initials",
                            "middle_names",
                            "surname",
                            "full",
                        ]
                    ]
                )
            )
        )
        .list.drop_nulls(),
        # funding: remove values that are likely not award IDs, e.g. empty strings, na, n/a etc
        funding=pl.col("funding")
        .list.eval(
            pl.struct(
                funder=pl.struct(
                    name=clean_string(
                        pl.element().struct.field("funder").struct.field("name"),
                    ),
                    ror=normalise_identifier(pl.element().struct.field("funder").struct.field("id")),
                ),
                status=pl.element().struct.field("status"),
                funding_opportunity_id=replace_with_null(
                    pl.element().struct.field("funding_opportunity_id"),
                    AWARD_IDS_EXCLUDE,
                ),
                award_id=replace_with_null(
                    pl.element().struct.field("grant_id"),
                    AWARD_IDS_EXCLUDE,
                ),
            )
        )
        .list.eval(
            pl.element().filter(
                pl.any_horizontal(
                    [pl.element().struct.field("funder").struct.field(field).is_not_null() for field in ["ror", "name"]]
                    + [
                        pl.element().struct.field(field).is_not_null()
                        for field in ["funder", "status", "funding_opportunity_id", "award_id"]
                    ]
                )
            )
        )
        .list.drop_nulls(),
    )

    return [
        ("dmps", dmps),
    ]


def transform_dmps(
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
        # Non customizable parameters, specific to DMPs
        schema=SCHEMA,
        transform_func=transform,
        file_glob="**/*jsonl.gz",
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
