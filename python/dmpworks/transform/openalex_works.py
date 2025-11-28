import logging
import os
import pathlib

import dmpworks.polars_expr_plugin as pe
import polars as pl
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import clean_string, normalise_identifier, normalise_openalex_doi
from dmpworks.transform.utils_file import read_jsonls
from polars._typing import SchemaDefinition

logger = logging.getLogger(__name__)

WORKS_SCHEMA: SchemaDefinition = {
    "id": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#id
    "doi": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#doi
    "ids": pl.Struct(  # https://docs.openalex.org/api-entities/works/work-object#ids
        {
            "doi": pl.String,
            "mag": pl.String,
            "openalex": pl.String,
            "pmid": pl.String,
            "pmcid": pl.String,
        }
    ),
    "title": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#title
    "abstract_inverted_index": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#abstract_inverted_index
    "type": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#type
    "publication_date": pl.Date,  # https://docs.openalex.org/api-entities/works/work-object#publication_date
    "updated_date": pl.Datetime,  # https://docs.openalex.org/api-entities/works/work-object#updated_date
    "authorships": pl.List(  # https://docs.openalex.org/api-entities/works/work-object#authorships
        pl.Struct(
            {
                "author": pl.Struct(
                    {
                        "id": pl.String,
                        "display_name": pl.String,
                        "orcid": pl.String,
                    }
                ),
                "institutions": pl.List(
                    pl.Struct(
                        {
                            "id": pl.String,
                            "display_name": pl.String,
                            "type": pl.String,
                            "ror": pl.String,
                        }
                    )
                ),
            }
        )
    ),
    "grants": pl.List(  # https://docs.openalex.org/api-entities/works/work-object#grants
        pl.Struct(
            {
                "funder": pl.String,
                "funder_display_name": pl.String,
                "award_id": pl.String,
            }
        )
    ),
    "primary_location": pl.Struct(  # https://docs.openalex.org/api-entities/works/work-object#primary_location
        {
            "source": pl.Struct(
                {
                    "display_name": pl.String,
                    "publisher": pl.String,
                }
            )
        }
    ),
}


def normalise_ids(expr: pl.Expr, field_names: list[str]) -> pl.Expr:
    return pl.struct(
        [normalise_identifier(expr.struct.field(field_name)).alias(field_name) for field_name in field_names]
    )


def transform_works(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    lz_cached = lz.cache()

    works = lz_cached.select(
        id=normalise_identifier(pl.col("id")),
        doi=normalise_openalex_doi(pl.col("doi")),
        ids=normalise_ids(pl.col("ids"), ["doi", "mag", "openalex", "pmid", "pmcid"]),
        title=clean_string(pl.col("title")),
        abstract=clean_string(pe.revert_inverted_index(pl.col("abstract_inverted_index"))),
        type=pl.col("type"),
        publication_date=pl.col("publication_date"),  # e.g. 2014-06-04
        updated_date=pl.col("updated_date"),  # e.g. 025-02-27T06:49:42.321119
        publication_venue=pl.col("primary_location").struct.field("source").struct.field("display_name"),
        authors=pl.col("authorships")
        .list.eval(
            pl.struct(
                [
                    normalise_identifier(pl.element().struct.field("author").struct.field("orcid")).alias("orcid"),
                    pe.parse_name(pl.element().struct.field("author").struct.field("display_name")).struct.unnest(),
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
        grants=pl.col("grants")
        .list.eval(
            pl.struct(
                funder_id=normalise_identifier(pl.element().struct.field("funder")),
                funder_display_name=pl.element().struct.field("funder_display_name"),
                award_id=pl.element().struct.field("award_id"),
            )
        )
        .list.eval(
            pl.element().filter(
                pl.any_horizontal(
                    [
                        pl.element().struct.field(field).is_not_null()
                        for field in [
                            "funder_id",
                            "funder_display_name",
                            "award_id",
                        ]
                    ]
                )
            )
        )
        .list.drop_nulls(),
    )

    institutions = (
        lz_cached.select(
            work_id=normalise_identifier(pl.col("id")),
            authorships=pl.col("authorships"),
        )
        .explode("authorships")
        .unnest("authorships")
        .explode("institutions")
        .unnest("institutions")
        .select(
            pl.col("work_id"),
            name=pl.col("display_name"),
            ror=normalise_identifier(pl.col("ror")),
        )
        .filter(pl.any_horizontal([pl.col(field).is_not_null() for field in ["name", "ror"]]))
        .unique(maintain_order=True)
    )
    institutions_by_work = (
        institutions.with_columns(
            inst=pl.struct(
                pl.col("name"),
                pl.col("ror"),
            )
        )
        .group_by("work_id")
        .agg(institutions=pl.col("inst").unique(maintain_order=True))
    )
    inst_dtype = institutions_by_work.collect_schema()["institutions"]
    openalex_works = works.join(institutions_by_work, left_on="id", right_on="work_id", how="left").with_columns(
        institutions=pl.col("institutions").fill_null(pl.lit([]).cast(inst_dtype))
    )

    return [
        ("openalex_works", openalex_works),
    ]


def transform_openalex_works(
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
        schema=WORKS_SCHEMA,
        transform_func=transform_works,
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
