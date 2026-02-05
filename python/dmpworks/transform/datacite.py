import logging
import os
import pathlib

import dmpworks.polars_expr_plugin as pe

import polars as pl
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import (
    clean_string,
    extract_orcid,
    normalise_datacite_doi,
    normalise_identifier,
    remove_markup,
    replace_with_null,
)
from dmpworks.transform.utils_file import extract_gzip, read_jsonls
from polars import Date
from polars._typing import SchemaDefinition

logger = logging.getLogger(__name__)

AFFILIATION_SCHEMA = pl.List(
    pl.Struct(
        {
            "name": pl.String,
            "affiliationIdentifier": pl.String,
            "affiliationIdentifierScheme": pl.String,
            "schemeUri": pl.String,
        }
    )
)
NAME_IDENTIFIERS_SCHEMA = pl.List(
    pl.Struct(
        {
            "nameIdentifier": pl.String,
            "nameIdentifierScheme": pl.String,
            "schemeUri": pl.String,
        }
    )
)
CREATOR_OR_CONTRIBUTOR = pl.Struct(
    {
        "givenName": pl.String,
        "familyName": pl.String,
        "name": pl.String,
        "nameType": pl.String,
        "affiliation": pl.String,  # Should all be lists, however some of these are objects
        # "affiliation": pl.List(
        #     pl.Struct(
        #         {
        #             "name": pl.String,
        #             "affiliationIdentifier": pl.String,
        #             "affiliationIdentifierScheme": pl.String,
        #             "schemeUri": pl.String,
        #         }
        #     )
        # ),
        "nameIdentifiers": pl.String,  # Should all be lists, however some of these are objects
        # "nameIdentifiers": pl.List(
        #     pl.Struct(
        #         {
        #             "nameIdentifier": pl.String,
        #             "nameIdentifierScheme": pl.String,
        #             "schemeUri": pl.String,
        #         }
        #     )
        # ),
    }
)

SCHEMA: SchemaDefinition = {
    "id": pl.String,
    "attributes": pl.Struct(
        {
            "created": pl.String,  # ISO 8601 string
            "updated": pl.String,  # ISO 8601 string
            "titles": pl.List(pl.Struct({"title": pl.String})),
            "descriptions": pl.List(pl.Struct({"description": pl.String})),
            "types": pl.Struct({"resourceTypeGeneral": pl.String}),
            "container": pl.Struct(
                {
                    "title": pl.String,
                }
            ),
            "creators": pl.List(CREATOR_OR_CONTRIBUTOR),
            "fundingReferences": pl.List(
                pl.Struct(
                    {
                        "funderIdentifier": pl.String,
                        "funderIdentifierType": pl.String,
                        "funderName": pl.String,
                        "awardNumber": pl.String,
                        "awardUri": pl.String,
                    }
                )
            ),
            "publisher": pl.Struct(
                {
                    "name": pl.String,
                }
            ),
            "relatedIdentifiers": pl.List(  # https://support.datacite.org/docs/connecting-to-works
                pl.Struct(
                    {"relationType": pl.String, "relatedIdentifier": pl.String, "relatedIdentifierType": pl.String}
                )
            ),
        }
    ),
}


def process_author_name(given_name: pl.Expr, family_name: pl.Expr, name: pl.Expr) -> pl.Expr:
    return (
        pl.when(name.str.strip_chars().str.len_bytes() > 0)
        .then(pe.parse_name(name))
        .when((given_name.str.strip_chars().str.len_bytes() > 0) | (family_name.str.strip_chars().str.len_bytes() > 0))
        .then(
            pe.parse_name(
                pl.concat_str(
                    [given_name.str.strip_chars(), family_name.str.strip_chars()],
                    separator=" ",
                    ignore_nulls=True,
                )
            )
        )
        .otherwise(
            pl.struct(
                first_initial=pl.lit(None, dtype=pl.Utf8),
                given_name=pl.lit(None, dtype=pl.Utf8),
                middle_initials=pl.lit(None, dtype=pl.Utf8),
                middle_names=pl.lit(None, dtype=pl.Utf8),
                surname=pl.lit(None, dtype=pl.Utf8),
                full=pl.lit(None, dtype=pl.Utf8),
            )
        )
    )


def process_orcid(expr: pl.Expr) -> pl.Expr:
    name_identifiers = pe.parse_datacite_name_identifiers(
        pl.coalesce(
            expr,
            pl.lit("", dtype=pl.Utf8),
        )
    )
    first_match = (
        pl.coalesce(name_identifiers, pl.lit([], dtype=NAME_IDENTIFIERS_SCHEMA))
        .list.filter(pl.element().struct.field("nameIdentifierScheme").str.contains("(?i)orc"))
        .list.first()
    )
    name_identifier = (
        pl.when(first_match.is_not_null())
        .then(
            first_match.struct.field("nameIdentifier"),
        )
        .otherwise(pl.lit(None))
    )

    return extract_orcid(name_identifier)


def transform(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    lz_cached = lz.cache()

    works = lz_cached.select(
        doi=normalise_datacite_doi(pl.col("id")),
        title=remove_markup(
            pl.col("attributes").struct.field("titles").list.eval(pl.element().struct.field("title")).list.join(" ")
        ),
        abstract=remove_markup(
            pl.col("attributes")
            .struct.field("descriptions")
            .list.eval(pl.element().struct.field("description"))
            .list.join(" ")
        ),
        type=pl.col("attributes").struct.field("types").struct.field("resourceTypeGeneral"),
        publication_date=pl.col("attributes")
        .struct.field("created")
        .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ")
        .cast(Date),  # E.g. 2018-05-06T17:23:29Z
        updated_date=pl.col("attributes")
        .struct.field("updated")
        .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ"),
        # E.g. 2025-01-01T00:00:01Z
        publication_venue=pl.col("attributes").struct.field("publisher").struct.field("name"),
        authors=pl.col("attributes").struct.field("creators")
        # Get all non institutional authors
        .list.eval(pl.element().filter(pl.element().struct.field("nameType") == "Personal"))
        .list.eval(
            pl.struct(
                [
                    process_orcid(pl.element().struct.field("nameIdentifiers")).alias("orcid"),
                    process_author_name(
                        pl.element().struct.field("givenName"),
                        pl.element().struct.field("familyName"),
                        pl.element().struct.field("name"),
                    ).struct.unnest(),
                ]
            )
        )
        .list.eval(
            pl.element().filter(
                pl.any_horizontal(
                    [
                        pl.element().struct.field(col).is_not_null()
                        for col in [
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
    ).with_columns(
        title=replace_with_null(pl.col("title"), [""]),
        abstract=replace_with_null(pl.col("abstract"), ["", ":unav", "Cover title."]),
    )

    funders_by_work = (
        lz_cached.select(
            work_doi=normalise_datacite_doi(pl.col("id")),
            funding_references=pl.col("attributes").struct.field("fundingReferences"),
        )
        .explode("funding_references")
        .unnest("funding_references")
        .with_columns(award_number=pl.col("awardNumber").str.split(",").list.eval(pl.element().str.strip_chars()))
        .explode("award_number")
        .select(
            work_doi=pl.col("work_doi"),
            funder_identifier=normalise_identifier(pl.col("funderIdentifier")),
            funder_identifier_type=pl.col("funderIdentifierType"),
            funder_name=pl.col("funderName"),
            award_number=clean_string(pl.col("award_number")),
            award_uri=pl.col("awardUri"),
        )
        .filter(
            pl.any_horizontal(
                [
                    pl.col(field).is_not_null()
                    for field in [
                        "funder_identifier",
                        "funder_identifier_type",
                        "funder_name",
                        "award_number",
                        "award_uri",
                    ]
                ]
            )
        )
        .group_by("work_doi")
        .agg(
            pl.struct(
                [
                    "funder_identifier",
                    "funder_identifier_type",
                    "funder_name",
                    "award_number",
                    "award_uri",
                ]
            ).alias("funders")
        )
    )

    institutions = (
        lz_cached.select(
            work_doi=normalise_datacite_doi(pl.col("id")), creators=pl.col("attributes").struct.field("creators")
        )
        .explode("creators")
        .unnest("creators")
        .select(
            pl.col("work_doi"),
            name_type=pl.col("nameType"),
            affiliation=pe.parse_datacite_affiliations(pl.col("affiliation")),
        )
        .filter(pl.col("name_type") == "Personal")
        .explode("affiliation")
        .unnest("affiliation")
        .select(
            pl.col("work_doi"),
            affiliation_identifier=normalise_identifier(pl.col("affiliationIdentifier")),
            affiliation_identifier_scheme=pl.col("affiliationIdentifierScheme"),
            name=pl.col("name"),
            scheme_uri=pl.col("schemeUri"),
        )
        .filter(
            pl.any_horizontal(
                [
                    pl.col(field).is_not_null()
                    for field in ["affiliation_identifier", "affiliation_identifier_scheme", "name", "scheme_uri"]
                ]
            )
        )
        .unique(maintain_order=True)
    )
    institutions_by_work = (
        institutions.with_columns(
            inst=pl.struct(
                pl.col("affiliation_identifier"),
                pl.col("affiliation_identifier_scheme"),
                pl.col("name"),
                pl.col("scheme_uri"),
            )
        )
        .group_by("work_doi")
        .agg(institutions=pl.col("inst").unique(maintain_order=True))
    )
    inst_dtype = institutions_by_work.collect_schema()["institutions"]
    funder_dtype = funders_by_work.collect_schema()["funders"]
    datacite_works = (
        works.join(
            institutions_by_work,
            left_on="doi",
            right_on="work_doi",
            how="left",
        )
        .join(
            funders_by_work,
            left_on="doi",
            right_on="work_doi",
            how="left",
        )
        .with_columns(
            institutions=pl.col("institutions").fill_null(pl.lit([]).cast(inst_dtype)),
            funders=pl.col("funders").fill_null(pl.lit([]).cast(funder_dtype)),
        )
    )

    # Build relations
    works_relations = (
        lz_cached.select(
            work_doi=normalise_datacite_doi(pl.col("id")),
            relatedIdentifiers=pl.col("attributes").struct.field("relatedIdentifiers"),
        )
        .explode("relatedIdentifiers")
        .unnest("relatedIdentifiers")
        .select(
            pl.col("work_doi"),
            relation_type=pl.col("relationType"),
            related_identifier=pl.col("relatedIdentifier"),
            # TODO: only run normalise_identifier for DOI  related_identifier_type
            related_identifier_type=pl.col("relatedIdentifierType"),
        )
        .filter(
            ~pl.all_horizontal(
                [pl.col(col).is_null() for col in ["relation_type", "related_identifier", "related_identifier_type"]]
            )
        )
    )

    return [
        ("datacite_works", datacite_works),
        ("datacite_works_relations", works_relations),
    ]


def transform_datacite(
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    batch_size: int = os.cpu_count(),
    extract_workers: int = 1,
    transform_workers: int = 2,
    cleanup_workers: int = 1,
    extract_queue_size: int = 0,
    transform_queue_size: int = 10,
    cleanup_queue_size: int = 0,
    max_file_processes: int = os.cpu_count(),
    n_batches: int = None,
    low_memory: bool = False,
):
    process_files_parallel(
        # Non customizable parameters, specific to DataCite
        schema=SCHEMA,
        transform_func=transform,
        file_glob="**/*jsonl.gz",
        read_func=read_jsonls,
        extract_func=extract_gzip,
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
