import logging
import pathlib
import shutil

import polars
import polars as pl
from dmpworks.transform.transforms import normalise_identifier, normalise_isni
from dmpworks.utils import timed
from polars._typing import SchemaDefinition

log = logging.getLogger(__name__)

SCHEMA: SchemaDefinition = {
    "id": pl.String,
    # "names": pl.List(pl.Struct({"value": pl.String, "types": pl.List(pl.String), "lang": pl.String})),
    # "domains": pl.List(pl.String),
    "external_ids": pl.List(pl.Struct({"type": pl.String, "all": pl.List(pl.String), "preferred": pl.String})),
}


def load_ror(ror_v2_json_file: pathlib.Path):
    return pl.read_json(ror_v2_json_file, schema=SCHEMA)


def create_ror_index(ror_df: pl.DataFrame) -> pl.DataFrame:
    # Get all unique ROR IDs
    ror_ids = (
        ror_df.select(ror_id=normalise_identifier(pl.col("id")))
        .unique()
        .with_columns(type=pl.lit("ror"), identifier=pl.col("ror_id"))
    )

    # Build mappings to other IDs
    other_ids = (
        ror_df.select(ror_id=normalise_identifier(pl.col("id")), external_ids=pl.col("external_ids"))
        .explode("external_ids")
        .unnest("external_ids")
        .select(pl.col("ror_id"), type=pl.col("type"), identifier=pl.col("all"))
        .explode("identifier")
        .filter(pl.col("type").is_not_null() | polars.col("identifier").is_not_null())
        .select(
            pl.col("ror_id"),
            type=pl.col("type"),
            identifier=pl.when(pl.col("type") == "isni")  # Clean ISNIs
            .then(normalise_isni(pl.col("identifier")))
            .when(pl.col("type") == "fundref")  # Add 10.13039 prefix to Fundref IDs
            .then(pl.concat_str([pl.lit("10.13039/"), pl.col("identifier").str.strip_chars().str.to_lowercase()]))
            .otherwise(pl.col("identifier").str.strip_chars().str.to_lowercase()),
        )
    )

    return pl.concat([ror_ids, other_ids])


@timed
def transform_ror(json_file: pathlib.Path, out_dir: pathlib.Path):
    log.info(f"Loading ROR: {json_file}")
    df_ror = load_ror(json_file)
    log.info(f"Creating ROR index")
    df_ror_index = create_ror_index(df_ror)

    # Cleanup existing output dir
    parquets_dir = out_dir / "parquets"
    log.info(f"Cleaning parquets dir: {parquets_dir}")
    shutil.rmtree(parquets_dir, ignore_errors=True)
    parquets_dir.mkdir(parents=True, exist_ok=True)

    file_path = parquets_dir / "ror.parquet"
    log.info(f"Saving ROR: {file_path}")
    df_ror_index.write_parquet(file_path, compression="snappy")
    log.info(f"ROR saved: {file_path}")
