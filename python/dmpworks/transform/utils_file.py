import gzip
import logging
import shutil
from multiprocessing.util import log_to_stderr
from pathlib import Path
from typing import Callable, Generator

import polars as pl
from polars._typing import SchemaDefinition


PrefetchLoaderExtractFunction = Callable[[list[Path], int], list[Path]]


def validate_directory(path: Path, expected_items: list[str]) -> bool:
    actual_items = {p.name for p in path.iterdir()}
    missing = [item for item in expected_items if item not in actual_items]

    if missing:
        logging.info("Missing items:", missing)
        return False

    return True


def extract_gzip(in_file: Path, out_file: Path) -> None:
    with gzip.open(in_file, "rb") as f_in, open(out_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def read_jsonls(files: list[Path], schema: SchemaDefinition, low_memory: bool) -> pl.LazyFrame:
    return pl.scan_ndjson(files, schema=schema, low_memory=low_memory)


def write_parquet(lz: pl.LazyFrame, out_file: Path) -> None:
    lz.sink_parquet(out_file, compression="snappy")


def setup_multiprocessing_logging(log_level: int):
    logging.basicConfig(
        level=log_level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] [%(threadName)s] %(message)s"
    )
    if log_level == logging.DEBUG:
        # Make multi-processing print logs
        log_to_stderr(logging.DEBUG)
