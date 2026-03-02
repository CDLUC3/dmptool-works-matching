import logging
import os
import pathlib
from dataclasses import dataclass
from typing import Annotated, Literal, Optional

from cyclopts import App, Parameter, validators

from dmpworks.cli_utils import Directory, LogLevel
from dmpworks.dataset_subset import load_dois, load_institutions
from dmpworks.transform.crossref_metadata import transform_crossref_metadata
from dmpworks.transform.datacite import transform_datacite
from dmpworks.transform.dataset_subset import create_dataset_subset
from dmpworks.transform.openalex_works import transform_openalex_works
from dmpworks.utils import copy_dict

app = App(name="transform", help="Transformation utilities.")


BatchSize = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of input files to process per batch (must be >= 1).",
    ),
]
RowGroupSize = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Parquet row group size (must be >= 1). For efficient downstream querying, target row group sizes of 128-512MB. Row groups are buffered fully in memory before being flushed to disk.",
    ),
]
RowGroupsPerFile = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of row groups per Parquet file (must be >= 1). Target file sizes of 512MB-1GB.",
    ),
]
MaxWorkers = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of workers to run in parallel (must be >= 1).",
    ),
]


@Parameter(name="*")
@dataclass
class CrossrefMetadataConfig:
    batch_size: BatchSize = 500
    row_group_size: RowGroupSize = 500_000
    row_groups_per_file: RowGroupsPerFile = 4
    max_workers: MaxWorkers = os.cpu_count()
    log_level: LogLevel = "INFO"


@app.command(name="crossref-metadata")
def crossref_metadata_cmd(
    in_dir: Directory,
    out_dir: Directory,
    *,
    config: Optional[CrossrefMetadataConfig] = None,
):
    """Transform Crossref Metadata to Parquet.

    Args:
        in_dir: Path to the input Crossref Metadata directory (e.g., /path/to/March 2025 Public Data File from Crossref).
        out_dir: Path to the output directory for transformed Parquet files (e.g. /path/to/crossref_metadata).
        config: optional configuration parameters.
    """

    config = CrossrefMetadataConfig() if config is None else config

    log_level = logging.getLevelName(config.log_level)
    transform_crossref_metadata(
        in_dir=in_dir,
        out_dir=out_dir,
        log_level=log_level,
        **copy_dict(vars(config), ["log_level"]),
    )


@Parameter(name="*")
@dataclass
class DataCiteConfig:
    batch_size: BatchSize = 150
    row_group_size: RowGroupSize = 250_000
    row_groups_per_file: RowGroupsPerFile = 8
    max_workers: MaxWorkers = 8
    log_level: LogLevel = "INFO"


@app.command(name="datacite")
def datacite_cmd(
    in_dir: Directory,
    out_dir: Directory,
    *,
    config: Optional[DataCiteConfig] = None,
):
    """Transform DataCite to Parquet.

    Args:
        in_dir: Path to the input DataCite dois directory (e.g., /path/to/DataCite_Public_Data_File_2024/dois).
        out_dir: Path to the output directory for transformed Parquet files (e.g. /path/to/datacite).
        config: optional configuration parameters.
    """

    config = DataCiteConfig() if config is None else config
    log_level = logging.getLevelName(config.log_level)
    transform_datacite(
        in_dir=in_dir,
        out_dir=out_dir,
        log_level=log_level,
        **copy_dict(vars(config), ["log_level"]),
    )


@Parameter(name="*")
@dataclass
class OpenAlexWorksConfig:
    batch_size: BatchSize = 16
    row_group_size: RowGroupSize = 200_000
    row_groups_per_file: RowGroupsPerFile = 4
    max_workers: MaxWorkers = os.cpu_count()
    log_level: LogLevel = "INFO"


@app.command(name="openalex-works")
def openalex_works_cmd(
    in_dir: Directory,
    out_dir: Directory,
    *,
    config: Optional[OpenAlexWorksConfig] = None,
):
    """Transform OpenAlex Works to Parquet.

    Args:
        in_dir: Path to the OpenAlex works directory (e.g. /path/to/openalex_snapshot/data/works).
        out_dir: "Path to the output directory (e.g. /path/to/openalex_works)."
        config: optional configuration parameters.
    """

    config = OpenAlexWorksConfig() if config is None else config
    log_level = logging.getLevelName(config.log_level)
    transform_openalex_works(
        in_dir=in_dir,
        out_dir=out_dir,
        log_level=log_level,
        **copy_dict(vars(config), ["log_level"]),
    )


@app.command(name="dataset-subset")
def dataset_subset_cmd(
    dataset: Literal["crossref-metadata", "datacite", "openalex-works"],
    in_dir: Directory,
    out_dir: Directory,
    institutions_path: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            ),
            env_var="DATASET_SUBSET_INSTITUTIONS_PATH",
            help="Path to a list of ROR IDs and institution names. Works authored by researchers from these institutions will be included.",
        ),
    ],
    dois_path: Annotated[
        Optional[pathlib.Path],
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            ),
            env_var="DATASET_SUBSET_DOIS_PATH",
            help="Path to a specific list of Work DOIs to include in the subset.",
        ),
    ] = None,
    log_level: LogLevel = "INFO",
):
    """Create a demo dataset.

    Args:
        dataset: The dataset to filter.
        in_dir: Path to the dataset directory (e.g. /path/to/openalex_works).
        out_dir: Path to the output directory (e.g. /path/to/demo_dataset/openalex).
        institutions_path: Path to a JSON file containing a list of ROR IDs and institution names, e.g. `[{"name": "University of California, San Diego", "ror": "0168r3w48"}]`. Works authored by researchers from these institutions will be included.
        dois_path: Path to a JSON file with specific list of Work DOIs to include in the subset, e.g. `["10.0000/abc", "10.0000/123"]`.
        log_level: Python log level.
    """

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    institutions = load_institutions(institutions_path)
    dois = load_dois(dois_path) if dois_path is not None else []

    create_dataset_subset(
        dataset=dataset,
        in_dir=in_dir,
        out_dir=out_dir,
        institutions=institutions,
        dois=dois,
        log_level=level,
    )


if __name__ == "__main__":
    app()
