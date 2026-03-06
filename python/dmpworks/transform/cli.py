import logging
import pathlib
from typing import Annotated, Literal

from cyclopts import App, Parameter, validators

from dmpworks.cli_utils import (
    CrossrefMetadataTransformConfig,
    DataCiteTransformConfig,
    Directory,
    LogLevel,
    OpenAlexWorksTransformConfig,
)
from dmpworks.transform.utils_file import setup_multiprocessing_logging

app = App(name="transform", help="Transformation utilities.")


@app.command(name="crossref-metadata")
def crossref_metadata_cmd(
    in_dir: Directory,
    out_dir: Directory,
    *,
    config: CrossrefMetadataTransformConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Transform Crossref Metadata to Parquet.

    Args:
        in_dir: Path to the input Crossref Metadata directory (e.g., /path/to/March 2025 Public Data File from Crossref).
        out_dir: Path to the output directory for transformed Parquet files (e.g. /path/to/crossref_metadata).
        config: optional configuration parameters.
        log_level: Python log level.
    """
    from dmpworks.transform.crossref_metadata import transform_crossref_metadata

    config = CrossrefMetadataTransformConfig() if config is None else config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)

    transform_crossref_metadata(
        in_dir=in_dir,
        out_dir=out_dir,
        **vars(config),
        log_level=level,
    )


@app.command(name="datacite")
def datacite_cmd(
    in_dir: Directory,
    out_dir: Directory,
    *,
    config: DataCiteTransformConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Transform DataCite to Parquet.

    Args:
        in_dir: Path to the input DataCite dois directory (e.g., /path/to/DataCite_Public_Data_File_2024/dois).
        out_dir: Path to the output directory for transformed Parquet files (e.g. /path/to/datacite).
        config: optional configuration parameters.
        log_level: Python log level.
    """
    from dmpworks.transform.datacite import transform_datacite

    config = DataCiteTransformConfig() if config is None else config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)

    transform_datacite(
        in_dir=in_dir,
        out_dir=out_dir,
        **vars(config),
        log_level=level,
    )


@app.command(name="openalex-works")
def openalex_works_cmd(
    in_dir: Directory,
    out_dir: Directory,
    *,
    config: OpenAlexWorksTransformConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Transform OpenAlex Works to Parquet.

    Args:
        in_dir: Path to the OpenAlex works directory (e.g. /path/to/openalex_snapshot/data/works).
        out_dir: "Path to the output directory (e.g. /path/to/openalex_works)."
        config: optional configuration parameters.
        log_level: Python log level.
    """
    from dmpworks.transform.openalex_works import transform_openalex_works

    config = OpenAlexWorksTransformConfig() if config is None else config
    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)

    transform_openalex_works(
        in_dir=in_dir,
        out_dir=out_dir,
        **vars(config),
        log_level=level,
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
        pathlib.Path | None,
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
    from dmpworks.dataset_subset import load_dois, load_institutions
    from dmpworks.transform.dataset_subset import create_dataset_subset

    level = logging.getLevelName(log_level)
    setup_multiprocessing_logging(level)

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
