import logging
import os
import pathlib
from dataclasses import dataclass
from typing import Annotated, Literal, Optional

from cyclopts import App, Parameter, validators

from dmpworks.cli_utils import Directory, LogLevel
from dmpworks.transform.crossref_metadata import transform_crossref_metadata
from dmpworks.transform.datacite import transform_datacite
from dmpworks.transform.demo_dataset import create_demo_dataset
from dmpworks.transform.dmps import transform_dmps
from dmpworks.transform.openalex_funders import transform_openalex_funders
from dmpworks.transform.openalex_works import transform_openalex_works
from dmpworks.transform.ror import transform_ror
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmpworks.utils import copy_dict

app = App(name="transform", help="Transformation utilities.")


BatchSize = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of files to process per batch (must be >= 1).",
    ),
]
NumExtractWorkers = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of parallel workers for file extraction (must be >= 1).",
    ),
]
NumTransformWorkers = Annotated[
    int,
    Parameter(
        validator=validators.Number(
            gte=1,
        ),
        help="Number of parallel workers for file transformation (must be >= 1).",
    ),
]
NumCleanupWorkers = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of parallel workers for file cleanup (must be >= 1).",
    ),
]
ExtractQueueSize = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=0),
        help="File extraction queue size (must be >= 0, zero is unlimited).",
    ),
]
TransformQueueSize = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=0),
        help="File transform queue size (must be >= 0, zero is unlimited).",
    ),
]
CleanupQueueSize = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=0),
        help="File cleanup queue size (must be >= 0, zero is unlimited).",
    ),
]
MaxFileProcesses = Annotated[
    int,
    Parameter(
        validator=validators.Number(gte=1),
        help="Number of processes to use when extracting files in parallel (must be >= 1).",
    ),
]
NumBatches = Annotated[
    Optional[int],
    Parameter(
        validator=validators.Number(gte=1),
        help="Set an explicit number of batches to process (e.g. for testing purposes).",
    ),
]
LowMemory = Annotated[
    bool,
    Parameter(
        help="Enable low memory mode for Polars when streaming records from files.",
    ),
]


@Parameter(name="*")
@dataclass
class CrossrefMetadataConfig:
    batch_size: BatchSize = os.cpu_count()
    extract_workers: NumExtractWorkers = 1
    transform_workers: NumTransformWorkers = 2
    cleanup_workers: NumCleanupWorkers = 1
    extract_queue_size: ExtractQueueSize = 0
    transform_queue_size: TransformQueueSize = 3
    cleanup_queue_size: CleanupQueueSize = 0
    max_file_processes: MaxFileProcesses = os.cpu_count()
    n_batches: NumBatches = None
    low_memory: LowMemory = False
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
        out_dir: Path to the output directory for transformed Parquet files (e.g. /path/to/parquets/crossref_metadata).
        config: optional configuration parameters.
    """

    config = CrossrefMetadataConfig() if config is None else config
    setup_multiprocessing_logging(logging.getLevelName(config.log_level))
    transform_crossref_metadata(
        in_dir,
        out_dir,
        **copy_dict(vars(config), ["log_level"]),
    )


@Parameter(name="*")
@dataclass
class DataCiteConfig:
    batch_size: BatchSize = os.cpu_count()
    extract_workers: NumExtractWorkers = 1
    transform_workers: NumTransformWorkers = 2
    cleanup_workers: NumCleanupWorkers = 1
    extract_queue_size: ExtractQueueSize = 0
    transform_queue_size: TransformQueueSize = 10
    cleanup_queue_size: CleanupQueueSize = 0
    max_file_processes: MaxFileProcesses = os.cpu_count()
    n_batches: NumBatches = None
    low_memory: LowMemory = False
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
        out_dir: Path to the output directory for transformed Parquet files (e.g. /path/to/parquets/datacite).
        config: optional configuration parameters.
    """

    config = DataCiteConfig() if config is None else config
    setup_multiprocessing_logging(logging.getLevelName(config.log_level))
    transform_datacite(
        in_dir,
        out_dir,
        **copy_dict(vars(config), ["log_level"]),
    )


@Parameter(name="*")
@dataclass
class OpenAlexFundersConfig:
    batch_size: BatchSize = os.cpu_count()
    extract_workers: NumExtractWorkers = 1
    transform_workers: NumTransformWorkers = 1
    cleanup_workers: NumCleanupWorkers = 1
    extract_queue_size: ExtractQueueSize = 0
    transform_queue_size: TransformQueueSize = 2
    cleanup_queue_size: CleanupQueueSize = 0
    max_file_processes: MaxFileProcesses = os.cpu_count()
    n_batches: NumBatches = None
    low_memory: LowMemory = False
    log_level: LogLevel = "INFO"


@app.command(name="openalex-funders")
def openalex_funders_cmd(
    in_dir: Directory,
    out_dir: Directory,
    *,
    config: Optional[OpenAlexFundersConfig] = None,
):
    """Transform OpenAlex Funders to Parquet.

    Args:
        in_dir: Path to the OpenAlex funders directory (e.g. /path/to/openalex_snapshot/data/funders).
        out_dir: Path to the output directory (e.g. /path/to/parquets/openalex_funders).
        config: optional configuration parameters.
    """

    config = OpenAlexFundersConfig() if config is None else config
    setup_multiprocessing_logging(logging.getLevelName(config.log_level))
    transform_openalex_funders(
        in_dir,
        out_dir,
        **copy_dict(vars(config), ["log_level"]),
    )


@Parameter(name="*")
@dataclass
class OpenAlexWorksConfig:
    batch_size: BatchSize = os.cpu_count()
    extract_workers: NumExtractWorkers = 1
    transform_workers: NumTransformWorkers = 1
    cleanup_workers: NumCleanupWorkers = 1
    extract_queue_size: ExtractQueueSize = 0
    transform_queue_size: TransformQueueSize = 2
    cleanup_queue_size: CleanupQueueSize = 0
    max_file_processes: MaxFileProcesses = os.cpu_count()
    n_batches: NumBatches = None
    low_memory: LowMemory = False
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
        out_dir: "Path to the output directory (e.g. /path/to/parquets/openalex_works)."
        config: optional configuration parameters.
    """

    config = OpenAlexWorksConfig() if config is None else config
    setup_multiprocessing_logging(logging.getLevelName(config.log_level))
    transform_openalex_works(
        in_dir,
        out_dir,
        **copy_dict(vars(config), ["log_level"]),
    )


@Parameter(name="*")
@dataclass
class DMPsConfig:
    batch_size: BatchSize = os.cpu_count()
    extract_workers: NumExtractWorkers = 1
    transform_workers: NumTransformWorkers = 1
    cleanup_workers: NumCleanupWorkers = 1
    extract_queue_size: ExtractQueueSize = 0
    transform_queue_size: TransformQueueSize = 1
    cleanup_queue_size: CleanupQueueSize = 0
    max_file_processes: MaxFileProcesses = os.cpu_count()
    n_batches: NumBatches = None
    low_memory: LowMemory = False
    log_level: LogLevel = "INFO"


@app.command(name="dmps")
def dmps_cmd(
    in_dir: Directory,
    out_dir: Directory,
    *,
    config: Optional[DMPsConfig] = None,
):
    """Transform DMPs to Parquet.

    Args:
        in_dir: Path to the DMPs directory (e.g. /path/to/dmps).
        out_dir: "Path to the output directory (e.g. /path/to/parquets/dmps)."
        config: optional configuration parameters.
    """

    config = DMPsConfig() if config is None else config
    setup_multiprocessing_logging(logging.getLevelName(config.log_level))
    transform_dmps(
        in_dir,
        out_dir,
        **copy_dict(vars(config), ["log_level"]),
    )


@app.command(name="ror")
def ror_works_cmd(
    ror_v2_json_file: Annotated[
        pathlib.Path,
        Parameter(validator=validators.Path(file_okay=True, dir_okay=False)),
    ],
    out_dir: Directory,
    log_level: LogLevel = "INFO",
):
    """Transform ROR to Parquet.

    Args:
        ror_v2_json_file: Path to the ROR V2 (e.g. /path/to/v1.63-2025-04-03-ror-data_schema_v2.json).
        out_dir: Path to the output directory (e.g. /path/to/ror_transformed).
        log_level: the Python logging level.
    """

    setup_multiprocessing_logging(logging.getLevelName(log_level))
    transform_ror(
        ror_v2_json_file,
        out_dir,
    )


@app.command(name="demo-dataset")
def demo_dataset_cmd(
    dataset: Literal["crossref-metadata", "datacite", "openalex-works"],
    ror_id: str,
    in_dir: Directory,
    out_dir: Directory,
    institution_name: Optional[str] = None,
    log_level: LogLevel = "INFO",
):
    """Create a demo dataset.

    Args:
        dataset: The dataset to filter.
        ror_id: A ROR ID without a prefix used to filter records.
        in_dir: Path to the dataset directory (e.g. /path/to/openalex_works).
        out_dir: Path to the output directory (e.g. /path/to/demo_dataset/openalex).
        institution_name: The name of the institution to filter.
        log_level: Python log level.
    """

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    create_demo_dataset(dataset, ror_id, institution_name, in_dir, out_dir, level)


if __name__ == "__main__":
    app()
