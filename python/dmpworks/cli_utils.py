from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Annotated, Literal, Optional

import pendulum
import pendulum.parsing
from cyclopts import Parameter, validators


def validate_date_str(type_, value):
    try:
        pendulum.from_format(value, "YYYY-MM-DD")
    except pendulum.parsing.exceptions.ParserError:
        raise ValueError(f"Invalid date: '{value}'. Must be in YYYY-MM-DD format.")


Directory = Annotated[
    pathlib.Path,
    Parameter(
        validator=validators.Path(
            dir_okay=True,
            file_okay=False,
            exists=True,
        )
    ),
]
LogLevel = Annotated[
    Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
    Parameter(help="Python log level"),
]
DateString = Annotated[str, Parameter(validator=validate_date_str)]


@dataclass
class DatasetSubset:
    enable: Annotated[
        bool,
        Parameter(
            env_var="DATASET_SUBSET_ENABLE",
            help="Enable subset creation to filter works by specific institutions or a list of DOIs.",
        ),
    ] = False
    institutions_s3_path: Annotated[
        Optional[str],
        Parameter(
            env_var="DATASET_SUBSET_INSTITUTIONS_S3_PATH",
            help="S3 path (excluding bucket URI) to a list of ROR IDs and institution names. Works authored by researchers from these institutions will be included.",
        ),
    ] = None
    dois_s3_path: Annotated[
        Optional[str],
        Parameter(
            env_var="DATASET_SUBSET_DOIS_S3_PATH",
            help="S3 path (excluding bucket URI) to a specific list of Work DOIs to include in the subset.",
        ),
    ] = None


@dataclass
class DMPSubset:
    enable: Annotated[
        bool,
        Parameter(
            env_var="DMP_SUBSET_ENABLE",
            help="Enable subset creation to filter DMPs by specific institutions or a list of DOIs.",
        ),
    ] = False
    institutions_s3_path: Annotated[
        Optional[str],
        Parameter(
            env_var="DMP_SUBSET_INSTITUTIONS_S3_PATH",
            help="S3 path (excluding bucket URI) to a list of ROR IDs and institution names. DMPs created by researchers from these institutions will be included.",
        ),
    ] = None
    dois_s3_path: Annotated[
        Optional[str],
        Parameter(
            env_var="DMP_SUBSET_DOIS_S3_PATH",
            help="S3 path (excluding bucket URI) to a specific list of DMP DOIs to include in the subset.",
        ),
    ] = None
