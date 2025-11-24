from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass, field
from typing import Annotated, Literal, Optional

import pendulum
import pendulum.parsing
from cyclopts import Parameter, validators


def validate_date_str(type_, value):
    try:
        pendulum.from_format(value, "YYYY-MM-DD")
    except pendulum.parsing.exceptions.ParserError:
        raise ValueError(f"Invalid date: '{value}'. Must be in YYYY-MM-DD format.")


def validate_institutions_str(type_, value):
    try:
        data = json.loads(value)
        if not isinstance(data, list):
            raise ValueError("JSON must be a list of objects")
        for item in data:
            if "name" not in item and "ror" not in item:
                raise ValueError(f"Items missing 'name' or 'ror' field")
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON, could not decode") from e


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
Institutions = Annotated[
    str,
    Parameter(
        env_var="DATASET_SUBSET_INSTITUTIONS",
        validator=validate_institutions_str,
        help="A list of the institutions to include in JSON Format.",
    ),
]


@dataclass(kw_only=True)
class DatasetSubsetInstitution:
    name: Optional[str] = None
    ror: Optional[str] = None

    def to_dict(self) -> dict:
        return {"name": self.name, "ror": self.ror}

    @classmethod
    def parse(cls, institutions_str: Optional[str]) -> list[DatasetSubsetInstitution]:
        if institutions_str is None:
            return []
        try:
            json_data = json.loads(institutions_str)
            return [cls(**item) for item in json_data]
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON string provided") from e


@dataclass
class DatasetSubset:
    enable: Annotated[
        bool,
        Parameter(
            env_var="DATASET_SUBSET_ENABLE",
            help="Whether or not to create a subset of this dataset based on a list of ROR IDs and institution names.",
        ),
    ] = False
    institutions: Institutions = field(default_factory=list)
