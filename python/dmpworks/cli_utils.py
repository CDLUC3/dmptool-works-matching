import pathlib
from typing import Annotated, Literal

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
