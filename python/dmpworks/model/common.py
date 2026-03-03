from __future__ import annotations

import datetime
from typing import Any, Optional

import pendulum
from pydantic import BaseModel


def to_camel(string: str) -> str:
    """Split the string by underscores and capitalize the first letter of each part except the first."""
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


class Institution(BaseModel):
    """Represents an institution.

    Attributes:
        name: The name of the institution.
        ror: The ROR ID of the institution.
    """
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    name: Optional[str]
    ror: Optional[str]


class Author(BaseModel):
    """Represents an author.

    Attributes:
        orcid: The ORCID of the author.
        first_initial: The first initial of the author.
        given_name: The given name of the author.
        middle_initials: The middle initials of the author.
        middle_names: The middle names of the author.
        surname: The surname of the author.
        full: The full name of the author.
    """
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    orcid: Optional[str]
    first_initial: Optional[str]
    given_name: Optional[str]
    middle_initials: Optional[str]
    middle_names: Optional[str]
    surname: Optional[str]
    full: Optional[str]


class Funder(BaseModel):
    """Represents a funder.

    Attributes:
        name: The name of the funder.
        ror: The ROR ID of the funder.
    """
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    name: Optional[str]
    ror: Optional[str]


class Award(BaseModel):
    """Represents an award.

    Attributes:
        award_id: The ID of the award.
    """
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    award_id: str


class Relation(BaseModel):
    """Represents a relation to another work.

    Attributes:
        doi: The DOI of the related work.
    """
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    doi: str


class Relations(BaseModel):
    """Represents a collection of relations.

    Attributes:
        intra_work_dois: DOIs of intra-work relations.
        possible_shared_project_dois: DOIs of possible shared project relations.
        dataset_citation_dois: DOIs of dataset citation relations.
    """
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    intra_work_dois: list[Relation]
    possible_shared_project_dois: list[Relation]
    dataset_citation_dois: list[Relation]


class Source(BaseModel):
    """Represents a source.

    Attributes:
        name: The name of the source.
        url: The URL of the source.
    """
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    name: Optional[str]
    url: Optional[str]


def parse_pendulum_datetime(v: Any) -> Optional[pendulum.DateTime]:
    """Parse a value into a pendulum DateTime object."""
    if v is None:
        return None
    elif isinstance(v, pendulum.DateTime):
        return v
    elif isinstance(v, datetime.datetime):
        return pendulum.instance(v)
    elif isinstance(v, str):
        try:
            parsed = pendulum.parse(v)
            if isinstance(parsed, pendulum.DateTime):
                return parsed
            elif isinstance(parsed, pendulum.Date):
                # Convert a date-only parse into a pendulum.DateTime at midnight
                return pendulum.datetime(parsed.year, parsed.month, parsed.day)
            else:
                raise ValueError(f"Parsed value is not a datetime: {v}")
        except Exception as e:
            raise ValueError(f"Failed to parse datetime string '{v}': {e}")

    raise TypeError(
        f"Expected str, datetime.date, datetime.datetime, pendulum.Date, or pendulum.DateTime, got {type(v).__name__}"
    )


def serialize_pendulum_datetime(v: Any) -> Optional[str]:
    """Serialize a pendulum DateTime object to an ISO 8601 string."""
    if v is None:
        return None
    elif isinstance(v, pendulum.DateTime):
        return v.to_iso8601_string()
    raise TypeError(f"Expected pendulum.DateTime, got {type(v).__name__}")


def parse_pendulum_date(v: Any) -> Optional[pendulum.Date]:
    """Parse a value into a pendulum Date object."""
    if v is None:
        return None
    elif isinstance(v, pendulum.Date):
        return v
    elif isinstance(v, (pendulum.DateTime, datetime.datetime)):
        return pendulum.instance(v).date()
    elif isinstance(v, datetime.date):
        return pendulum.date(v.year, v.month, v.day)
    elif isinstance(v, str):
        try:
            parsed = pendulum.parse(v)
            if isinstance(parsed, pendulum.Date):
                return parsed
            if isinstance(parsed, pendulum.DateTime):
                return parsed.date()
            else:
                raise ValueError(f"Parsed value is not a date: {v}")
        except Exception as e:
            raise ValueError(f"Failed to parse date string '{v}': {e}")

    raise TypeError(
        f"Expected str, datetime.date, or pendulum.Date, datetime.datetime, pendulum.DateTime, got {type(v).__name__}"
    )


def serialize_pendulum_date(v: Any) -> Optional[str]:
    """Serialize a pendulum Date object to a date string."""
    if v is None:
        return None
    if isinstance(v, pendulum.Date):
        return v.to_date_string()
    raise TypeError(f"Expected pendulum.Date, got {type(v).__name__}")
