from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


def to_camel(string: str) -> str:
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


class Institution(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    name: Optional[str]
    ror: Optional[str]


class Author(BaseModel):
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
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    name: Optional[str]
    ror: Optional[str]


class Award(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    award_id: str


class Source(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    name: Optional[str]
    url: Optional[str]
