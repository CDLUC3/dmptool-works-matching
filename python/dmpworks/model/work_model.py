from functools import cached_property
from typing import Optional

import pendulum
from pydantic import BaseModel, field_serializer, field_validator

from dmpworks.model.common import (
    Author,
    Award,
    Funder,
    Institution,
    parse_pendulum_date,
    parse_pendulum_datetime,
    Relations,
    serialize_pendulum_date,
    serialize_pendulum_datetime,
    Source,
    to_camel,
)


class WorkModel(BaseModel):
    """Represents a scholarly work.

    Attributes:
        doi: The DOI of the work.
        hash: A hash of the work's content.
        title: The title of the work.
        abstract_text: The abstract text of the work.
        work_type: The type of the work (e.g., article, dataset).
        publication_date: The publication date.
        updated_date: The date the work was last updated.
        publication_venue: The venue where the work was published.
        institutions: A list of associated institutions.
        authors: A list of authors.
        funders: A list of funders.
        awards: A list of awards.
        relations: Relations to other works.
        source: The source of the work metadata.
    """

    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    doi: str
    hash: str
    title: Optional[str] = None
    abstract_text: Optional[str] = None
    work_type: str
    publication_date: Optional[pendulum.Date]
    updated_date: Optional[pendulum.DateTime]
    publication_venue: Optional[str] = None
    institutions: list[Institution]
    authors: list[Author]
    funders: list[Funder]
    awards: list[Award]
    relations: Relations
    source: Source

    @field_validator("publication_date", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        return parse_pendulum_date(v)

    @field_validator("updated_date", mode="before")
    @classmethod
    def parse_pendulum_datetime(cls, v):
        return parse_pendulum_datetime(v)

    @field_serializer("publication_date")
    def serialize_pendulum_date(self, v: Optional[pendulum.Date]):
        return serialize_pendulum_date(v)

    @field_serializer("updated_date")
    def serialize_pendulum_datetime(self, v: Optional[pendulum.DateTime]):
        return serialize_pendulum_datetime(v)
