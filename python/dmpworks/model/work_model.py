import pendulum
from pydantic import BaseModel, field_serializer, field_validator

from dmpworks.model.common import (
    Author,
    Award,
    Funder,
    Institution,
    Relations,
    Source,
    parse_pendulum_date,
    parse_pendulum_datetime,
    serialize_pendulum_date,
    serialize_pendulum_datetime,
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
    title: str | None = None
    abstract_text: str | None = None
    work_type: str
    publication_date: pendulum.Date | None
    updated_date: pendulum.DateTime | None
    publication_venue: str | None = None
    institutions: list[Institution]
    authors: list[Author]
    funders: list[Funder]
    awards: list[Award]
    relations: Relations
    source: Source

    @field_validator("publication_date", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        """Parse a date string into a pendulum.Date object.

        Args:
            v: The value to parse.

        Returns:
            pendulum.Date: The parsed date.
        """
        return parse_pendulum_date(v)

    @field_validator("updated_date", mode="before")
    @classmethod
    def parse_pendulum_datetime(cls, v):
        """Parse a datetime string into a pendulum.DateTime object.

        Args:
            v: The value to parse.

        Returns:
            pendulum.DateTime: The parsed datetime.
        """
        return parse_pendulum_datetime(v)

    @field_serializer("publication_date")
    def serialize_pendulum_date(self, v: pendulum.Date | None):
        """Serialize a pendulum.Date object into a string.

        Args:
            v: The date object to serialize.

        Returns:
            str: The serialized date string.
        """
        return serialize_pendulum_date(v)

    @field_serializer("updated_date")
    def serialize_pendulum_datetime(self, v: pendulum.DateTime | None):
        """Serialize a pendulum.DateTime object into a string.

        Args:
            v: The datetime object to serialize.

        Returns:
            str: The serialized datetime string.
        """
        return serialize_pendulum_datetime(v)
