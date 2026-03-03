from __future__ import annotations

from functools import cached_property

import pendulum
from pydantic import BaseModel, field_serializer, field_validator

from dmpworks.funders.award_id import AwardID
from dmpworks.model.common import (
    Author,
    Funder,
    Institution,
    parse_pendulum_date,
    parse_pendulum_datetime,
    serialize_pendulum_date,
    serialize_pendulum_datetime,
    to_camel,
)


class DMPModel(BaseModel):
    """Represents a Data Management Plan (DMP).

    Attributes:
        doi: The DOI of the DMP.
        created: The creation date and time.
        registered: The registration date and time.
        modified: The modification date and time.
        title: The title of the DMP.
        abstract_text: The abstract text of the DMP.
        project_start: The project start date.
        project_end: The project end date.
        institutions: A list of associated institutions.
        authors: A list of authors.
        funding: A list of funding items.
        published_outputs: A list of published outputs.
        external_data: External data associated with the DMP.
    """

    model_config = {
        "arbitrary_types_allowed": True,
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    doi: str
    created: pendulum.DateTime | None
    registered: pendulum.DateTime | None
    modified: pendulum.DateTime | None
    title: str | None
    abstract_text: str | None
    project_start: pendulum.Date | None
    project_end: pendulum.Date | None
    institutions: list[Institution]
    authors: list[Author]
    funding: list[FundingItem]
    published_outputs: list[ResearchOutput] | None
    external_data: ExternalData | None = None

    @cached_property
    def funded_dois(self) -> list[str]:
        """Get a list of funded DOIs from external data.

        Returns:
            list[str]: A list of unique funded DOIs.
        """
        funded_dois = set()
        for award in self.external_data.awards:
            for doi in award.funded_dois:
                funded_dois.add(doi)
        return list(funded_dois)

    @field_validator("created", "registered", "modified", mode="before")
    @classmethod
    def parse_pendulum_datetime(cls, v):
        """Parse a datetime string into a pendulum.DateTime object.

        Args:
            v: The value to parse.

        Returns:
            pendulum.DateTime: The parsed datetime.
        """
        return parse_pendulum_datetime(v)

    @field_serializer("created", "registered", "modified")
    def serialize_pendulum_datetime(self, v: pendulum.DateTime):
        """Serialize a pendulum.DateTime object into a string.

        Args:
            v: The datetime object to serialize.

        Returns:
            str: The serialized datetime string.
        """
        return serialize_pendulum_datetime(v)

    @field_validator("project_start", "project_end", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        """Parse a date string into a pendulum.Date object.

        Args:
            v: The value to parse.

        Returns:
            pendulum.Date: The parsed date.
        """
        return parse_pendulum_date(v)

    @field_serializer("project_start", "project_end")
    def serialize_pendulum_date(self, v: pendulum.Date):
        """Serialize a pendulum.Date object into a string.

        Args:
            v: The date object to serialize.

        Returns:
            str: The serialized date string.
        """
        return serialize_pendulum_date(v)


class ResearchOutput(BaseModel):
    """Represents a research output.

    Attributes:
        doi: The DOI of the research output.
    """

    doi: str


class FundingItem(BaseModel):
    """Represents a funding item.

    Attributes:
        funder: The funder.
        funding_opportunity_id: The funding opportunity ID.
        status: The status of the funding.
        award_id: The award ID.
    """

    funder: Funder | None
    funding_opportunity_id: str | None
    status: str | None
    award_id: str | None


class ExternalData(BaseModel):
    """Represents external data associated with a DMP.

    Attributes:
        updated: The date and time the external data was updated.
        awards: A list of awards.
    """

    model_config = {
        "arbitrary_types_allowed": True,
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    updated: pendulum.DateTime
    awards: list[Award]

    @field_validator("updated", mode="before")
    @classmethod
    def parse_pendulum_datetime(cls, v):
        """Parse a datetime string into a pendulum.DateTime object.

        Args:
            v: The value to parse.

        Returns:
            pendulum.DateTime: The parsed date.
        """
        return parse_pendulum_datetime(v)

    @field_serializer("updated")
    def serialize_pendulum_datetime(self, v: pendulum.DateTime):
        """Serialize a pendulum.DateTime object into a string.

        Args:
            v: The datetime object to serialize.

        Returns:
            str: The serialized datetime string.
        """
        return serialize_pendulum_datetime(v)


class Award(BaseModel):
    """Represents an award in external data.

    Attributes:
        funder: The funder.
        award_id: The award ID object.
        funded_dois: A list of DOIs funded by this award.
        award_url: The URL of the award.
    """

    model_config = {
        "arbitrary_types_allowed": True,
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    funder: Funder | None
    award_id: AwardID | None
    funded_dois: list[str]
    award_url: str | None = None

    @field_validator("award_id", mode="before")
    @classmethod
    def parse_award_id(cls, v):
        """Parse an award ID from a dictionary or AwardID object.

        Args:
            v: The value to parse.

        Returns:
            AwardID: The parsed AwardID object.

        Raises:
            TypeError: If the value is not an AwardID or dict.
        """
        if isinstance(v, AwardID):
            return v
        if isinstance(v, dict):
            return AwardID.from_dict(v)
        raise TypeError(f"Expected MyClass or dict, got {type(v)}")

    @field_serializer("award_id")
    def serialize_award_id(self, v: AwardID):
        """Serialize an AwardID object into a dictionary.

        Args:
            v: The AwardID object to serialize.

        Returns:
            dict: The serialized AwardID dictionary.
        """
        return v.to_dict()
