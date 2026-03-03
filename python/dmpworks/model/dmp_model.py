from __future__ import annotations

from functools import cached_property
from typing import Optional

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
    created: Optional[pendulum.DateTime]
    registered: Optional[pendulum.DateTime]
    modified: Optional[pendulum.DateTime]
    title: Optional[str]
    abstract_text: Optional[str]
    project_start: Optional[pendulum.Date]
    project_end: Optional[pendulum.Date]
    institutions: list[Institution]
    authors: list[Author]
    funding: list[FundingItem]
    published_outputs: Optional[list[ResearchOutput]]
    external_data: Optional[ExternalData] = None

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
        return parse_pendulum_datetime(v)

    @field_serializer("created", "registered", "modified")
    def serialize_pendulum_datetime(self, v: pendulum.DateTime):
        return serialize_pendulum_datetime(v)

    @field_validator("project_start", "project_end", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        return parse_pendulum_date(v)

    @field_serializer("project_start", "project_end")
    def serialize_pendulum_date(self, v: pendulum.Date):
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

    funder: Optional[Funder]
    funding_opportunity_id: Optional[str]
    status: Optional[str]
    award_id: Optional[str]


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
    def parse_pendulum_date(cls, v):
        return parse_pendulum_date(v)

    @field_serializer("updated")
    def serialize_pendulum_date(self, v: pendulum.DateTime):
        return serialize_pendulum_date(v)


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

    funder: Optional[Funder]
    award_id: Optional[AwardID]
    funded_dois: list[str]
    award_url: Optional[str] = None

    @field_validator("award_id", mode="before")
    @classmethod
    def parse_award_id(cls, v):
        if isinstance(v, AwardID):
            return v
        if isinstance(v, dict):
            return AwardID.from_dict(v)
        raise TypeError(f"Expected MyClass or dict, got {type(v)}")

    @field_serializer("award_id")
    def serialize_award_id(self, v: AwardID):
        return v.to_dict()
