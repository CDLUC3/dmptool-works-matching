from __future__ import annotations

import datetime
from functools import cached_property
from typing import Optional

import pendulum
from pydantic import BaseModel, field_serializer, field_validator

from dmpworks.funders.award_id import AwardID
from dmpworks.model.common import Author, Funder, Institution, to_camel


class DMPModel(BaseModel):
    model_config = {
        "arbitrary_types_allowed": True,
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    doi: str
    created: Optional[pendulum.Date]
    registered: Optional[pendulum.Date]
    modified: Optional[pendulum.Date]
    title: Optional[str]
    abstract: Optional[str]
    project_start: Optional[pendulum.Date]
    project_end: Optional[pendulum.Date]
    institutions: list[Institution]
    authors: list[Author]
    funding: list[FundingItem]
    external_data: Optional[ExternalData] = None

    @cached_property
    def funded_dois(self) -> list[str]:
        funded_dois = set()
        for award in self.external_data.awards:
            for doi in award.funded_dois:
                funded_dois.add(doi)
        return list(funded_dois)

    @field_validator("created", "registered", "modified", "project_start", "project_end", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        if isinstance(v, str):
            return pendulum.parse(v).date()
        elif isinstance(v, datetime.date):
            return pendulum.instance(v)
        return v

    @field_serializer("created", "registered", "modified", "project_start", "project_end")
    def serialize_pendulum_date(self, v: pendulum.Date):
        return v.to_date_string()


class FundingItem(BaseModel):
    funder: Optional[Funder]
    funding_opportunity_id: Optional[str]
    status: Optional[str]
    award_id: Optional[str]


class ExternalData(BaseModel):
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
        if isinstance(v, str):
            return pendulum.parse(v)
        elif isinstance(v, datetime.datetime):
            return pendulum.instance(v)
        return v

    @field_serializer("updated")
    def serialize_pendulum_date(self, v: pendulum.DateTime):
        return v.to_iso8601_string()


class Award(BaseModel):
    model_config = {
        "arbitrary_types_allowed": True,
        "alias_generator": to_camel,
        "populate_by_name": True,
    }

    funder: Optional[Funder]
    award_id: Optional[AwardID]
    funded_dois: list[str]
    award_url: Optional[str] = None

    @cached_property
    def funded_dois_set(self) -> frozenset[str]:
        return frozenset(self.funded_dois)

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
