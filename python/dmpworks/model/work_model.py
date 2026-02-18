import datetime
from functools import cached_property
from typing import Optional

import pendulum
from pydantic import BaseModel, computed_field, field_serializer, field_validator

from dmpworks.model.common import Author, Award, Funder, Institution, Source, to_camel


class WorkModel(BaseModel):
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
    source: Source

    @computed_field
    def institutions_names_text(self) -> str:
        return ", ".join(dict.fromkeys(inst.name for inst in self.institutions))

    @computed_field
    def authors_names_text(self) -> str:
        return ", ".join(dict.fromkeys(author.full for author in self.authors))

    @computed_field
    def funders_names_text(self) -> str:
        return ", ".join(dict.fromkeys(funder.name for funder in self.funders))

    @cached_property
    def funder_ids_set(self) -> frozenset[str]:
        return frozenset(self.funder_ids)

    @field_validator("publication_date", mode="before")
    @classmethod
    def parse_pendulum_date(cls, v):
        if isinstance(v, str):
            return pendulum.parse(v).date()
        elif isinstance(v, datetime.date):
            return pendulum.instance(v)
        return v

    @field_validator("updated_date", mode="before")
    @classmethod
    def parse_pendulum_datetime(cls, v):
        if isinstance(v, str):
            return pendulum.parse(v)
        elif isinstance(v, datetime.datetime):
            return pendulum.instance(v)
        return v

    @field_serializer("publication_date")
    def serialize_pendulum_date(self, v: Optional[pendulum.Date]):
        if v is None:
            return None

        return v.to_date_string()

    @field_serializer("updated_date")
    def serialize_pendulum_datetime(self, v: Optional[pendulum.DateTime]):
        if v is None:
            return None

        return v.to_iso8601_string()
