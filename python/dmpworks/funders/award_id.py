from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Self, TypeVar

from dmpworks.utils import import_from_path

log = logging.getLogger(__name__)

T = TypeVar("T", bound="AwardID")


class AwardID(ABC):
    parent_ror_ids: list = []  # The funder ROR IDs

    def __init__(self, text: str, fields: list[str]):
        self.text: str = text
        self.fields: list[str] = fields
        self.related_awards: list[Self] = []

    @abstractmethod
    def fetch_additional_metadata(self):
        """Fetches additional metadata associated with the award ID"""
        raise NotImplementedError("Please implement")

    @abstractmethod
    def generate_variants(self) -> list[str]:
        """Generates variants of the funder ID"""
        raise NotImplementedError("Please implement")

    @staticmethod
    @abstractmethod
    def parse(text: Optional[str]) -> Optional[T]:
        """Parses a funder ID"""
        raise NotImplementedError("Please implement")

    @abstractmethod
    def identifier_string(self) -> str:
        """The canonical identifier as a string"""
        raise NotImplementedError("Please implement")

    @abstractmethod
    def award_url(self) -> Optional[str]:
        """Returns the URL for the award"""
        raise NotImplementedError("Please implement")

    @cached_property
    def all_variants(self) -> list[str]:
        award_ids = set()

        # Award IDs for this award
        for award_id in self.generate_variants():
            award_ids.add(award_id)

        # Add award IDs for related awards
        for related_award in self.related_awards:
            for award_id in related_award.generate_variants():
                award_ids.add(award_id)

        return list(award_ids)

    def parts(self) -> list[IdentifierPart]:
        """The parts that make up the ID"""
        parts = []
        for field in self.fields:
            value = getattr(self, field)
            parts.append(IdentifierPart(value, field))
        return parts

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        for field in self.fields:
            if getattr(self, field) != getattr(other, field):
                return False

        return True

    def __hash__(self):
        values = [getattr(self, field) for field in self.fields]
        return hash(tuple(values))

    def __repr__(self):
        class_name = self.__class__.__name__
        attrs = ", ".join(f"{field}={getattr(self, field)!r}" for field in self.fields)
        return f"{class_name}({attrs})"

    @classmethod
    def from_dict(cls, dict_: dict) -> AwardID:
        """Construct an AwardID from a dict, you must pass the correct subclass"""

        cls_ = cls
        if cls == AwardID:
            # Fallback to class path stored in dict_
            class_path = import_from_path(dict_.get("class"))
            if not issubclass(class_path, AwardID):
                raise TypeError(f"AwardID.from_dict: cls {class_path} must be a subclass of AwardID")
            cls_ = class_path
        elif not issubclass(cls, AwardID):
            raise TypeError(f"AwardID.from_dict: cls {cls_} must be a subclass of AwardID")

        parts = [IdentifierPart.from_dict(part) for part in dict_.get("parts", [])]
        parts_dict = {part.type: part.value for part in parts}

        obj = cls_(**parts_dict)
        obj.related_awards = [cls_.from_dict(award) for award in dict_.get("related_awards", [])]

        return obj

    def to_dict(self) -> dict:
        """Converts the Award ID into a dict"""

        return {
            "class": f"{self.__class__.__module__}.{self.__class__.__name__}",
            "parts": [part.to_dict() for part in self.parts()],
            "related_awards": [award.to_dict() for award in self.related_awards],
        }


@dataclass
class Identifier:
    id: str
    type: str

    @classmethod
    def from_dict(cls, dict_) -> Identifier:
        return Identifier(
            dict_.get("id"),
            dict_.get("type"),
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": self.type,
        }


@dataclass
class IdentifierPart:
    value: str
    type: str

    @classmethod
    def from_dict(cls, dict_) -> IdentifierPart:
        return IdentifierPart(
            dict_.get("value"),
            dict_.get("type"),
        )

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "type": self.type,
        }
