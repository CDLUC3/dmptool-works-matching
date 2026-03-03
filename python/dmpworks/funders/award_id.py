from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
import logging
from typing import ClassVar, Self, TypeVar

from dmpworks.utils import import_from_path

log = logging.getLogger(__name__)

T = TypeVar("T", bound="AwardID")


class AwardID(ABC):
    """Abstract base class for Award IDs.

    Attributes:
        parent_ror_ids: The parent funder ROR IDs.
        text: The original text of the award ID.
        fields: The fields that make up the award ID.
        related_awards: A list of related AwardID objects.
    """

    parent_ror_ids: ClassVar[list[str]] = []  # The funder ROR IDs

    def __init__(self, text: str, fields: list[str]):
        """Initialize the AwardID.

        Args:
            text: The original text of the award ID.
            fields: The fields that make up the award ID.
        """
        self.text: str = text
        self.fields: list[str] = fields
        self.related_awards: list[Self] = []

    @abstractmethod
    def fetch_additional_metadata(self):
        """Fetches additional metadata associated with the award ID."""
        raise NotImplementedError("Please implement")

    @abstractmethod
    def generate_variants(self) -> list[str]:
        """Generates variants of the funder ID."""
        raise NotImplementedError("Please implement")

    @staticmethod
    @abstractmethod
    def parse(text: str | None) -> T | None:
        """Parses a funder ID."""
        raise NotImplementedError("Please implement")

    @abstractmethod
    def identifier_string(self) -> str:
        """The canonical identifier as a string."""
        raise NotImplementedError("Please implement")

    @abstractmethod
    def award_url(self) -> str | None:
        """Returns the URL for the award."""
        raise NotImplementedError("Please implement")

    @cached_property
    def all_variants(self) -> list[str]:
        """Get all variants of the award ID, including related awards.

        Returns:
            list[str]: A list of all variant strings.
        """
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
        """The parts that make up the ID.

        Returns:
            list[IdentifierPart]: A list of IdentifierPart objects.
        """
        parts = []
        for field in self.fields:
            value = getattr(self, field)
            parts.append(IdentifierPart(value, field))
        return parts

    def __eq__(self, other):
        """Check equality with another object.

        Args:
            other: The object to compare with.

        Returns:
            bool: True if equal, False otherwise.
        """
        if not isinstance(other, self.__class__):
            return False

        return all(getattr(self, field) == getattr(other, field) for field in self.fields)

    def __hash__(self):
        """Calculate the hash of the object.

        Returns:
            int: The hash value.
        """
        values = [getattr(self, field) for field in self.fields]
        return hash(tuple(values))

    def __repr__(self):
        """Return a string representation of the object.

        Returns:
            str: The string representation.
        """
        class_name = self.__class__.__name__
        attrs = ", ".join(f"{field}={getattr(self, field)!r}" for field in self.fields)
        return f"{class_name}({attrs})"

    @classmethod
    def from_dict(cls, dict_: dict) -> AwardID:
        """Construct an AwardID from a dict.

        Args:
            dict_: The dictionary containing award ID data.

        Returns:
            AwardID: An instance of AwardID or a subclass.

        Raises:
            TypeError: If the class specified in the dict is not a subclass of AwardID.
        """
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
        """Converts the Award ID into a dict.

        Returns:
            dict: A dictionary representation of the AwardID.
        """
        return {
            "class": f"{self.__class__.__module__}.{self.__class__.__name__}",
            "parts": [part.to_dict() for part in self.parts()],
            "related_awards": [award.to_dict() for award in self.related_awards],
        }


@dataclass
class Identifier:
    """Represents an identifier.

    Attributes:
        id: The identifier string.
        type: The type of the identifier.
    """

    id: str
    type: str

    @classmethod
    def from_dict(cls, dict_) -> Identifier:
        """Create an Identifier from a dictionary.

        Args:
            dict_: The dictionary containing identifier data.

        Returns:
            Identifier: An Identifier object.
        """
        return Identifier(
            dict_.get("id"),
            dict_.get("type"),
        )

    def to_dict(self) -> dict:
        """Convert the Identifier to a dictionary.

        Returns:
            dict: A dictionary representation of the Identifier.
        """
        return {
            "id": self.id,
            "type": self.type,
        }


@dataclass
class IdentifierPart:
    """Represents a part of an identifier.

    Attributes:
        value: The value of the part.
        type: The type of the part.
    """

    value: str
    type: str

    @classmethod
    def from_dict(cls, dict_) -> IdentifierPart:
        """Create an IdentifierPart from a dictionary.

        Args:
            dict_: The dictionary containing identifier part data.

        Returns:
            IdentifierPart: An IdentifierPart object.
        """
        return IdentifierPart(
            dict_.get("value"),
            dict_.get("type"),
        )

    def to_dict(self) -> dict:
        """Convert the IdentifierPart to a dictionary.

        Returns:
            dict: A dictionary representation of the IdentifierPart.
        """
        return {
            "value": self.value,
            "type": self.type,
        }
