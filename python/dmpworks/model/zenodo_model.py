from __future__ import annotations

import pendulum
from pydantic import BaseModel, Field, field_serializer, field_validator

from dmpworks.model.common import parse_pendulum_date, serialize_pendulum_date


class ZenodoFile(BaseModel):
    """A file entry in a Zenodo record.

    Attributes:
        link: Download link for the file.
        file_hash: MD5 checksum string (md5:...).
        filename: File name.
        file_type: File type/extension.
    """

    link: str | None = None
    file_hash: str | None = None
    file_name: str | None = None
    file_type: str | None = None


class ZenodoRecord(BaseModel):
    """A Zenodo record with publication date and file list.

    Attributes:
        publication_date: Publication date of the record.
        files: List of files attached to this record.
    """

    model_config = {"arbitrary_types_allowed": True}

    publication_date: pendulum.Date
    files: list[ZenodoFile] = Field(default_factory=list)

    @field_validator("publication_date", mode="before")
    @classmethod
    def parse_publication_date(cls, v):
        """Parse a date string or object into a pendulum.Date.

        Args:
            v: The value to parse.

        Returns:
            pendulum.Date: The parsed date.
        """
        return parse_pendulum_date(v)

    @field_serializer("publication_date")
    def serialize_publication_date(self, v: pendulum.Date) -> str | None:
        """Serialize the publication date to a YYYY-MM-DD string.

        Args:
            v: The pendulum.Date to serialize.

        Returns:
            str: The serialized date string.
        """
        return serialize_pendulum_date(v)
