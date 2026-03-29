from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pendulum


@dataclass(frozen=True)
class DatasetRelease:
    """Canonical return type for all dataset version detectors.

    Attributes:
        release_date: Provider release date.
        file_name: File to download, if applicable.
        download_url: Direct download URL, if applicable.
        file_hash: MD5 checksum (md5:...) for the file, if applicable.
        metadata: Arbitrary extra key/value pairs for dataset-specific data.
    """

    release_date: pendulum.Date
    file_name: str | None = None
    download_url: str | None = None
    file_hash: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
