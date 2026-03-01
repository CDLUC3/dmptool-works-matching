from __future__ import annotations

from typing import Optional, NamedTuple

from ._internal import __version__ as __version__
from ._internal import parse_name as _parse_name
from ._internal import strip_markup
from ._internal import revert_inverted_index


class ParsedName(NamedTuple):
    first_initial: Optional[str]
    given_name: Optional[str]
    middle_initials: Optional[str]
    middle_names: Optional[str]
    surname: Optional[str]
    full: Optional[str]


def parse_name(
    raw_given_name: Optional[str] = None,
    raw_surname: Optional[str] = None,
    raw_full: Optional[str] = None,
) -> ParsedName:
    return ParsedName(*_parse_name(raw_given_name, raw_surname, raw_full))


__all__ = ["__version__", "parse_name", "strip_markup", "revert_inverted_index"]
