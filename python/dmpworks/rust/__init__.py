from __future__ import annotations

from typing import NamedTuple

from ._internal import (
    __version__,
    has_alphabetic_initials,
    parse_name as _parse_name,
    revert_inverted_index,
    strip_markup,
)


class ParsedName(NamedTuple):
    first_initial: str | None
    given_name: str | None
    middle_initials: str | None
    middle_names: str | None
    surname: str | None
    full: str | None


def parse_name(
    raw_given_name: str | None = None,
    raw_surname: str | None = None,
    raw_full: str | None = None,
) -> ParsedName:
    """Parse a name into its components.

    Args:
        raw_given_name: The raw given name.
        raw_surname: The raw surname.
        raw_full: The raw full name.

    Returns:
        ParsedName: A named tuple containing the parsed name components.
    """
    return ParsedName(*_parse_name(raw_given_name, raw_surname, raw_full))


__all__ = [
    "__version__",
    "has_alphabetic_initials",
    "parse_name",
    "revert_inverted_index",
    "strip_markup",
]
