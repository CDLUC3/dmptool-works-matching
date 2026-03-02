import re
from typing import Any, Optional

import pendulum
from dmpworks.rust import parse_name, ParsedName, strip_markup
from pendulum.exceptions import ParserError


def extract_doi(text: Optional[str]) -> Optional[str]:
    """
    Extract the first DOI found in a string using a regular expression.

    The match is case-insensitive and follows the standard DOI pattern
    beginning with "10.". If a DOI is found, it is normalized using
    `clean_string` before being returned.

    Args:
        text: Input text that may contain a DOI.

    Returns:
        The normalized DOI if found, otherwise None.
    """

    if text is None:
        return None

    pattern = r"10\.[\d.]+/[^\s]+"
    match = re.search(pattern, str(text), re.IGNORECASE)
    if match:
        return clean_string(match.group(0), lower=True)
    return None


def extract_ror(text: Optional[str]) -> Optional[str]:
    """
    Extract first ROR ID from string.

    Args:
        text:

    Returns:
        The normalised ROR ID if found, otherwise None.
    """

    if text is None:
        return None

    pattern = r"0[a-hj-km-np-tv-z|0-9]{6}[0-9]{2}"
    match = re.search(pattern, str(text), re.IGNORECASE)
    if match:
        return clean_string(match.group(0), lower=True)
    return None


def clean_string(value: Optional[str], lower: bool = False) -> Optional[str]:
    """
    Normalize a string by lowercasing and trimming surrounding whitespace.

    Args:
        value: Input string to normalize.
        lower: Whether to lower the string or not.

    Returns:
        The normalized string, or None if the input is None.
    """

    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    return text.lower() if lower else text


def normalise_identifier(identifier: Optional[str]) -> Optional[str]:
    """
    Normalize an identifier by removing any embedded HTTP(S) URL prefixes
    and applying standard string cleaning.

    All occurrences of patterns matching "http(s)://<domain>/" are removed,
    not just a leading prefix. The resulting value is then passed to
    `clean_string` for further normalization.

    Args:
        identifier: Identifier string that may contain one or more URL prefixes.

    Returns:
        The normalized identifier string, or None if the input is None.
    """

    if identifier is None:
        return None

    value = re.sub(
        r"https?://[^/]+/",
        "",
        str(identifier),
        flags=re.IGNORECASE,
    )

    return clean_string(value, lower=True)


def parse_iso8601_calendar_date(date_str: Optional[str]) -> Optional[pendulum.Date]:
    """
    Parse an ISO 8601 calendar date string into a `pendulum.Date`.

    The input is parsed using `pendulum.parse`, and the date component
    is returned. If `date_str` is None or cannot be parsed, None is returned.

    Args:
        date_str: ISO 8601 calendar date string (e.g. "2025-01-01").

    Returns:
        A `pendulum.Date` if parsing succeeds, otherwise None.
    """

    if date_str is None:
        return None

    try:
        return pendulum.parse(str(date_str)).date()
    except (ParserError, ValueError):
        return None


def parse_iso8601_datetime(datetime_str: Optional[str]) -> Optional[pendulum.DateTime]:
    """
    Parse an ISO 8601 datetime string into a UTC-normalized, naive `pendulum.DateTime`.

    The input is parsed using `pendulum.parse`, converted to UTC, and then
    made timezone-naive. If `datetime_str` is None or cannot be parsed,
    None is returned.

    Args:
        datetime_str: ISO 8601 datetime string (e.g. "2025-01-01T00:00:01Z").

    Returns:
        A naive `pendulum.DateTime` in UTC if parsing succeeds, otherwise None.
    """

    if datetime_str is None:
        return None

    try:
        return pendulum.parse(str(datetime_str)).in_timezone("UTC").naive()
    except (ParserError, ValueError):
        return None


def extract_orcid(text: Optional[str]) -> Optional[str]:
    """Extract an ORCID ID from a string using a regex.

    :param text: the text.
    :return: the ORCID ID or None if no ORCID was found.
    """

    if text is None:
        return None

    pattern = r"\d{4}-\d{4}-\d{4}-\d{3}[\dx]"
    match = re.search(pattern, str(text), re.IGNORECASE)
    if match:
        return clean_string(match.group(0), lower=True)
    return None


def to_optional_string(value: Any) -> Optional[str]:
    """Converts a value that should be either a string or None into a string.
    Sometimes values that should be strings are read by simdjson as other types,
    such as integers.

    Args:
        value: a value.

    Returns: a string or None.

    """

    if value is None:
        return None
    return str(value)


def parse_author_name(given_name: Optional[str], surname: Optional[str], full_name: Optional[str] = None) -> ParsedName:
    """Parses an author name"""

    full = clean_string(full_name, lower=False)
    if full:
        return parse_name(full)

    given = clean_string(given_name, lower=False)
    family = clean_string(surname, lower=False)
    if given or family:
        full = " ".join(part for part in (given, family) if part)
        return parse_name(full)

    return ParsedName(*(None,) * 6)


def replace_with_null(value: Optional[str], values: set[str]) -> Optional[str]:
    """
    Strip whitespace and return None if the lowercased value matches any entry
    in `values`.
    """

    if value is None:
        return None

    stripped = str(value).strip()

    return None if stripped.lower() in values else stripped
