from collections.abc import Sequence

__version__: str

def parse_name(
    raw_given_name: str | None = ...,
    raw_surname: str | None = ...,
    raw_full: str | None = ...,
) -> tuple[
    str | None,  # first_initial
    str | None,  # given_name
    str | None,  # middle_initials
    str | None,  # middle_names
    str | None,  # surname
    str | None,  # full
]: ...
def revert_inverted_index(text: bytes | None, null_if_equals: Sequence[str] | None = ...) -> str | None: ...
def strip_markup(text: str | None, null_if_equals: Sequence[str] | None = ...) -> str | None: ...
def has_alphabetic_initials(text: str | None) -> bool: ...
