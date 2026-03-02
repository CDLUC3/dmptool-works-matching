from typing import Sequence, Tuple, Optional

__version__: str

def parse_name(
    text: Optional[str],
) -> Tuple[
    Optional[str],  # first_initial
    Optional[str],  # given_name
    Optional[str],  # middle_initials
    Optional[str],  # middle_names
    Optional[str],  # surname
    Optional[str],  # full
]: ...
def revert_inverted_index(text: Optional[bytes]) -> Optional[str]: ...
def strip_markup(text: Optional[str], null_if_equals: Optional[Sequence[str]] = ...) -> Optional[str]: ...
