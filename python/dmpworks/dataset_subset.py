from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pydantic import TypeAdapter

from dmpworks.model.common import Institution

if TYPE_CHECKING:
    import pathlib


def load_institutions(file_path: pathlib.Path) -> list[Institution]:
    """Load a list of institutions from a JSON file.

    Args:
        file_path: The path to the JSON file containing institution data.

    Returns:
        A list of Institution objects.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file content is not valid JSON.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Could not load institutions, file does not exist: {file_path}")

    try:
        with file_path.open() as f:
            json_data = json.load(f)
            institutions_list_adapter = TypeAdapter(list[Institution])
            return institutions_list_adapter.validate_python(json_data)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON provided") from e


def load_dois(file_path: pathlib.Path) -> list[str]:
    """Load a list of DOIs from a JSON file.

    Args:
        file_path: The path to the JSON file containing DOI strings.

    Returns:
        A list of DOI strings.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file content is not valid JSON.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Could not load DOIs, file does not exist: {file_path}")

    try:
        with file_path.open() as f:
            json_data = json.load(f)
            str_list_adapter = TypeAdapter(list[str])
            return str_list_adapter.validate_python(json_data)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON provided") from e
