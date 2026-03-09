from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pydantic import TypeAdapter

from dmpworks.model.common import Institution
from dmpworks.transform.simdjson_transforms import extract_doi, extract_ror

if TYPE_CHECKING:
    import pathlib


def load_institutions(file_path: pathlib.Path) -> list[Institution]:
    """Load a list of institutions from a JSON file.

    ROR IDs are extracted and normalised using extract_ror. Institutions where
    both name and ror are None after processing are excluded.

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
            institutions = institutions_list_adapter.validate_python(json_data)
            result = []
            for inst in institutions:
                ror = extract_ror(inst.ror)
                name = inst.name
                if name is None and ror is None:
                    continue
                result.append(Institution(name=name, ror=ror))
            return result
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON provided") from e


def load_dois(file_path: pathlib.Path) -> list[str]:
    """Load a list of DOIs from a JSON file.

    DOIs are extracted and normalised using extract_doi. Entries that do not
    contain a valid DOI are excluded.

    Args:
        file_path: The path to the JSON file containing DOI strings.

    Returns:
        A list of normalised DOI strings.

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
            raw_dois = str_list_adapter.validate_python(json_data)
            return [doi for raw in raw_dois if (doi := extract_doi(raw)) is not None]
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON provided") from e
