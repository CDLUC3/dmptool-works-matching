from __future__ import annotations

import json
import pathlib

from pydantic import TypeAdapter

from dmpworks.model.common import Institution


def load_institutions(file_path: pathlib.Path) -> list[Institution]:
    if not file_path.exists():
        raise FileNotFoundError(f"Could not load institutions, file does not exist: {file_path}")

    try:
        with open(file_path) as f:
            json_data = json.load(f)
            institutions_list_adapter = TypeAdapter(list[Institution])
            return institutions_list_adapter.validate_python(json_data)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON provided") from e


def load_dois(file_path: pathlib.Path) -> list[str]:
    if not file_path.exists():
        raise FileNotFoundError(f"Could not load DOIs, file does not exist: {file_path}")

    try:
        with open(file_path) as f:
            json_data = json.load(f)
            str_list_adapter = TypeAdapter(list[str])
            return str_list_adapter.validate_python(json_data)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON provided") from e
