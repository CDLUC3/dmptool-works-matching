import json
import logging
from importlib.resources import files
from typing import Any

from opensearchpy import OpenSearch

MAPPINGS_PACKAGE = "dmpworks.opensearch.mappings"


def load_mapping(mapping_filename: str) -> dict[str, Any]:
    resource = files(MAPPINGS_PACKAGE) / mapping_filename

    # Validate mapping file
    if not resource.is_file():
        raise FileNotFoundError(f"mapping {mapping_filename} not found in {MAPPINGS_PACKAGE} package resources")

    # Load mapping
    with resource.open("r", encoding="utf-8") as f:
        return json.load(f)


def create_index(client: OpenSearch, index_name: str, mapping_filename: str):
    mapping = load_mapping(mapping_filename)
    response = client.indices.create(index=index_name, body=mapping)
    logging.info(response)


def update_mapping(client: OpenSearch, index_name: str, mapping_filename: str):
    data = load_mapping(mapping_filename)
    mappings = data.get("mappings", {})
    response = client.indices.put_mapping(index=index_name, body=mappings)
    logging.info(response)
