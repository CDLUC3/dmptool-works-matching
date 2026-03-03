import json
import logging
from importlib.resources import files
from typing import Any

import opensearchpy
from opensearchpy import OpenSearch

MAPPINGS_PACKAGE = "dmpworks.opensearch.mappings"


def load_mapping(mapping_filename: str) -> dict[str, Any]:
    """Load an OpenSearch mapping from a file in the mappings package.

    Args:
        mapping_filename: The name of the mapping file.

    Returns:
        dict[str, Any]: The loaded mapping as a dictionary.

    Raises:
        FileNotFoundError: If the mapping file is not found.
    """
    resource = files(MAPPINGS_PACKAGE) / mapping_filename

    # Validate mapping file
    if not resource.is_file():
        raise FileNotFoundError(f"mapping {mapping_filename} not found in {MAPPINGS_PACKAGE} package resources")

    # Load mapping
    with resource.open("r", encoding="utf-8") as f:
        return json.load(f)


def create_index(client: OpenSearch, index_name: str, mapping_filename: str):
    """Create an OpenSearch index with the specified mapping.

    Args:
        client: The OpenSearch client.
        index_name: The name of the index to create.
        mapping_filename: The name of the mapping file to use.

    Raises:
        opensearchpy.exceptions.RequestError: If the index creation fails (except if it already exists).
    """
    mapping = load_mapping(mapping_filename)
    try:
        response = client.indices.create(index=index_name, body=mapping)
        logging.info(response)
    except opensearchpy.exceptions.RequestError as e:
        if e.status_code == 400 and e.error == "resource_already_exists_exception":
            logging.warning(f"Index already exists: {index_name}")
        else:
            raise e


def update_mapping(client: OpenSearch, index_name: str, mapping_filename: str):
    """Update the mapping of an existing OpenSearch index.

    Args:
        client: The OpenSearch client.
        index_name: The name of the index to update.
        mapping_filename: The name of the mapping file to use.
    """
    data = load_mapping(mapping_filename)
    mappings = data.get("mappings", {})
    response = client.indices.put_mapping(index=index_name, body=mappings)
    logging.info(response)
