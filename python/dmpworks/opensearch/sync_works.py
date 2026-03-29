from collections.abc import Iterator
import logging
import pathlib

import pyarrow as pa
import pyarrow.compute as pc

from dmpworks.cli_utils import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.opensearch.index import create_index
from dmpworks.opensearch.sync import delete_docs, sync_docs
from dmpworks.opensearch.utils import (
    force_index_refresh,
    make_opensearch_client,
    update_refresh_interval,
)
from dmpworks.utils import timed

log = logging.getLogger(__name__)

COLUMNS = [
    "doi",
    "hash",
    "title",
    "abstract_text",
    "work_type",
    "publication_date",
    "updated_date",
    "publication_venue",
    "institutions",
    "authors",
    "funders",
    "awards",
    "relations",
    "source",
]

WORKS_MAPPING_FILE = "works-mapping.json"


def batch_to_work_actions(
    index_name: str,
    batch: pa.RecordBatch,
) -> Iterator[dict]:
    """Convert a batch of work records to OpenSearch actions.

    Args:
        index_name: The name of the works index.
        batch: The batch of work records.

    Yields:
        dict: An OpenSearch bulk action.
    """
    # Convert date and datetimes
    batch = batch.set_column(
        batch.schema.get_field_index("publication_date"),
        "publication_date",
        pc.strftime(batch["publication_date"], format="%Y-%m-%d"),
    )
    batch = batch.set_column(
        batch.schema.get_field_index("updated_date"),
        "updated_date",
        pc.strftime(batch["updated_date"], format="%Y-%m-%dT%H:%MZ"),
    )

    # Create actions
    for i in range(batch.num_rows):
        doc = {name: batch[name][i].as_py() for name in batch.schema.names}
        doi = doc["doi"]
        yield {
            "_op_type": "update",
            "_index": index_name,
            "_id": doi,
            "doc": doc,
            "doc_as_upsert": True,
        }


@timed
def sync_works(
    *,
    index_name: str,
    works_index_export: pathlib.Path,
    doi_state_export: pathlib.Path,
    release_date: str,
    client_config: OpenSearchClientConfig,
    sync_config: OpenSearchSyncConfig,
    log_level: int = logging.INFO,
):
    """Sync works from parquet files to OpenSearch.

    Args:
        index_name: The name of the works index.
        works_index_export: The directory containing the works index export.
        doi_state_export: The directory containing the DOI state export.
        release_date: Release date (YYYY-MM-DD) to filter records by updated_date.
        client_config: The OpenSearch client configuration.
        sync_config: The sync configuration.
        log_level: The logging level.
    """
    client = None

    try:
        # Create index (if it doesn't exist already)
        client = make_opensearch_client(client_config)
        create_index(client, index_name, WORKS_MAPPING_FILE)

        # Disable refresh interval
        update_refresh_interval(
            client=client,
            index=index_name,
            refresh_interval="-1",
        )

        # Upsert new works
        sync_docs(
            index_name=index_name,
            in_dir=works_index_export,
            batch_to_actions_func=batch_to_work_actions,
            include_columns=COLUMNS,
            client_config=client_config,
            sync_config=sync_config,
            log_level=log_level,
        )

        # Delete works
        delete_docs(
            index_name=index_name,
            doi_state_dir=doi_state_export,
            release_date=release_date,
            client_config=client_config,
        )
    finally:
        if client:
            update_refresh_interval(
                client=client,
                index=index_name,
                refresh_interval="180s",
            )
            force_index_refresh(
                client=client,
                index=index_name,
            )
