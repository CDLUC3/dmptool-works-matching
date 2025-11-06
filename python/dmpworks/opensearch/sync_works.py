import logging
import pathlib
from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc

from dmpworks.opensearch.sync import sync_docs
from dmpworks.opensearch.utils import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.utils import timed

log = logging.getLogger(__name__)

COLUMNS = [
    "doi",
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
    "source",
]


def batch_to_work_actions(
    index_name: str,
    batch: pa.RecordBatch,
) -> Iterator[dict]:
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
        yield {
            "_op_type": "update",
            "_index": index_name,
            "_id": doc["doi"],
            "doc": doc,
            "doc_as_upsert": True,
        }


@timed
def sync_works(
    index_name: str,
    in_dir: pathlib.Path,
    client_config: OpenSearchClientConfig,
    sync_config: OpenSearchSyncConfig,
    log_level: int = logging.INFO,
):
    sync_docs(
        index_name=index_name,
        in_dir=in_dir,
        batch_to_actions_func=batch_to_work_actions,
        include_columns=COLUMNS,
        client_config=client_config,
        sync_config=sync_config,
        log_level=log_level,
    )
