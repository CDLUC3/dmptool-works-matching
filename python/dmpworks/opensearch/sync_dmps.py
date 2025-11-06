import logging
import pathlib
from typing import Iterator

import pyarrow as pa

from dmpworks.opensearch.sync import sync_docs
from dmpworks.opensearch.utils import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.utils import timed

log = logging.getLogger(__name__)

COLUMNS = [
    "doi",
    "created",
    "registered",
    "modified",
    "title",
    "abstract",
    "project_start",
    "project_end",
    "institutions",
    "authors",
    "funding",
]


def batch_to_work_actions(
    index_name: str,
    batch: pa.RecordBatch,
) -> Iterator[dict]:
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
def sync_dmps(
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
