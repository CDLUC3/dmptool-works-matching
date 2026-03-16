"""Lambda entry point for checking whether all prerequisite datasets are ready for process-works."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from dmpworks.scheduler.config import LambdaEnvSettings
from dmpworks.scheduler.dynamodb_store import (
    SQLMESH_INITIAL_RUN_ID,
    get_latest_known_release,
    get_task_checkpoint,
)

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)

REQUIRED_CHECKPOINTS: dict[str, tuple[str, str]] = {
    "run_id_openalex_works": ("openalex-works", "transform"),
    "run_id_datacite": ("datacite", "transform"),
    "run_id_crossref_metadata": ("crossref-metadata", "transform"),
    "run_id_ror": ("ror", "download"),
    "run_id_data_citation_corpus": ("data-citation-corpus", "download"),
}


def check_datasets_ready_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    """Check whether all 5 required datasets have completed TaskCheckpoints and are not in-flight.

    For each required dataset, verifies that:
    1. A completed TaskCheckpoint exists.
    2. The latest DatasetReleaseRecord is not in STARTED state (i.e., no active ingest run).

    Also looks up the previous SQLMesh checkpoint run_id for incremental runs.

    Args:
        event: Workflow event (passed through; no required fields).
        context: Lambda context.

    Returns:
        If not all datasets are ready: ``{"all_ready": False}``.
        If all ready: ``{"all_ready": True, "run_id_openalex_works": ..., "run_id_datacite": ...,
        "run_id_crossref_metadata": ..., "run_id_ror": ..., "run_id_data_citation_corpus": ...,
        "publication_date_openalex_works": ..., "publication_date_datacite": ...,
        "publication_date_crossref_metadata": ..., "publication_date_ror": ...,
        "publication_date_data_citation_corpus": ..., "run_id_sqlmesh_prev": ...}``.
    """
    LambdaEnvSettings()

    run_ids: dict[str, str] = {}

    for pool_key, (workflow_key, task_name) in REQUIRED_CHECKPOINTS.items():
        checkpoint = get_task_checkpoint(workflow_key=workflow_key, task_name=task_name)
        if checkpoint is None:
            log.info(f"Dataset not ready — no checkpoint: workflow_key={workflow_key} task_name={task_name}")
            return {"all_ready": False}

        latest_release = get_latest_known_release(dataset=workflow_key)
        if latest_release is not None and latest_release.status == "STARTED":
            log.info(f"Dataset not ready — ingest in-flight: workflow_key={workflow_key}")
            return {"all_ready": False}

        run_ids[pool_key] = checkpoint.run_id
        pub_date_key = pool_key.replace("run_id_", "publication_date_", 1)
        run_ids[pub_date_key] = checkpoint.task_key.split("#")[1]
        log.info(
            f"Dataset ready: workflow_key={workflow_key} task_name={task_name} "
            f"run_id={checkpoint.run_id} publication_date={run_ids[pub_date_key]}"
        )

    sqlmesh_checkpoint = get_task_checkpoint(workflow_key="process-works", task_name="sqlmesh")
    run_id_sqlmesh_prev = sqlmesh_checkpoint.run_id if sqlmesh_checkpoint else SQLMESH_INITIAL_RUN_ID
    log.info(f"Previous SQLMesh run_id: {run_id_sqlmesh_prev}")

    return {"all_ready": True, **run_ids, "run_id_sqlmesh_prev": run_id_sqlmesh_prev}
