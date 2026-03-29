"""Lambda entry point for setting ProcessDMPsRunRecord status and task run IDs."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from dmpworks.scheduler.config import LambdaEnvSettings
from dmpworks.scheduler.dynamodb_store import set_process_dmps_run_status

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)


def set_process_dmps_run_status_handler(
    event: dict[str, Any], context: LambdaContext  # noqa: ARG001
) -> dict[str, Any]:
    """Set status and/or task run IDs on a ProcessDMPsRunRecord.

    All fields beyond release_date and run_id are optional — only those present in the
    event are updated. This allows both full status updates and partial task run_id
    recording from a single handler.

    Args:
        event: Workflow event containing release_date, run_id, and optionally
            process_dmps_status, run_id_sync_dmps, run_id_enrich_dmps,
            run_id_dmp_works_search, run_id_merge_related_works.
        context: Lambda context.

    Returns:
        The unmodified event dict.
    """
    LambdaEnvSettings()

    release_date = event["release_date"]
    run_id = event["run_id"]

    kwargs: dict[str, Any] = {}
    if status := event.get("process_dmps_status"):
        kwargs["status"] = status
    if run_id_sync_dmps := event.get("run_id_sync_dmps"):
        kwargs["run_id_sync_dmps"] = run_id_sync_dmps
    if run_id_enrich_dmps := event.get("run_id_enrich_dmps"):
        kwargs["run_id_enrich_dmps"] = run_id_enrich_dmps
    if run_id_dmp_works_search := event.get("run_id_dmp_works_search"):
        kwargs["run_id_dmp_works_search"] = run_id_dmp_works_search
    if run_id_merge_related_works := event.get("run_id_merge_related_works"):
        kwargs["run_id_merge_related_works"] = run_id_merge_related_works

    log.info(f"Updating process DMPs run: release_date={release_date} run_id={run_id} fields={list(kwargs)}")
    set_process_dmps_run_status(release_date=release_date, run_id=run_id, **kwargs)

    return event
