"""Lambda entry point for starting the process-dmps Step Functions execution on a schedule."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

import pendulum

from dmpworks.scheduler.dynamodb_store import (
    get_latest_process_dmps_run,
    get_latest_process_dmps_run_recent,
    get_latest_process_works_run_recent,
)
from dmpworks.scheduler.handler.start_execution import start_execution

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)

BLOCKING_STATUSES = frozenset({"STARTED", "WAITING_FOR_APPROVAL", "FAILED"})


def start_process_dmps_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    """Start the process-dmps Step Functions execution for today's date.

    Uses today's UTC date as the release_date, then starts the ProcessDmpsStateMachine.
    Sets run_all_dmps to False so the modification window filter applies.

    Before starting, checks DynamoDB to ensure no process-works or process-dmps run is
    still in flight. Skips (returns without starting) if:
      - The latest process-works run is in a blocking state (STARTED, WAITING_FOR_APPROVAL, FAILED).
      - The latest process-dmps run is in a blocking state.
      - The latest process-works completed, but its chained process-dmps (matched by
        release_date equality) has not yet completed.

    Args:
        event: EventBridge scheduled event (passed through; no required fields).
        context: Lambda context.

    Returns:
        Dict with execution_arn and release_date, or a dict with skipped=True and a reason
        when the guard prevents a new execution.
    """
    latest_works = get_latest_process_works_run_recent()
    if latest_works is not None and latest_works.status in BLOCKING_STATUSES:
        reason = f"process-works {latest_works.status}"
        log.info(
            f"Skipping process-dmps: {reason} "
            f"(release_date={latest_works.release_date}, run_id={latest_works.run_id})"
        )
        return {"skipped": True, "reason": reason}

    latest_dmps = get_latest_process_dmps_run_recent()
    if latest_dmps is not None and latest_dmps.status in BLOCKING_STATUSES:
        reason = f"process-dmps {latest_dmps.status}"
        log.info(
            f"Skipping process-dmps: previous {reason} "
            f"(release_date={latest_dmps.release_date}, run_id={latest_dmps.run_id})"
        )
        return {"skipped": True, "reason": reason}

    if latest_works is not None and latest_works.status == "COMPLETED":
        if latest_dmps is not None and latest_dmps.release_date == latest_works.release_date:
            chained = latest_dmps
        else:
            chained = get_latest_process_dmps_run(release_date=latest_works.release_date)
        if chained is None or chained.status in BLOCKING_STATUSES:
            chained_status = "missing" if chained is None else chained.status
            reason = f"chained process-dmps {chained_status}"
            log.info(
                f"Skipping process-dmps: chained run for process-works "
                f"release_date={latest_works.release_date} is {chained_status}"
            )
            return {"skipped": True, "reason": reason}

    release_date = pendulum.now("UTC").to_date_string()
    return start_execution(
        workflow_key="process-dmps",
        release_date=release_date,
        payload={
            "run_all_dmps": False,
            "skip_sync_dmps": False,
            "skip_enrich_dmps": False,
            "skip_dmp_works_search": False,
            "skip_merge_related_works": False,
        },
    )
