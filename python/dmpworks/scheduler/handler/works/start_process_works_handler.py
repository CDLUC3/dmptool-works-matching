"""Lambda entry point for starting the process-works Step Functions execution on a schedule."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

import pendulum

from dmpworks.scheduler.handler.start_execution import start_execution

logging.getLogger().setLevel(logging.INFO)


def start_process_works_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    """Start the process-works Step Functions execution for today's date.

    Uses today's UTC date as the release_date, then starts the ProcessWorksStateMachine.

    Args:
        event: EventBridge scheduled event (passed through; no required fields).
        context: Lambda context.

    Returns:
        Dict with execution_arn and release_date.
    """
    release_date = pendulum.now("UTC").to_date_string()
    return start_execution(
        workflow_key="process-works",
        release_date=release_date,
        payload={
            "skip_sqlmesh": False,
            "skip_sync_works": False,
            "start_process_dmps": True,
            "run_all_dmps": True,
            "skip_sync_dmps": False,
            "skip_enrich_dmps": False,
            "skip_dmp_works_search": False,
            "skip_merge_related_works": False,
        },
    )
