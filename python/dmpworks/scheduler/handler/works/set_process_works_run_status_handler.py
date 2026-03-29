"""Lambda entry point for setting ProcessWorksRunRecord status."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from dmpworks.scheduler.config import LambdaEnvSettings
from dmpworks.scheduler.dynamodb_store import set_process_works_run_status

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)


def set_process_works_run_status_handler(
    event: dict[str, Any], context: LambdaContext  # noqa: ARG001
) -> dict[str, Any]:
    """Set the status of a ProcessWorksRunRecord.

    Args:
        event: Workflow event containing release_date, run_id, process_works_status,
            and optionally execution_arn.
        context: Lambda context.

    Returns:
        The unmodified event dict.
    """
    LambdaEnvSettings()

    release_date = event["release_date"]
    run_id = event["run_id"]
    status = event["process_works_status"]

    kwargs: dict[str, Any] = {}
    if execution_arn := event.get("execution_arn"):
        kwargs["step_function_execution_arn"] = execution_arn

    log.info(f"Marking process works run status: release_date={release_date} run_id={run_id} status={status}")
    set_process_works_run_status(release_date=release_date, run_id=run_id, status=status, **kwargs)

    return event
