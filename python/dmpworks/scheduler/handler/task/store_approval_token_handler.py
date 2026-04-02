"""Lambda entry point for storing an approval token when a child SM fails and the parent waits for retry."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from aws_lambda_powertools.utilities.validation import validator

from dmpworks.scheduler.config import LambdaEnvSettings
from dmpworks.scheduler.dynamodb_store import (
    set_process_dmps_run_status,
    set_process_works_run_status,
    update_release_status,
)

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)

INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["workflow_key", "task_name", "approval_token"],
    "properties": {
        "workflow_key": {"type": "string"},
        "task_name": {"type": "string"},
        "approval_token": {"type": "string"},
        "release_date": {"type": "string"},
        "run_id": {"type": "string"},
        "dataset": {"type": "string"},
    },
}


@validator(inbound_schema=INPUT_SCHEMA)
def store_approval_token_handler(event: dict, context: LambdaContext) -> dict:  # noqa: ARG001
    """Store an approval token in the appropriate run record so the CLI can resume the parent.

    Routes to the correct DynamoDB model based on workflow_key:
    - "process-dmps" -> ProcessDMPsRunRecord (keyed by release_date, run_id)
    - "process-works" -> ProcessWorksRunRecord (keyed by release_date, run_id)
    - dataset workflows -> DatasetReleaseRecord (keyed by dataset, release_date)

    Args:
        event: Dict with workflow_key, task_name, approval_token, and primary key fields.
        context: Lambda context.

    Returns:
        The unmodified event dict.
    """
    LambdaEnvSettings()
    workflow_key = event["workflow_key"]
    task_name = event["task_name"]
    approval_token = event["approval_token"]

    if workflow_key == "process-dmps":
        set_process_dmps_run_status(
            release_date=event["release_date"],
            run_id=event["run_id"],
            status="WAITING_FOR_APPROVAL",
            approval_token=approval_token,
            approval_task_name=task_name,
        )
    elif workflow_key == "process-works":
        set_process_works_run_status(
            release_date=event["release_date"],
            run_id=event["run_id"],
            status="WAITING_FOR_APPROVAL",
            approval_token=approval_token,
            approval_task_name=task_name,
        )
    else:
        update_release_status(
            dataset=event["workflow_key"],
            release_date=event["release_date"],
            status="WAITING_FOR_APPROVAL",
            approval_token=approval_token,
            approval_task_name=task_name,
        )

    log.info(f"Stored approval token for {workflow_key}/{task_name}")
    return event
