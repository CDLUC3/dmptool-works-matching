"""Lambda entry point for marking jobs COMPLETED."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from aws_lambda_powertools.utilities.validation import validator

from dmpworks.scheduler.config import LambdaEnvSettings
from dmpworks.scheduler.dynamodb_store import set_task_checkpoint, set_task_run_status

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)

INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["workflow_key", "release_date", "task_type", "current"],
    "properties": {
        "workflow_key": {"type": "string"},
        "release_date": {"type": "string"},
        "task_type": {"type": "string"},
        "current": {
            "type": "object",
            "required": ["run_name", "run_id"],
            "properties": {
                "run_name": {"type": "string"},
                "run_id": {"type": "string"},
            },
        },
    },
}

OUTPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["workflow_key", "release_date", "task_type", "current"],
    "properties": {
        "workflow_key": {"type": "string"},
        "release_date": {"type": "string"},
        "task_type": {"type": "string"},
        "current": {
            "type": "object",
            "required": ["run_name", "run_id"],
            "properties": {
                "run_name": {"type": "string"},
                "run_id": {"type": "string"},
            },
        },
    },
}


@validator(inbound_schema=INPUT_SCHEMA)
def set_task_run_complete_handler(event: dict, context: LambdaContext) -> dict:  # noqa: ARG001
    """Mark current task run COMPLETED and write run_id to DatasetReleaseRecord.

    Args:
        event: Workflow event containing dataset, release_date, task_type,
            and current dict with run_name and run_id.
        context: Lambda context.

    Returns:
        The unmodified event dict.
    """
    LambdaEnvSettings()
    set_task_run_status(
        run_name=event["current"]["run_name"],
        run_id=event["current"]["run_id"],
        status="COMPLETED",
    )
    set_task_checkpoint(
        workflow_key=event["workflow_key"],
        task_name=event["task_type"],
        date=event["release_date"],
        run_id=event["current"]["run_id"],
    )
    return event
