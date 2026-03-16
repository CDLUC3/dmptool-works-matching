"""Lambda entry point for marking dataset releases as STARTED or COMPLETED."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from aws_lambda_powertools.utilities.validation import validator

from dmpworks.scheduler.config import LambdaEnvSettings
from dmpworks.scheduler.dynamodb_store import update_release_status

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)

INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["workflow_key", "publication_date", "release_status"],
    "properties": {
        "workflow_key": {"type": "string"},
        "publication_date": {"type": "string"},
        "release_status": {"type": "string"},
        "execution_arn": {"type": "string"},
    },
}

OUTPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["workflow_key", "publication_date", "release_status"],
    "properties": {
        "workflow_key": {"type": "string"},
        "publication_date": {"type": "string"},
        "release_status": {"type": "string"},
        "execution_arn": {"type": "string"},
    },
}


@validator(inbound_schema=INPUT_SCHEMA)
def set_release_status_handler(event: dict, context: LambdaContext) -> dict:  # noqa: ARG001
    """Mark a DatasetReleaseRecord with the given status.

    Args:
        event: Workflow event containing dataset, publication_date, status, and
            optionally execution_arn.
        context: Lambda context.

    Returns:
        The unmodified event dict.
    """
    LambdaEnvSettings()
    status = event["release_status"]
    kwargs = {"step_function_execution_arn": event.get("execution_arn", "")} if status == "STARTED" else {}
    update_release_status(
        dataset=event["workflow_key"],
        publication_date=event["publication_date"],
        status=status,
        **kwargs,
    )
    return event
