"""Lambda entry point for generating a run ID and execution name."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from aws_lambda_powertools.utilities.validation import validator

from dmpworks.scheduler.batch_params import generate_run_id
from dmpworks.scheduler.config import LambdaEnvSettings

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)

INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["task_name", "date", "workflow_prefix"],
    "properties": {
        "task_name": {"type": "string"},
        "date": {"type": "string"},
        "workflow_prefix": {"type": "string"},
    },
}

OUTPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["run_id", "execution_name"],
    "properties": {
        "run_id": {"type": "string"},
        "execution_name": {"type": "string"},
    },
}


@validator(inbound_schema=INPUT_SCHEMA, outbound_schema=OUTPUT_SCHEMA)
def generate_run_id_handler(event: dict, context: LambdaContext) -> dict:  # noqa: ARG001
    """Generate a unique run ID and execution name for a state machine task.

    The execution name follows the convention: {workflow_prefix}-{task_name}-{date}-{child_run_id}.

    Args:
        event: Dict with task_name, date, and workflow_prefix.
        context: Lambda context.

    Returns:
        Dict with run_id and execution_name.
    """
    LambdaEnvSettings()
    run_id = generate_run_id()
    execution_name = f"{event['workflow_prefix']}-{event['task_name']}-{event['date']}-{run_id}"
    log.info(f"Generated run ID: {run_id}, execution name: {execution_name}")
    return {"run_id": run_id, "execution_name": execution_name}
