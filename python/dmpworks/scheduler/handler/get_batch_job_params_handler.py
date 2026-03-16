"""Lambda entry point for computing Batch parameters."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from aws_lambda_powertools.utilities.validation import validator

from dmpworks.scheduler.batch_params import compute_batch_params
from dmpworks.scheduler.config import LambdaEnvSettings, load_lambda_config
from dmpworks.scheduler.dynamodb_store import create_task_run

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)

INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["workflow_key", "task_type", "publication_date", "bucket_name", "aws_env", "execution_arn"],
    "properties": {
        "workflow_key": {"type": "string"},
        "task_type": {"type": "string"},
        "publication_date": {"type": "string"},
        "bucket_name": {"type": "string"},
        "aws_env": {"type": "string"},
        "execution_arn": {"type": "string"},
    },
}

OUTPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": [
        "workflow_key",
        "task_type",
        "publication_date",
        "bucket_name",
        "aws_env",
        "execution_arn",
        "run_id",
        "run_name",
        "batch_params",
    ],
    "properties": {
        "workflow_key": {"type": "string"},
        "task_type": {"type": "string"},
        "publication_date": {"type": "string"},
        "bucket_name": {"type": "string"},
        "aws_env": {"type": "string"},
        "execution_arn": {"type": "string"},
        "run_id": {
            "type": "string",
            "description": "Unique run ID for this stage execution (format: YYYYMMDDTHHmmss-xxxxxxxx).",
        },
        "run_name": {
            "type": "string",
            "description": "Job name from the factory, used as the TaskRunRecord run_name.",
        },
        "batch_params": {
            "type": "object",
            "required": ["JobName", "JobQueue", "JobDefinition", "ContainerOverrides"],
            "properties": {
                "JobName": {"type": "string"},
                "JobQueue": {"type": "string"},
                "JobDefinition": {"type": "string"},
                "ContainerOverrides": {
                    "type": "object",
                    "required": ["Command", "Vcpus", "Memory", "Environment"],
                    "properties": {
                        "Command": {"type": "array", "items": {"type": "string"}},
                        "Vcpus": {"type": "integer"},
                        "Memory": {"type": "integer"},
                        "Environment": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["Name", "Value"],
                                "properties": {
                                    "Name": {"type": "string"},
                                    "Value": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            },
        },
    },
}


@validator(inbound_schema=INPUT_SCHEMA)
def get_batch_job_params_handler(event: dict, context: LambdaContext) -> dict:  # noqa: ARG001
    """Compute Batch params and create a TaskRunRecord (STARTED).

    Args:
        event: Workflow event containing dataset, task_type, publication_date,
            bucket_name, aws_env, execution_arn, and task-type-specific fields.
        context: Lambda context.

    Returns:
        The event merged with run_id, run_name, and batch_params.
    """
    settings = LambdaEnvSettings()
    config = load_lambda_config(settings.aws_env)

    result = compute_batch_params(event, config)
    create_task_run(
        run_name=result["run_name"],
        run_id=result["run_id"],
        execution_arn=event.get("execution_arn", ""),
        metadata={
            "workflow_key": event["workflow_key"],
            "publication_date": event["publication_date"],
        },
    )
    return {**event, **result}
