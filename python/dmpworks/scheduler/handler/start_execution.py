"""Shared helper for starting Step Functions executions from scheduled handlers."""

from __future__ import annotations

import json
import logging
from typing import Any

import boto3

from dmpworks.scheduler.batch_params import generate_run_id
from dmpworks.scheduler.config import StartProcessEnvSettings

log = logging.getLogger(__name__)


def start_execution(*, workflow_key: str, release_date: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Start a Step Functions execution for a scheduled workflow.

    Loads environment settings, generates a unique run ID, and starts
    the state machine with the given payload merged with common fields
    (workflow_key, release_date, aws_env, bucket_name).

    Args:
        workflow_key: Workflow identifier, e.g. "process-works".
        release_date: ISO date string for this run.
        payload: Workflow-specific fields to include in the execution input.

    Returns:
        Dict with execution_arn and release_date.
    """
    settings = StartProcessEnvSettings()
    run_id = generate_run_id()
    execution_name = f"{workflow_key}-{release_date}-{run_id}"

    log.info(f"Starting {workflow_key} execution: release_date={release_date} execution_name={execution_name}")

    sfn = boto3.client("stepfunctions")
    response = sfn.start_execution(
        stateMachineArn=settings.state_machine_arn,
        name=execution_name,
        input=json.dumps(
            {
                "workflow_key": workflow_key,
                "release_date": release_date,
                "aws_env": settings.aws_env,
                "bucket_name": settings.bucket_name,
                **payload,
            }
        ),
    )

    log.info(f"Started {workflow_key} execution: release_date={release_date} executionArn={response['executionArn']}")
    return {"execution_arn": response["executionArn"], "release_date": release_date}
