"""Lambda entry point for starting the process-works Step Functions execution on a schedule."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

import boto3
import pendulum

from dmpworks.scheduler.batch_params import generate_run_id
from dmpworks.scheduler.config import StartProcessWorksEnvSettings

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)


def second_monday_of_month(dt: pendulum.DateTime) -> pendulum.DateTime:
    """Return the second Monday of the month containing the given datetime.

    Args:
        dt: Any datetime within the target month.

    Returns:
        A datetime on the second Monday of that month.
    """
    first_day = dt.start_of("month")
    # pendulum day_of_week: 0=Monday ... 6=Sunday
    days_until_monday = (7 - first_day.day_of_week) % 7
    first_monday = first_day.add(days=days_until_monday)
    return first_monday.add(weeks=1)


def start_process_works_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    """Start the process-works Step Functions execution for the second Monday of the current month.

    Computes the second Monday of the current month as the release_date,
    then starts the ProcessWorksStateMachine.

    Args:
        event: EventBridge scheduled event (passed through; no required fields).
        context: Lambda context.

    Returns:
        Dict with execution_arn and release_date.
    """
    settings = StartProcessWorksEnvSettings()

    now = pendulum.now("UTC")
    release_date = second_monday_of_month(now).to_date_string()

    run_id = generate_run_id()
    execution_name = f"process-works-{release_date}-{run_id}"

    log.info(f"Starting process-works execution: release_date={release_date} execution_name={execution_name}")

    sfn = boto3.client("stepfunctions")
    response = sfn.start_execution(
        stateMachineArn=settings.state_machine_arn,
        name=execution_name,
        input=json.dumps(
            {
                "workflow_key": "process-works",
                "release_date": release_date,
                "aws_env": settings.aws_env,
                "bucket_name": settings.bucket_name,
                "skip_sqlmesh": False,
                "skip_sync_works": False,
            }
        ),
    )

    log.info(f"Started process-works execution: release_date={release_date} executionArn={response['executionArn']}")
    return {"execution_arn": response["executionArn"], "release_date": release_date}
