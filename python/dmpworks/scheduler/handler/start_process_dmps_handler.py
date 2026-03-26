"""Lambda entry point for starting the process-dmps Step Functions execution on a schedule."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

import boto3
import pendulum

from dmpworks.scheduler.batch_params import generate_run_id
from dmpworks.scheduler.config import StartProcessDMPsEnvSettings

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)


def start_process_dmps_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    """Start the process-dmps Step Functions execution for today's date.

    Uses today's UTC date as run_date, then starts the ProcessDmpsStateMachine.
    Sets run_all_dmps to False so the modification window filter applies.

    Args:
        event: EventBridge scheduled event (passed through; no required fields).
        context: Lambda context.

    Returns:
        Dict with execution_arn and run_date.
    """
    settings = StartProcessDMPsEnvSettings()

    run_date = pendulum.now("UTC").to_date_string()
    run_id = generate_run_id()
    execution_name = f"process-dmps-{run_date}-{run_id}"

    log.info(f"Starting process-dmps execution: run_date={run_date} execution_name={execution_name}")

    sfn = boto3.client("stepfunctions")
    response = sfn.start_execution(
        stateMachineArn=settings.state_machine_arn,
        name=execution_name,
        input=json.dumps(
            {
                "workflow_key": "process-dmps",
                "publication_date": run_date,
                "run_date": run_date,
                "aws_env": settings.aws_env,
                "bucket_name": settings.bucket_name,
                "run_all_dmps": False,
                "skip_sync_dmps": False,
                "skip_enrich_dmps": False,
                "skip_dmp_works_search": False,
                "skip_merge_related_works": False,
            }
        ),
    )

    log.info(f"Started process-dmps execution: run_date={run_date} executionArn={response['executionArn']}")
    return {"execution_arn": response["executionArn"], "run_date": run_date}
