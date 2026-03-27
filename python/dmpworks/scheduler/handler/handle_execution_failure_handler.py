"""Lambda entry point for handling Step Functions execution failures via EventBridge."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from dmpworks.scheduler.dynamodb_store import (
    TaskRunRecord,
    set_process_dmps_run_status,
    set_process_works_run_status,
    set_task_run_status,
    update_release_status,
)

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)


def handle_execution_failure_handler(event: dict, context: LambdaContext) -> None:  # noqa: ARG001
    """Mark the release (and task run, if applicable) as FAILED when a Step Functions execution fails.

    Triggered by EventBridge on Step Functions ``ExecutionFailed`` events for all state
    machines (parent and child SMs). Child SMs are identified by the presence of
    ``TaskToken`` in the execution input, which is injected by the parent's
    ``waitForTaskToken`` invocation. For child SM failures the specific TaskRunRecord is
    also marked FAILED via a GSI lookup by execution ARN.

    For process-works executions (workflow_key == "process-works"), marks the
    ProcessWorksRunRecord as FAILED. For process-dmps executions
    (workflow_key == "process-dmps"), marks the ProcessDMPsRunRecord as FAILED.
    All other workflows mark the DatasetReleaseRecord as FAILED.

    Args:
        event: EventBridge ``Step Functions Execution Status Change`` event.
        context: Lambda context.
    """
    detail = event["detail"]
    execution_input = json.loads(detail["input"])
    workflow_key = execution_input["workflow_key"]
    publication_date = execution_input["publication_date"]
    error_message = detail.get("cause")

    log.info(
        f"Execution failed: workflow_key={workflow_key} publication_date={publication_date} executionArn={detail['executionArn']}"
    )

    # Child SMs with a TaskToken are managed by the parent's Catch → approval flow.
    # The parent handles the retry lifecycle, so we skip marking the parent record as FAILED.
    is_managed_child = "TaskToken" in execution_input

    if workflow_key == "process-works":
        task_run_id = execution_input.get("run_id")
        if task_run_id and not is_managed_child:
            log.info(f"Marking process works run FAILED: run_date={publication_date} run_id={task_run_id}")
            set_process_works_run_status(
                run_date=publication_date,
                run_id=task_run_id,
                status="FAILED",
                error=error_message,
            )
        else:
            log.info(f"Skipping parent record update for process-works failure (executionArn={detail['executionArn']})")
    elif workflow_key == "process-dmps":
        task_run_id = execution_input.get("run_id")
        if task_run_id and not is_managed_child:
            log.info(f"Marking process DMPs run FAILED: run_date={publication_date} run_id={task_run_id}")
            set_process_dmps_run_status(
                run_date=publication_date,
                run_id=task_run_id,
                status="FAILED",
                error=error_message,
            )
        else:
            log.info(f"Skipping parent record update for process-dmps failure (executionArn={detail['executionArn']})")
    elif is_managed_child:
        log.info(f"Skipping parent record update for managed child failure (executionArn={detail['executionArn']})")
    else:
        update_release_status(dataset=workflow_key, publication_date=publication_date, status="FAILED")

    if "TaskToken" in execution_input:
        task_run = next(
            TaskRunRecord.step_function_execution_arn_index.query(detail["executionArn"]),
            None,
        )
        if task_run:
            log.info(f"Marking task run FAILED: run_name={task_run.run_name} run_id={task_run.run_id}")
            set_task_run_status(
                run_name=task_run.run_name,
                run_id=task_run.run_id,
                status="FAILED",
                error=error_message,
            )
        else:
            log.info(
                f"No task run found for executionArn={detail['executionArn']} (failed before task run was created)"
            )
