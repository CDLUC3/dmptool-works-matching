"""Unit tests for handle_execution_failure_handler."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from dmpworks.scheduler.handler.handle_execution_failure_handler import handle_execution_failure_handler

PARENT_SM_ARN = "arn:aws:states:us-east-1:123456789012:stateMachine:dmpworks-dev-dataset-ingest"
CHILD_SM_ARN = "arn:aws:states:us-east-1:123456789012:stateMachine:dmpworks-dev-download"
EXECUTION_ARN = "arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-download:abc123"


def make_event(
    *,
    state_machine_arn: str,
    execution_arn: str,
    execution_input: dict,
    cause: str | None = None,
    status: str = "FAILED",
) -> dict:
    """Build a minimal EventBridge Step Functions Execution Status Change event."""
    detail = {
        "executionArn": execution_arn,
        "stateMachineArn": state_machine_arn,
        "status": status,
        "input": json.dumps(execution_input),
    }
    if cause is not None:
        detail["cause"] = cause
    return {"source": "aws.states", "detail-type": "Step Functions Execution Status Change", "detail": detail}


BASE_INPUT = {"workflow_key": "openalex-works", "release_date": "2025-01-01", "aws_env": "dev"}


class TestParentSmFailure:
    """Failure events from the parent state machine."""

    def test_marks_release_failed_and_clears_approval_token(self):
        """Parent SM failure marks the release FAILED, clears any approval token, and does not touch task runs."""
        event = make_event(state_machine_arn=PARENT_SM_ARN, execution_arn=EXECUTION_ARN, execution_input=BASE_INPUT)

        with (
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.update_release_status") as mock_release,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.clear_approval_token") as mock_clear,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.set_task_run_status") as mock_task,
        ):
            handle_execution_failure_handler(event, None)

        mock_release.assert_called_once_with(dataset="openalex-works", release_date="2025-01-01", status="FAILED")
        mock_clear.assert_called_once_with(
            workflow_key="openalex-works", dataset="openalex-works", release_date="2025-01-01"
        )
        mock_task.assert_not_called()


class TestChildSmFailureWithTaskRun:
    """Failure events from a child state machine when a task run record exists."""

    def test_managed_child_skips_release_but_marks_task_run_failed(self):
        """Managed child SM failure (with TaskToken) skips release update but marks task run FAILED."""
        child_input = {**BASE_INPUT, "TaskToken": "token-abc123"}
        event = make_event(
            state_machine_arn=CHILD_SM_ARN,
            execution_arn=EXECUTION_ARN,
            execution_input=child_input,
            cause="States.TaskFailed: Batch job failed",
        )

        mock_task_run = MagicMock()
        mock_task_run.run_name = "openalex-works-download"
        mock_task_run.run_id = "20250101T060000-00abc123"

        with (
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.update_release_status") as mock_release,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.set_task_run_status") as mock_task,
            patch(
                "dmpworks.scheduler.handler.handle_execution_failure_handler.TaskRunRecord"
                ".step_function_execution_arn_index"
            ) as mock_index,
        ):
            mock_index.query.return_value = iter([mock_task_run])
            handle_execution_failure_handler(event, None)

        mock_release.assert_not_called()
        mock_index.query.assert_called_once_with(EXECUTION_ARN)
        mock_task.assert_called_once_with(
            run_name="openalex-works-download",
            run_id="20250101T060000-00abc123",
            status="FAILED",
            error="States.TaskFailed: Batch job failed",
        )


class TestChildSmFailureWithoutTaskRun:
    """Failure events from a child SM when no task run record exists yet."""

    def test_managed_child_without_task_run_skips_release_update(self):
        """Managed child SM failure with no matching task run skips release update (no crash)."""
        child_input = {**BASE_INPUT, "TaskToken": "token-abc123"}
        event = make_event(
            state_machine_arn=CHILD_SM_ARN,
            execution_arn=EXECUTION_ARN,
            execution_input=child_input,
        )

        with (
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.update_release_status") as mock_release,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.set_task_run_status") as mock_task,
            patch(
                "dmpworks.scheduler.handler.handle_execution_failure_handler.TaskRunRecord"
                ".step_function_execution_arn_index"
            ) as mock_index,
        ):
            mock_index.query.return_value = iter([])
            handle_execution_failure_handler(event, None)

        mock_release.assert_not_called()
        mock_task.assert_not_called()


PROCESS_WORKS_INPUT = {"workflow_key": "process-works", "release_date": "2025-01-10", "aws_env": "dev"}


class TestProcessWorksFailure:
    """Failure events from process-works state machines."""

    def test_parent_sm_marks_run_failed_and_clears_approval_token(self):
        """Parent SM failure marks ProcessWorksRunRecord FAILED and clears approval token."""
        works_input = {**PROCESS_WORKS_INPUT, "run_id": "20250110T080000-1234abcd"}
        event = make_event(
            state_machine_arn=PARENT_SM_ARN,
            execution_arn=EXECUTION_ARN,
            execution_input=works_input,
            cause="Execution timed out",
        )

        with (
            patch(
                "dmpworks.scheduler.handler.handle_execution_failure_handler.set_process_works_run_status"
            ) as mock_works,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.clear_approval_token") as mock_clear,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.set_task_run_status") as mock_task,
        ):
            handle_execution_failure_handler(event, None)

        mock_works.assert_called_once_with(
            release_date="2025-01-10",
            run_id="20250110T080000-1234abcd",
            status="FAILED",
            error="Execution timed out",
        )
        mock_clear.assert_called_once_with(
            workflow_key="process-works", release_date="2025-01-10", run_id="20250110T080000-1234abcd"
        )
        mock_task.assert_not_called()


PROCESS_DMPS_INPUT = {"workflow_key": "process-dmps", "release_date": "2025-01-15", "aws_env": "dev"}


class TestProcessDmpsFailure:
    """Failure events from process-dmps state machines."""

    def test_parent_sm_with_run_id_marks_run_failed_and_clears_approval_token(self):
        """Parent SM failure with run_id marks ProcessDMPsRunRecord FAILED and clears approval token."""
        dmps_input = {**PROCESS_DMPS_INPUT, "run_id": "20250115T170000-abcd1234"}
        event = make_event(
            state_machine_arn=PARENT_SM_ARN,
            execution_arn=EXECUTION_ARN,
            execution_input=dmps_input,
            cause="Lambda function timed out",
        )

        with (
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.update_release_status") as mock_release,
            patch(
                "dmpworks.scheduler.handler.handle_execution_failure_handler.set_process_dmps_run_status"
            ) as mock_dmps,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.clear_approval_token") as mock_clear,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.set_task_run_status") as mock_task,
        ):
            handle_execution_failure_handler(event, None)

        mock_dmps.assert_called_once_with(
            release_date="2025-01-15",
            run_id="20250115T170000-abcd1234",
            status="FAILED",
            error="Lambda function timed out",
        )
        mock_clear.assert_called_once_with(
            workflow_key="process-dmps", release_date="2025-01-15", run_id="20250115T170000-abcd1234"
        )
        mock_release.assert_not_called()
        mock_task.assert_not_called()

    def test_parent_sm_without_run_id_logs_only(self):
        """Parent SM failure without run_id does not call any DynamoDB status functions."""
        event = make_event(
            state_machine_arn=PARENT_SM_ARN,
            execution_arn=EXECUTION_ARN,
            execution_input=PROCESS_DMPS_INPUT,
        )

        with (
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.update_release_status") as mock_release,
            patch(
                "dmpworks.scheduler.handler.handle_execution_failure_handler.set_process_dmps_run_status"
            ) as mock_dmps,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.set_task_run_status") as mock_task,
        ):
            handle_execution_failure_handler(event, None)

        mock_dmps.assert_not_called()
        mock_release.assert_not_called()
        mock_task.assert_not_called()

    def test_managed_child_skips_run_update_but_marks_task_run_failed(self):
        """Managed child SM failure skips ProcessDMPsRunRecord update but marks TaskRunRecord FAILED."""
        child_input = {**PROCESS_DMPS_INPUT, "run_id": "20250115T170000-abcd1234", "TaskToken": "token-xyz"}
        event = make_event(
            state_machine_arn=CHILD_SM_ARN,
            execution_arn=EXECUTION_ARN,
            execution_input=child_input,
            cause="Batch job failed",
        )

        mock_task_run = MagicMock()
        mock_task_run.run_name = "sync-dmps"
        mock_task_run.run_id = "20250115T170000-abcd1234"

        with (
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.update_release_status") as mock_release,
            patch(
                "dmpworks.scheduler.handler.handle_execution_failure_handler.set_process_dmps_run_status"
            ) as mock_dmps,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.set_task_run_status") as mock_task,
            patch(
                "dmpworks.scheduler.handler.handle_execution_failure_handler.TaskRunRecord"
                ".step_function_execution_arn_index"
            ) as mock_index,
        ):
            mock_index.query.return_value = iter([mock_task_run])
            handle_execution_failure_handler(event, None)

        mock_dmps.assert_not_called()
        mock_release.assert_not_called()
        mock_task.assert_called_once_with(
            run_name="sync-dmps",
            run_id="20250115T170000-abcd1234",
            status="FAILED",
            error="Batch job failed",
        )


class TestAbortedEvents:
    """ABORTED events should pass ABORTED status through to DynamoDB rather than FAILED."""

    @pytest.mark.parametrize(
        "workflow_key,execution_input,mock_target,expected_kwargs",
        [
            (
                "openalex-works",
                {"workflow_key": "openalex-works", "release_date": "2025-01-01", "aws_env": "dev"},
                "dmpworks.scheduler.handler.handle_execution_failure_handler.update_release_status",
                {"dataset": "openalex-works", "release_date": "2025-01-01", "status": "ABORTED"},
            ),
            (
                "process-works",
                {"workflow_key": "process-works", "release_date": "2025-01-10", "aws_env": "dev", "run_id": "run123"},
                "dmpworks.scheduler.handler.handle_execution_failure_handler.set_process_works_run_status",
                {"release_date": "2025-01-10", "run_id": "run123", "status": "ABORTED", "error": None},
            ),
            (
                "process-dmps",
                {"workflow_key": "process-dmps", "release_date": "2025-01-15", "aws_env": "dev", "run_id": "run456"},
                "dmpworks.scheduler.handler.handle_execution_failure_handler.set_process_dmps_run_status",
                {"release_date": "2025-01-15", "run_id": "run456", "status": "ABORTED", "error": None},
            ),
        ],
        ids=["dataset-ingest", "process-works", "process-dmps"],
    )
    def test_aborted_event_passes_aborted_status(self, workflow_key, execution_input, mock_target, expected_kwargs):
        """ABORTED events record ABORTED status in DynamoDB, not FAILED."""
        event = make_event(
            state_machine_arn=PARENT_SM_ARN,
            execution_arn=EXECUTION_ARN,
            execution_input=execution_input,
            status="ABORTED",
        )
        with (
            patch(mock_target) as mock_fn,
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.clear_approval_token"),
        ):
            handle_execution_failure_handler(event, None)
        mock_fn.assert_called_once_with(**expected_kwargs)

    def test_aborted_managed_child_marks_task_run_aborted(self):
        """ABORTED managed child SM marks TaskRunRecord as ABORTED, not FAILED."""
        child_input = {**BASE_INPUT, "TaskToken": "token-abc123"}
        event = make_event(
            state_machine_arn=CHILD_SM_ARN,
            execution_arn=EXECUTION_ARN,
            execution_input=child_input,
            status="ABORTED",
        )
        mock_task_run = MagicMock()
        mock_task_run.run_name = "openalex-works-download"
        mock_task_run.run_id = "20250101T060000-00abc123"

        with (
            patch("dmpworks.scheduler.handler.handle_execution_failure_handler.set_task_run_status") as mock_task,
            patch(
                "dmpworks.scheduler.handler.handle_execution_failure_handler.TaskRunRecord"
                ".step_function_execution_arn_index"
            ) as mock_index,
        ):
            mock_index.query.return_value = iter([mock_task_run])
            handle_execution_failure_handler(event, None)

        mock_task.assert_called_once_with(
            run_name="openalex-works-download",
            run_id="20250101T060000-00abc123",
            status="ABORTED",
            error=None,
        )
