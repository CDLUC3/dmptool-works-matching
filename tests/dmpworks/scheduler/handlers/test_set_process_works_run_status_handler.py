"""Unit tests for set_process_works_run_status_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.set_process_works_run_status_handler import set_process_works_run_status_handler

PATCH_BASE = "dmpworks.scheduler.handler.set_process_works_run_status_handler"


class TestMarkStarted:
    """Marking a run as STARTED."""

    def test_calls_set_status_with_started(self):
        event = {
            "run_date": "2025-01-13",
            "run_id": "20250113T060000-aabbccdd",
            "process_works_status": "STARTED",
            "execution_arn": "arn:aws:states:us-east-1:123:execution:sm:abc",
        }

        with (
            patch(f"{PATCH_BASE}.set_process_works_run_status") as mock_set,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = set_process_works_run_status_handler(event, None)

        mock_set.assert_called_once_with(
            run_date="2025-01-13",
            run_id="20250113T060000-aabbccdd",
            status="STARTED",
            step_function_execution_arn="arn:aws:states:us-east-1:123:execution:sm:abc",
        )
        assert result is event

    def test_omits_execution_arn_when_absent(self):
        event = {
            "run_date": "2025-01-13",
            "run_id": "20250113T060000-aabbccdd",
            "process_works_status": "STARTED",
        }

        with (
            patch(f"{PATCH_BASE}.set_process_works_run_status") as mock_set,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            set_process_works_run_status_handler(event, None)

        call_kwargs = mock_set.call_args.kwargs
        assert "step_function_execution_arn" not in call_kwargs


class TestMarkCompleted:
    """Marking a run as COMPLETED."""

    def test_calls_set_status_with_completed(self):
        event = {
            "run_date": "2025-01-13",
            "run_id": "20250113T060000-aabbccdd",
            "process_works_status": "COMPLETED",
        }

        with (
            patch(f"{PATCH_BASE}.set_process_works_run_status") as mock_set,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            set_process_works_run_status_handler(event, None)

        mock_set.assert_called_once_with(
            run_date="2025-01-13",
            run_id="20250113T060000-aabbccdd",
            status="COMPLETED",
        )
