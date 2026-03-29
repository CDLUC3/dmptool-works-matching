"""Unit tests for create_process_dmps_run_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.dmps.create_process_dmps_run_handler import create_process_dmps_run_handler

PATCH_BASE = "dmpworks.scheduler.handler.dmps.create_process_dmps_run_handler"

BASE_EVENT = {
    "release_date": "2025-01-15",
    "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-process-dmps:abc123",
    "aws_env": "dev",
}


class TestCreateRun:
    """create_process_dmps_run_handler creates the DynamoDB record and returns run_id."""

    def test_calls_create_with_correct_args(self):
        with (
            patch(f"{PATCH_BASE}.create_process_dmps_run") as mock_create,
            patch(f"{PATCH_BASE}.generate_run_id", return_value="20250115T060000-aabbccdd"),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = create_process_dmps_run_handler(BASE_EVENT, None)

        mock_create.assert_called_once_with(
            release_date="2025-01-15",
            run_id="20250115T060000-aabbccdd",
            execution_arn="arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-process-dmps:abc123",
        )

    def test_returns_event_merged_with_run_id(self):
        with (
            patch(f"{PATCH_BASE}.create_process_dmps_run"),
            patch(f"{PATCH_BASE}.generate_run_id", return_value="20250115T060000-aabbccdd"),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = create_process_dmps_run_handler(BASE_EVENT, None)

        assert result["run_id"] == "20250115T060000-aabbccdd"
        assert result["release_date"] == "2025-01-15"
        assert result["execution_arn"] == BASE_EVENT["execution_arn"]
