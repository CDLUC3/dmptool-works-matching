"""Unit tests for set_release_status_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.ingest.set_release_status_handler import set_release_status_handler

PATCH_BASE = "dmpworks.scheduler.handler.ingest.set_release_status_handler"

BASE_EVENT = {
    "workflow_key": "openalex-works",
    "release_date": "2025-01-15",
    "release_status": "STARTED",
    "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-ingest:abc123",
}


class TestStarted:
    """Marking a release as STARTED includes execution_arn."""

    def test_passes_execution_arn(self):
        with (
            patch(f"{PATCH_BASE}.update_release_status") as mock_update,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = set_release_status_handler(BASE_EVENT, None)

        mock_update.assert_called_once_with(
            dataset="openalex-works",
            release_date="2025-01-15",
            status="STARTED",
            step_function_execution_arn="arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-ingest:abc123",
        )
        assert result is BASE_EVENT


class TestCompleted:
    """Marking a release as COMPLETED excludes execution_arn."""

    def test_excludes_execution_arn(self):
        event = {**BASE_EVENT, "release_status": "COMPLETED"}

        with (
            patch(f"{PATCH_BASE}.update_release_status") as mock_update,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            set_release_status_handler(event, None)

        call_kwargs = mock_update.call_args.kwargs
        assert "step_function_execution_arn" not in call_kwargs
        mock_update.assert_called_once_with(
            dataset="openalex-works",
            release_date="2025-01-15",
            status="COMPLETED",
        )
