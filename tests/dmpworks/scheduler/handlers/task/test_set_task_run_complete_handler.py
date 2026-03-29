"""Unit tests for set_task_run_complete_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.task.set_task_run_complete_handler import set_task_run_complete_handler

PATCH_BASE = "dmpworks.scheduler.handler.task.set_task_run_complete_handler"

BASE_EVENT = {
    "workflow_key": "openalex-works",
    "release_date": "2025-01-15",
    "task_type": "download",
    "current": {
        "run_name": "openalex-works-download",
        "run_id": "20250115T060000-aabbccdd",
    },
}


class TestMarkCompleted:
    """set_task_run_complete_handler marks task COMPLETED and writes checkpoint."""

    def test_sets_status_and_checkpoint(self):
        with (
            patch(f"{PATCH_BASE}.set_task_run_status") as mock_status,
            patch(f"{PATCH_BASE}.set_task_checkpoint") as mock_checkpoint,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = set_task_run_complete_handler(BASE_EVENT, None)

        mock_status.assert_called_once_with(
            run_name="openalex-works-download",
            run_id="20250115T060000-aabbccdd",
            status="COMPLETED",
        )
        mock_checkpoint.assert_called_once_with(
            workflow_key="openalex-works",
            task_name="download",
            date="2025-01-15",
            run_id="20250115T060000-aabbccdd",
        )
        assert result is BASE_EVENT
