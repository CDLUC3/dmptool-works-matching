"""Tests for generate_child_run_id_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.generate_child_run_id_handler import generate_child_run_id_handler


class TestGenerateChildRunIdHandler:
    @patch("dmpworks.scheduler.handler.generate_child_run_id_handler.LambdaEnvSettings")
    @patch(
        "dmpworks.scheduler.handler.generate_child_run_id_handler.generate_run_id",
        return_value="20260328T120000-a1b2c3d4",
    )
    def test_returns_run_id_and_execution_name(self, mock_run_id, mock_env):
        event = {"task_name": "sync-dmps", "date": "2026-03-28", "workflow_prefix": "process-dmps"}
        result = generate_child_run_id_handler(event, None)

        assert result["child_run_id"] == "20260328T120000-a1b2c3d4"
        assert result["execution_name"] == "process-dmps-sync-dmps-2026-03-28-20260328T120000-a1b2c3d4"
