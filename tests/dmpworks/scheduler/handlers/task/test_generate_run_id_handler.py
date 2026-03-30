"""Tests for generate_run_id_handler."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dmpworks.scheduler.handler.task.generate_run_id_handler import generate_run_id_handler

RUN_ID = "20260328T120000-a1b2c3d4"


class TestGenerateRunIdHandler:
    @pytest.mark.parametrize(
        "event, expected_execution_name",
        [
            pytest.param(
                {"task_name": "sync-dmps", "date": "2026-03-28", "workflow_prefix": "process-dmps"},
                "process-dmps-sync-dmps-2026-03-28-20260328T120000-a1b2c3d4",
                id="with-task-name",
            ),
            pytest.param(
                {"date": "2026-03-28", "workflow_prefix": "process-dmps"},
                "process-dmps-2026-03-28-20260328T120000-a1b2c3d4",
                id="without-task-name",
            ),
        ],
    )
    @patch("dmpworks.scheduler.handler.task.generate_run_id_handler.LambdaEnvSettings")
    @patch(
        "dmpworks.scheduler.handler.task.generate_run_id_handler.generate_run_id",
        return_value=RUN_ID,
    )
    def test_generates_execution_name(self, mock_run_id, mock_env, event, expected_execution_name):
        result = generate_run_id_handler(event, None)

        assert result["run_id"] == RUN_ID
        assert result["execution_name"] == expected_execution_name
