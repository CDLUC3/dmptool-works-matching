"""Unit tests for start_process_works_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.works.start_process_works_handler import (
    start_process_works_handler,
)
import pendulum

PATCH_BASE = "dmpworks.scheduler.handler.works.start_process_works_handler"


class TestStartExecution:
    """start_process_works_handler computes the release date and delegates to start_execution."""

    def test_passes_todays_date_and_payload(self):
        with (
            patch(f"{PATCH_BASE}.pendulum") as mock_pendulum,
            patch(
                f"{PATCH_BASE}.start_execution",
                return_value={
                    "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-process-works:exec-1",
                    "release_date": "2025-01-15",
                },
            ) as mock_start,
        ):
            mock_pendulum.now.return_value = pendulum.parse("2025-01-15T16:00:00", tz="UTC")
            result = start_process_works_handler({}, None)

        mock_start.assert_called_once_with(
            workflow_key="process-works",
            release_date="2025-01-15",
            payload={
                "skip_sqlmesh": False,
                "skip_sync_works": False,
                "start_process_dmps": True,
                "run_all_dmps": True,
                "skip_sync_dmps": False,
                "skip_enrich_dmps": False,
                "skip_dmp_works_search": False,
                "skip_merge_related_works": False,
            },
        )
        assert (
            result["execution_arn"]
            == "arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-process-works:exec-1"
        )
        assert result["release_date"] == "2025-01-15"
