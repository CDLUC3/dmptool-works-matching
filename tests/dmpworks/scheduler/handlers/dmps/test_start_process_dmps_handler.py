"""Unit tests for start_process_dmps_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.dmps.start_process_dmps_handler import start_process_dmps_handler
import pendulum

PATCH_BASE = "dmpworks.scheduler.handler.dmps.start_process_dmps_handler"


class TestStartExecution:
    """start_process_dmps_handler computes the release date and delegates to start_execution."""

    def test_passes_today_and_payload(self):
        with (
            patch(f"{PATCH_BASE}.pendulum") as mock_pendulum,
            patch(
                f"{PATCH_BASE}.start_execution",
                return_value={"execution_arn": "arn:test", "release_date": "2025-01-15"},
            ) as mock_start,
        ):
            mock_pendulum.now.return_value = pendulum.parse("2025-01-15T10:00:00", tz="UTC")
            result = start_process_dmps_handler({}, None)

        mock_start.assert_called_once_with(
            workflow_key="process-dmps",
            release_date="2025-01-15",
            payload={
                "run_all_dmps": False,
                "skip_sync_dmps": False,
                "skip_enrich_dmps": False,
                "skip_dmp_works_search": False,
                "skip_merge_related_works": False,
            },
        )
        assert result["execution_arn"] == "arn:test"
        assert result["release_date"] == "2025-01-15"
