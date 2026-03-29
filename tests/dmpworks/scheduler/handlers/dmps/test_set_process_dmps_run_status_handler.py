"""Unit tests for set_process_dmps_run_status_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.dmps.set_process_dmps_run_status_handler import set_process_dmps_run_status_handler

PATCH_BASE = "dmpworks.scheduler.handler.dmps.set_process_dmps_run_status_handler"

BASE_EVENT = {
    "release_date": "2025-01-15",
    "run_id": "20250115T060000-aabbccdd",
}


class TestStatusUpdate:
    """Setting status on a ProcessDMPsRunRecord."""

    def test_passes_status_when_present(self):
        event = {**BASE_EVENT, "process_dmps_status": "COMPLETED"}

        with (
            patch(f"{PATCH_BASE}.set_process_dmps_run_status") as mock_set,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = set_process_dmps_run_status_handler(event, None)

        mock_set.assert_called_once_with(
            release_date="2025-01-15",
            run_id="20250115T060000-aabbccdd",
            status="COMPLETED",
        )
        assert result is event

    def test_passes_task_run_ids(self):
        event = {
            **BASE_EVENT,
            "run_id_sync_dmps": "sync-run",
            "run_id_enrich_dmps": "enrich-run",
        }

        with (
            patch(f"{PATCH_BASE}.set_process_dmps_run_status") as mock_set,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            set_process_dmps_run_status_handler(event, None)

        call_kwargs = mock_set.call_args.kwargs
        assert call_kwargs["run_id_sync_dmps"] == "sync-run"
        assert call_kwargs["run_id_enrich_dmps"] == "enrich-run"
        assert "status" not in call_kwargs

    def test_omits_absent_fields(self):
        with (
            patch(f"{PATCH_BASE}.set_process_dmps_run_status") as mock_set,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            set_process_dmps_run_status_handler(BASE_EVENT, None)

        mock_set.assert_called_once_with(
            release_date="2025-01-15",
            run_id="20250115T060000-aabbccdd",
        )
