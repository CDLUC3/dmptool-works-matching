"""Unit tests for start_process_dmps_handler."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from dmpworks.scheduler.handler.dmps.start_process_dmps_handler import start_process_dmps_handler
import pendulum
import pytest

PATCH_BASE = "dmpworks.scheduler.handler.dmps.start_process_dmps_handler"


def mock_record(*, status: str, release_date: str, run_id: str = "run-1"):
    """Build a lightweight stand-in for a PynamoDB run record."""
    return SimpleNamespace(status=status, release_date=release_date, run_id=run_id)


class TestStartExecution:
    """Happy path: no active runs, handler starts a new execution."""

    def test_starts_execution_with_today_and_payload(self):
        with (
            patch(f"{PATCH_BASE}.pendulum") as mock_pendulum,
            patch(f"{PATCH_BASE}.get_latest_process_works_run_recent", return_value=None),
            patch(f"{PATCH_BASE}.get_latest_process_dmps_run_recent", return_value=None),
            patch(f"{PATCH_BASE}.get_latest_process_dmps_run", return_value=None),
            patch(
                f"{PATCH_BASE}.start_execution",
                return_value={
                    "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-process-dmps:exec-1",
                    "release_date": "2025-01-15",
                },
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
        assert (
            result["execution_arn"]
            == "arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-process-dmps:exec-1"
        )
        assert result["release_date"] == "2025-01-15"

    def test_runs_when_prior_runs_all_completed(self):
        """A completed works run with a matching completed chained dmps does not block."""
        works = mock_record(status="COMPLETED", release_date="2025-01-13")
        chained = mock_record(status="COMPLETED", release_date="2025-01-13", run_id="chained-1")
        latest_dmps = mock_record(status="COMPLETED", release_date="2025-01-14", run_id="dmps-1")

        with (
            patch(f"{PATCH_BASE}.pendulum") as mock_pendulum,
            patch(f"{PATCH_BASE}.get_latest_process_works_run_recent", return_value=works),
            patch(f"{PATCH_BASE}.get_latest_process_dmps_run_recent", return_value=latest_dmps),
            patch(f"{PATCH_BASE}.get_latest_process_dmps_run", return_value=chained) as mock_chained,
            patch(f"{PATCH_BASE}.start_execution", return_value={"execution_arn": "arn", "release_date": "2025-01-15"}),
        ):
            mock_pendulum.now.return_value = pendulum.parse("2025-01-15T10:00:00", tz="UTC")
            result = start_process_dmps_handler({}, None)

        mock_chained.assert_called_once_with(release_date="2025-01-13")
        assert "skipped" not in result
        assert result["release_date"] == "2025-01-15"

    def test_reuses_latest_dmps_when_it_is_the_chained_run(self):
        """When latest_dmps.release_date matches works.release_date, no second DDB query is issued."""
        works = mock_record(status="COMPLETED", release_date="2025-01-13")
        latest_dmps = mock_record(status="COMPLETED", release_date="2025-01-13", run_id="chained-1")

        with (
            patch(f"{PATCH_BASE}.pendulum") as mock_pendulum,
            patch(f"{PATCH_BASE}.get_latest_process_works_run_recent", return_value=works),
            patch(f"{PATCH_BASE}.get_latest_process_dmps_run_recent", return_value=latest_dmps),
            patch(f"{PATCH_BASE}.get_latest_process_dmps_run") as mock_chained,
            patch(f"{PATCH_BASE}.start_execution", return_value={"execution_arn": "arn", "release_date": "2025-01-15"}),
        ):
            mock_pendulum.now.return_value = pendulum.parse("2025-01-15T10:00:00", tz="UTC")
            result = start_process_dmps_handler({}, None)

        mock_chained.assert_not_called()
        assert "skipped" not in result


class TestGuardSkips:
    """Guard skips a new execution when a prior run is still in flight or failed."""

    @pytest.mark.parametrize(
        "works_status,dmps_status,chained,expected_reason_prefix",
        [
            ("STARTED", "COMPLETED", "COMPLETED", "process-works STARTED"),
            ("WAITING_FOR_APPROVAL", "COMPLETED", "COMPLETED", "process-works WAITING_FOR_APPROVAL"),
            ("FAILED", "COMPLETED", "COMPLETED", "process-works FAILED"),
            ("COMPLETED", "STARTED", "COMPLETED", "process-dmps STARTED"),
            ("COMPLETED", "FAILED", "COMPLETED", "process-dmps FAILED"),
            ("COMPLETED", "WAITING_FOR_APPROVAL", "COMPLETED", "process-dmps WAITING_FOR_APPROVAL"),
            ("COMPLETED", "COMPLETED", None, "chained process-dmps missing"),
            ("COMPLETED", "COMPLETED", "STARTED", "chained process-dmps STARTED"),
            ("COMPLETED", "COMPLETED", "FAILED", "chained process-dmps FAILED"),
        ],
    )
    def test_skips_with_reason(self, works_status, dmps_status, chained, expected_reason_prefix):
        works = mock_record(status=works_status, release_date="2025-01-13")
        # latest_dmps must not match the works release_date when testing the chained path,
        # otherwise the "latest_dmps BLOCKING" branch can fire first for non-COMPLETED chained cases.
        latest_dmps = mock_record(status=dmps_status, release_date="2025-01-14", run_id="dmps-1")
        chained_record = (
            None if chained is None else mock_record(status=chained, release_date="2025-01-13", run_id="chained-1")
        )

        with (
            patch(f"{PATCH_BASE}.get_latest_process_works_run_recent", return_value=works),
            patch(f"{PATCH_BASE}.get_latest_process_dmps_run_recent", return_value=latest_dmps),
            patch(f"{PATCH_BASE}.get_latest_process_dmps_run", return_value=chained_record),
            patch(f"{PATCH_BASE}.start_execution") as mock_start,
        ):
            result = start_process_dmps_handler({}, None)

        mock_start.assert_not_called()
        assert result["skipped"] is True
        assert result["reason"] == expected_reason_prefix
