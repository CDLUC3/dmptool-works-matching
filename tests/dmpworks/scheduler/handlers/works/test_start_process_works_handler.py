"""Unit tests for start_process_works_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.works.start_process_works_handler import (
    second_monday_of_month,
    start_process_works_handler,
)
import pendulum
import pytest

PATCH_BASE = "dmpworks.scheduler.handler.works.start_process_works_handler"


class TestSecondMondayOfMonth:
    """second_monday_of_month returns the correct date for various months."""

    @pytest.mark.parametrize(
        "date_str, expected",
        [
            ("2025-01-15", "2025-01-13"),  # Jan 2025: 1st is Wed → 1st Mon=6th → 2nd Mon=13th
            ("2025-02-01", "2025-02-10"),  # Feb 2025: 1st is Sat → 1st Mon=3rd → 2nd Mon=10th
            ("2025-03-20", "2025-03-10"),  # Mar 2025: 1st is Sat → 1st Mon=3rd → 2nd Mon=10th
            ("2025-09-01", "2025-09-08"),  # Sep 2025: 1st is Mon → 1st Mon=8th → 2nd Mon=8th
            ("2026-06-01", "2026-06-08"),  # Jun 2026: 1st is Mon → 2nd Mon=8th
        ],
        ids=["jan-2025", "feb-2025", "mar-2025", "sep-2025-starts-monday", "jun-2026"],
    )
    def test_computes_correct_date(self, date_str, expected):
        dt = pendulum.parse(date_str, tz="UTC")
        assert second_monday_of_month(dt).to_date_string() == expected


class TestStartExecution:
    """start_process_works_handler computes the release date and delegates to start_execution."""

    def test_passes_second_monday_and_payload(self):
        with (
            patch(f"{PATCH_BASE}.pendulum") as mock_pendulum,
            patch(
                f"{PATCH_BASE}.start_execution",
                return_value={"execution_arn": "arn:test", "release_date": "2025-01-13"},
            ) as mock_start,
        ):
            mock_pendulum.now.return_value = pendulum.parse("2025-01-15T16:00:00", tz="UTC")
            result = start_process_works_handler({}, None)

        mock_start.assert_called_once_with(
            workflow_key="process-works",
            release_date="2025-01-13",
            payload={"skip_sqlmesh": False, "skip_sync_works": False},
        )
        assert result["execution_arn"] == "arn:test"
        assert result["release_date"] == "2025-01-13"
