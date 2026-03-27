"""Tests for pipeline display helper functions."""

from datetime import UTC, datetime, timedelta

import pytest

from dmpworks.pipeline.display import (
    _cron_to_english,
    _format_duration,
    _next_runs_local,
    _parse_eventbridge_cron,
    _status_style,
)


class TestParseEventbridgeCron:
    """Verify EventBridge cron expression parsing and field stripping."""

    @pytest.mark.parametrize(
        "expression,expected",
        [
            ("cron(0 15 ? * MON-FRI *)", "0 15 ? * MON-FRI"),
            ("cron(0 3 ? * TUE-SAT *)", "0 3 ? * TUE-SAT"),
            ("cron(0 0 L * ? *)", "0 0 L * ?"),
            ("cron(0 16 ? * 2#2 *)", "0 16 ? * 2#2"),
        ],
        ids=["weekday-3pm", "weekday-3am", "last-day-of-month", "second-monday"],
    )
    def test_strips_wrapper_and_year_field(self, expression, expected):
        print(f"expression={expression}")
        assert _parse_eventbridge_cron(expression=expression) == expected

    def test_returns_none_for_non_cron(self):
        assert _parse_eventbridge_cron(expression="rate(1 day)") is None

    def test_returns_none_for_empty(self):
        assert _parse_eventbridge_cron(expression="") is None


class TestCronToEnglish:
    """Verify cron-to-English conversion produces readable UTC descriptions."""

    def test_weekday_schedule(self):
        result = _cron_to_english(expression="cron(0 15 ? * MON-FRI *)")
        assert "UTC" in result
        assert result != "cron(0 15 ? * MON-FRI *)"

    def test_non_cron_returns_raw(self):
        assert _cron_to_english(expression="rate(1 day)") == "rate(1 day)"


class TestNextRunsLocal:
    """Verify next run time computation returns non-empty results for valid cron."""

    def test_produces_runs_for_valid_cron(self):
        result = _next_runs_local(expression="cron(0 15 ? * MON-FRI *)")
        lines = result.strip().split("\n")
        assert len(lines) == 3

    def test_returns_empty_for_non_cron(self):
        assert _next_runs_local(expression="rate(1 day)") == ""


class TestStatusStyle:
    """Verify Rich style mapping for execution statuses."""

    @pytest.mark.parametrize(
        "status,expected",
        [
            ("SUCCEEDED", "green"),
            ("RUNNING", "yellow"),
            ("FAILED", "red"),
            ("TIMED_OUT", "red"),
            ("ABORTED", "red"),
        ],
        ids=["succeeded", "running", "failed", "timed-out", "aborted"],
    )
    def test_returns_correct_style(self, status, expected):
        print(f"status={status}")
        assert _status_style(status=status) == expected


class TestFormatDuration:
    """Verify human-readable duration formatting."""

    @pytest.mark.parametrize(
        "seconds,expected",
        [
            (9000, "2h30m"),
            (3600, "1h0m"),
            (300, "5m"),
            (59, "0m"),
        ],
        ids=["2h30m", "1h-exact", "5m", "under-1m"],
    )
    def test_formats_duration(self, seconds, expected):
        start = datetime(2026, 3, 20, 8, 0, tzinfo=UTC)
        stop = start + timedelta(seconds=seconds)
        print(f"seconds={seconds}")
        assert _format_duration(start=start, stop=stop) == expected

    def test_returns_empty_when_still_running(self):
        start = datetime(2026, 3, 20, 8, 0, tzinfo=UTC)
        assert _format_duration(start=start, stop=None) == ""
