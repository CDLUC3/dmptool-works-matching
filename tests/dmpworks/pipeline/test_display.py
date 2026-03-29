"""Tests for pipeline display helper functions."""

from datetime import UTC, datetime, timedelta
from io import StringIO

import pytest
from rich.console import Console

from dmpworks.pipeline.display import (
    build_execution_tree,
    cron_to_english,
    format_duration,
    next_runs_local,
    parse_eventbridge_cron,
    record_status_style,
    status_style,
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
        assert parse_eventbridge_cron(expression=expression) == expected

    def test_returns_none_for_non_cron(self):
        assert parse_eventbridge_cron(expression="rate(1 day)") is None

    def test_returns_none_for_empty(self):
        assert parse_eventbridge_cron(expression="") is None


class TestCronToEnglish:
    """Verify cron-to-English conversion produces readable UTC descriptions."""

    def test_weekday_schedule(self):
        result = cron_to_english(expression="cron(0 15 ? * MON-FRI *)")
        assert "UTC" in result
        assert result != "cron(0 15 ? * MON-FRI *)"

    def test_non_cron_returns_raw(self):
        assert cron_to_english(expression="rate(1 day)") == "rate(1 day)"


class TestNextRunsLocal:
    """Verify next run time computation returns non-empty results for valid cron."""

    def test_produces_runs_for_valid_cron(self):
        result = next_runs_local(expression="cron(0 15 ? * MON-FRI *)")
        lines = result.strip().split("\n")
        assert len(lines) == 3

    def test_returns_empty_for_non_cron(self):
        assert next_runs_local(expression="rate(1 day)") == ""


class TestRecordStatusStyle:
    """Verify Rich style mapping for DynamoDB record statuses."""

    @pytest.mark.parametrize(
        "status,expected",
        [
            ("COMPLETED", "green"),
            ("STARTED", "yellow"),
            ("FAILED", "red"),
            ("ABORTED", "red"),
            ("WAITING_FOR_APPROVAL", ""),
        ],
        ids=["completed", "started", "failed", "aborted", "waiting"],
    )
    def test_returns_correct_style(self, status, expected):
        print(f"status={status}")
        assert record_status_style(status=status) == expected


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
        assert status_style(status=status) == expected


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
        assert format_duration(start=start, stop=stop) == expected

    def test_returns_empty_when_still_running(self):
        start = datetime(2026, 3, 20, 8, 0, tzinfo=UTC)
        assert format_duration(start=start, stop=None) == ""


def make_execution(*, workflow: str = "process-works", name: str = "parent-exec", children: list | None = None) -> dict:
    """Build a minimal execution dict for tree rendering tests."""
    start = datetime(2026, 3, 30, 0, 28, tzinfo=UTC)
    stop = datetime(2026, 3, 30, 0, 35, tzinfo=UTC)
    return {
        "workflow": workflow,
        "name": name,
        "status": "RUNNING",
        "start_date": start,
        "stop_date": None,
        "children": children or [
            {
                "name": "process-works-sqlmesh-child",
                "status": "FAILED",
                "start_date": start,
                "stop_date": stop,
            }
        ],
    }


class TestBuildExecutionTree:
    """Verify build_execution_tree renders retryable child annotations correctly."""

    def _render(self, tree) -> str:
        buf = StringIO()
        Console(file=buf, highlight=False, markup=True).print(tree)
        return buf.getvalue()

    def test_retryable_child_gets_retry_marker(self):
        """A child whose name is in retryable_children should have a retry marker in the output."""
        ex = make_execution()
        tree = build_execution_tree(
            title="Test",
            executions=[ex],
            retryable_children=frozenset({"process-works-sqlmesh-child"}),
        )
        output = self._render(tree)
        assert "retry available" in output
        assert "process-works-sqlmesh-child" in output

    def test_non_retryable_child_has_no_marker(self):
        """Children not in retryable_children should not have a retry marker."""
        ex = make_execution()
        tree = build_execution_tree(title="Test", executions=[ex])
        output = self._render(tree)
        assert "retry available" not in output
        assert "process-works-sqlmesh-child" in output
