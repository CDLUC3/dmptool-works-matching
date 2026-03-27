"""Rich table rendering for pipeline status display."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from cron_descriptor import get_description
from croniter import croniter
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

if TYPE_CHECKING:
    from dmpworks.scheduler.dynamodb_store import (
        DatasetReleaseRecord,
        ProcessDMPsRunRecord,
        ProcessWorksRunRecord,
        TaskCheckpointRecord,
    )

console = Console()


def display_dataset_releases(*, records: list[DatasetReleaseRecord]) -> None:
    """Render dataset releases as a rich table.

    Args:
        records: List of DatasetReleaseRecord instances.
    """
    table = Table(title="Dataset Releases")
    table.add_column("Dataset", style="cyan")
    table.add_column("Publication Date")
    table.add_column("Status")
    table.add_column("Download URL", max_width=60)

    for r in sorted(records, key=lambda x: (x.dataset, x.publication_date), reverse=True):
        style = (
            "green"
            if r.status == "COMPLETED"
            else "yellow" if r.status == "STARTED" else "red" if r.status == "FAILED" else ""
        )
        table.add_row(r.dataset, r.publication_date, f"[{style}]{r.status}[/{style}]", r.download_url or "")

    console.print(table)


def display_task_checkpoints(*, records: list[TaskCheckpointRecord], title: str = "Task Checkpoints") -> None:
    """Render task checkpoints as a rich table.

    Args:
        records: List of TaskCheckpointRecord instances.
        title: Table title.
    """
    table = Table(title=title)
    table.add_column("Workflow", style="cyan")
    table.add_column("Task")
    table.add_column("Date")
    table.add_column("Run ID", max_width=30)
    table.add_column("Completed At")

    for r in sorted(records, key=lambda x: (x.workflow_key, x.task_key)):
        parts = r.task_key.split("#", 1)
        task_name = parts[0]
        date = parts[1] if len(parts) > 1 else ""
        table.add_row(r.workflow_key, task_name, date, r.run_id, r.completed_at)

    console.print(table)


def display_process_works_runs(*, records: list[ProcessWorksRunRecord]) -> None:
    """Render process-works runs as a rich table.

    Args:
        records: List of ProcessWorksRunRecord instances.
    """
    table = Table(title="Process-Works Runs")
    table.add_column("Run Date", style="cyan")
    table.add_column("Run ID", max_width=30)
    table.add_column("Status")

    for r in sorted(records, key=lambda x: (x.run_date, x.run_id), reverse=True):
        style = (
            "green"
            if r.status == "COMPLETED"
            else "yellow" if r.status == "STARTED" else "red" if r.status == "FAILED" else ""
        )
        table.add_row(r.run_date, r.run_id, f"[{style}]{r.status}[/{style}]")

    console.print(table)


def display_process_dmps_runs(*, records: list[ProcessDMPsRunRecord]) -> None:
    """Render process-dmps runs as a rich table.

    Args:
        records: List of ProcessDMPsRunRecord instances.
    """
    table = Table(title="Process-DMPs Runs")
    table.add_column("Run Date", style="cyan")
    table.add_column("Run ID", max_width=30)
    table.add_column("Status")

    for r in sorted(records, key=lambda x: (x.run_date, x.run_id), reverse=True):
        style = (
            "green"
            if r.status == "COMPLETED"
            else "yellow" if r.status == "STARTED" else "red" if r.status == "FAILED" else ""
        )
        table.add_row(r.run_date, r.run_id, f"[{style}]{r.status}[/{style}]")

    console.print(table)


def _status_style(*, status: str) -> str:
    """Return the Rich style string for an execution status.

    Args:
        status: SFN execution status.

    Returns:
        Rich markup style name.
    """
    if status == "SUCCEEDED":
        return "green"
    if status == "RUNNING":
        return "yellow"
    return "red"


def _format_duration(*, start: datetime, stop: datetime | None) -> str:
    """Format the duration between two datetimes as a human-readable string.

    Args:
        start: Start datetime.
        stop: Stop datetime, or None if still running.

    Returns:
        Duration string like "2h30m" or "running".
    """
    if stop is None:
        return ""
    delta = stop - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h{minutes}m"
    return f"{minutes}m"


def _format_time_local(*, dt: datetime) -> str:
    """Format a datetime in the system timezone.

    Args:
        dt: A timezone-aware datetime.

    Returns:
        Formatted string like "Mar 20 08:00".
    """
    return dt.astimezone().strftime("%b %d %H:%M")


def display_executions(*, executions: list[dict], start_dt: datetime, end_dt: datetime) -> None:
    """Render Step Functions executions as a Rich tree with nested children.

    Args:
        executions: List of execution dicts with workflow, name, status, start_date,
            stop_date, and children keys.
        start_dt: Start of the date range filter.
        end_dt: End of the date range filter.
    """
    if not executions:
        console.print("[dim]No executions found in the specified date range.[/dim]")
        return

    start_label = start_dt.strftime("%Y-%m-%d")
    end_label = (end_dt).strftime("%Y-%m-%d")
    tree = Tree(f"[bold]State Machine Executions ({start_label} \u2192 {end_label})[/bold]")

    # Group executions by workflow.
    workflows: dict[str, list[dict]] = {}
    for ex in executions:
        workflows.setdefault(ex["workflow"], []).append(ex)

    for workflow, execs in workflows.items():
        workflow_branch = tree.add(f"[bold cyan]{workflow}[/bold cyan]")
        for ex in sorted(execs, key=lambda e: e["start_date"], reverse=True):
            style = _status_style(status=ex["status"])
            start_str = _format_time_local(dt=ex["start_date"])
            stop_str = _format_time_local(dt=ex["stop_date"]) if ex.get("stop_date") else "..."
            duration = _format_duration(start=ex["start_date"], stop=ex.get("stop_date"))
            duration_str = f"  ({duration})" if duration else ""

            exec_label = f"{ex['name']}  [{style}]{ex['status']}[/{style}]  {start_str} \u2192 {stop_str}{duration_str}"
            exec_branch = workflow_branch.add(exec_label)

            for child in ex.get("children", []):
                child_style = _status_style(status=child["status"])
                child_start = _format_time_local(dt=child["start_date"])
                child_stop = _format_time_local(dt=child["stop_date"]) if child.get("stop_date") else "..."
                child_duration = _format_duration(start=child["start_date"], stop=child.get("stop_date"))
                child_duration_str = f"  ({child_duration})" if child_duration else ""
                child_label = child["name"]
                exec_branch.add(
                    f"{child_label}  [{child_style}]{child['status']}[/{child_style}]  {child_start} \u2192 {child_stop}{child_duration_str}"
                )

    console.print(tree)
    console.print(f"[dim]{len(executions)} execution(s) shown.[/dim]")


def _parse_eventbridge_cron(*, expression: str) -> str | None:
    """Extract the 5-field cron expression from an EventBridge schedule expression.

    Strips the ``cron(...)`` wrapper and drops the year field (6th field) since
    ``cron-descriptor`` works with standard 5-field cron expressions.

    Args:
        expression: EventBridge schedule expression (e.g., ``cron(0 15 ? * MON-FRI *)``).

    Returns:
        5-field cron string, or None if the expression is not a cron expression.
    """
    if not expression.startswith("cron("):
        return None
    inner = expression[5:-1].strip()
    fields = inner.split()
    # EventBridge cron has 6 fields (min hour dom month dow year); standard cron has 5.
    eventbridge_cron_field_count = 6
    if len(fields) == eventbridge_cron_field_count:
        fields = fields[:5]
    return " ".join(fields)


def _cron_to_english(*, expression: str, cron_expr: str | None = None) -> str:
    """Convert an EventBridge cron expression to a human-readable UTC description.

    Args:
        expression: EventBridge schedule expression (used as fallback).
        cron_expr: Pre-parsed 5-field cron string, or None to parse from expression.

    Returns:
        Human-readable description, or the raw expression on parse failure.
    """
    if cron_expr is None:
        cron_expr = _parse_eventbridge_cron(expression=expression)
    if cron_expr is None:
        return expression
    try:
        return f"{get_description(cron_expr)} UTC"
    except Exception:
        return expression


def _next_runs_local(*, expression: str, cron_expr: str | None = None, count: int = 3) -> str:
    """Compute the next run times for a cron expression in the system timezone.

    Args:
        expression: EventBridge schedule expression (used for parsing if cron_expr not provided).
        cron_expr: Pre-parsed 5-field cron string, or None to parse from expression.
        count: Number of upcoming run times to show.

    Returns:
        Formatted string of next run times in local timezone, or empty string on failure.
    """
    if cron_expr is None:
        cron_expr = _parse_eventbridge_cron(expression=expression)
    if cron_expr is None:
        return ""
    try:
        now_utc = datetime.now(UTC)
        cron = croniter(cron_expr, now_utc)
        runs = []
        for _ in range(count):
            next_run = cron.get_next(datetime).astimezone()
            runs.append(next_run.strftime("%a %b %d %H:%M %Z"))
        return "\n".join(runs)
    except Exception:
        return ""


def display_schedules(*, rules: list[dict[str, str]]) -> None:
    """Render EventBridge schedule rules as a rich table.

    Args:
        rules: List of dicts with keys name, schedule_expression, state, description.
    """
    table = Table(title="EventBridge Schedules")
    table.add_column("Name", style="cyan")
    table.add_column("Schedule (UTC)")
    table.add_column("State")
    table.add_column("Next Runs (Local)", max_width=60)

    for r in rules:
        style = "green" if r["state"] == "ENABLED" else "red"
        parsed = _parse_eventbridge_cron(expression=r["schedule_expression"])
        utc_desc = _cron_to_english(expression=r["schedule_expression"], cron_expr=parsed)
        local_runs = _next_runs_local(expression=r["schedule_expression"], cron_expr=parsed)
        table.add_row(r["name"], utc_desc, f"[{style}]{r['state']}[/{style}]", local_runs)

    console.print(table)


def display_discovered_versions(*, result: dict) -> None:
    """Render version checker results as a rich table.

    Args:
        result: Response dict from the version checker Lambda.
    """
    discovered = result.get("discovered", [])
    triggered = result.get("triggered", [])

    if not discovered and not triggered:
        console.print("[dim]No new dataset versions discovered.[/dim]")
        return

    table = Table(title="Discovered Versions" if result.get("dry_run") else "Triggered Ingests")
    table.add_column("Dataset", style="cyan")
    table.add_column("Publication Date")
    table.add_column("Download URL", max_width=60)

    items = discovered if discovered else triggered
    for item in items:
        table.add_row(item["dataset"], item["publication_date"], item.get("download_url", ""))

    console.print(table)

    if result.get("dry_run"):
        console.print("[dim]Dry run — no SFN executions were started.[/dim]")


def display_cleanup_plan(*, plan: list[dict]) -> None:
    """Render the S3 cleanup plan as a rich table.

    Args:
        plan: List of dicts with keys prefix_type, run_id, bucket_name.
    """
    table = Table(title=f"S3 Cleanup Plan ({len(plan)} stale prefixes)")
    table.add_column("Prefix Type", style="cyan")
    table.add_column("Run ID")
    table.add_column("S3 Path", max_width=80)

    for item in plan:
        path = f"s3://{item['bucket_name']}/{item['prefix_type']}/{item['run_id']}/"
        table.add_row(item["prefix_type"], item["run_id"], path)

    console.print(table)
