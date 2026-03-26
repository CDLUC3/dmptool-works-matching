"""Rich table rendering for pipeline status display."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

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


def display_schedules(*, rules: list[dict[str, str]]) -> None:
    """Render EventBridge schedule rules as a rich table.

    Args:
        rules: List of dicts with keys name, schedule_expression, state, description.
    """
    table = Table(title="EventBridge Schedules")
    table.add_column("Name", style="cyan")
    table.add_column("Schedule")
    table.add_column("State")
    table.add_column("Description", max_width=60)

    for r in rules:
        style = "green" if r["state"] == "ENABLED" else "red"
        table.add_row(r["name"], r["schedule_expression"], f"[{style}]{r['state']}[/{style}]", r.get("description", ""))

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
