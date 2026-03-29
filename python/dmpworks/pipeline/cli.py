"""Pipeline CLI for inspecting state, managing schedules, and triggering Step Function runs."""

from __future__ import annotations

from typing import Annotated, Literal

from cyclopts import App, Parameter

app = App(name="pipeline", help="Interactive pipeline management commands.")
show_app = App(name="show", help="Read-only inspection commands.")
runs_app = App(name="runs", help="Workflow run history and run actions.")
start_app = App(name="start", help="Start a Step Function execution interactively.")
schedules_app = App(name="schedules", help="Show, pause, or resume EventBridge schedule rules.")
admin_app = App(name="admin", help="Operational and maintenance commands.")

app.command(show_app)
app.command(runs_app)
runs_app.command(start_app)
app.command(schedules_app)
app.command(admin_app)

EnvTypes = Literal["dev", "stg", "prd"]

# Known workflow keys for checkpoint scanning.
DATASET_WORKFLOW_KEYS = (
    "openalex-works",
    "datacite",
    "crossref-metadata",
    "ror",
    "data-citation-corpus",
)

# Known task names per workflow for checkpoint scanning.
DATASET_TASK_NAMES = ("download", "subset", "transform")
PROCESS_WORKS_TASK_NAMES = ("sqlmesh", "sync-works")
PROCESS_DMPS_TASK_NAMES = ("sync-dmps", "enrich-dmps", "dmp-works-search", "merge-related-works")


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


@show_app.command(name="status")
def show_status_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
) -> None:
    """Display the full pipeline dashboard from DynamoDB (releases, checkpoints, process runs)."""
    from dmpworks.pipeline.aws import set_env

    set_env(env=env)

    from dmpworks.pipeline.display import (
        display_dataset_releases,
        display_process_dmps_runs,
        display_process_works_runs,
        display_task_checkpoints,
    )
    from dmpworks.scheduler.dynamodb_store import (
        DatasetReleaseRecord,
        get_task_checkpoint,
        scan_all_process_dmps_runs,
        scan_all_process_works_runs,
    )

    # Dataset releases — last 2 per dataset (cadence varies: monthly to yearly).
    releases = []
    for dataset in DATASET_WORKFLOW_KEYS:
        releases.extend(DatasetReleaseRecord.query(dataset, scan_index_forward=False, limit=2))
    display_dataset_releases(records=releases)

    # Task checkpoints — latest 1 per (workflow, task) combo.
    checkpoints = []
    for wk in DATASET_WORKFLOW_KEYS:
        for tn in DATASET_TASK_NAMES:
            cp = get_task_checkpoint(workflow_key=wk, task_name=tn)
            if cp is not None:
                checkpoints.append(cp)
    for tn in PROCESS_WORKS_TASK_NAMES:
        cp = get_task_checkpoint(workflow_key="process-works", task_name=tn)
        if cp is not None:
            checkpoints.append(cp)
    for tn in PROCESS_DMPS_TASK_NAMES:
        cp = get_task_checkpoint(workflow_key="process-dmps", task_name=tn)
        if cp is not None:
            checkpoints.append(cp)
    display_task_checkpoints(records=checkpoints)

    # Process-works runs — last 3 (monthly cadence).
    works_runs = sorted(scan_all_process_works_runs(), key=lambda r: (r.release_date, r.run_id), reverse=True)[:3]
    display_process_works_runs(records=works_runs)

    # Process-DMPs runs — last 7 (daily cadence).
    dmps_runs = sorted(scan_all_process_dmps_runs(), key=lambda r: (r.release_date, r.run_id), reverse=True)[:7]
    display_process_dmps_runs(records=dmps_runs)


@show_app.command(name="releases")
def show_releases_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    start_date: Annotated[
        str | None,
        Parameter(help="Start date filter (YYYY-MM-DD). Defaults to 3 months ago."),
    ] = None,
) -> None:
    """Display dataset releases from DynamoDB."""
    from datetime import UTC, datetime, timedelta

    from dmpworks.pipeline.aws import set_env

    set_env(env=env)

    start_date = start_date or (datetime.now(UTC) - timedelta(days=90)).strftime("%Y-%m-%d")

    from dmpworks.pipeline.display import display_dataset_releases
    from dmpworks.scheduler.dynamodb_store import DatasetReleaseRecord

    releases = []
    for dataset in DATASET_WORKFLOW_KEYS:
        releases.extend(
            DatasetReleaseRecord.query(
                dataset,
                DatasetReleaseRecord.release_date >= start_date,
                scan_index_forward=False,
            )
        )
    display_dataset_releases(records=releases)


@show_app.command(name="checkpoints")
def show_checkpoints_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    start_date: Annotated[
        str | None,
        Parameter(help="Start date filter (YYYY-MM-DD). Defaults to 3 months ago."),
    ] = None,
    end_date: Annotated[
        str | None,
        Parameter(help="End date filter (YYYY-MM-DD). Defaults to today."),
    ] = None,
) -> None:
    """Display task checkpoints from DynamoDB."""
    from datetime import UTC, datetime, timedelta

    from dmpworks.pipeline.aws import set_env

    set_env(env=env)

    now = datetime.now(UTC)
    end_date = end_date or now.strftime("%Y-%m-%d")
    start_date = start_date or (now - timedelta(days=90)).strftime("%Y-%m-%d")

    from dmpworks.pipeline.display import display_task_checkpoints
    from dmpworks.scheduler.dynamodb_store import scan_task_checkpoints

    checkpoints = scan_all_checkpoints(
        scan_task_checkpoints=scan_task_checkpoints, start_date=start_date, end_date=end_date
    )
    display_task_checkpoints(records=checkpoints)


@show_app.command(name="processes")
def show_processes_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    start_date: Annotated[
        str | None,
        Parameter(help="Start date filter (YYYY-MM-DD). Defaults to 3 months ago."),
    ] = None,
    end_date: Annotated[
        str | None,
        Parameter(help="End date filter (YYYY-MM-DD). Defaults to today."),
    ] = None,
) -> None:
    """Display process-works and process-dmps runs from DynamoDB."""
    from datetime import UTC, datetime, timedelta

    from dmpworks.pipeline.aws import set_env

    set_env(env=env)

    now = datetime.now(UTC)
    end_date = end_date or now.strftime("%Y-%m-%d")
    start_date = start_date or (now - timedelta(days=90)).strftime("%Y-%m-%d")

    from dmpworks.pipeline.display import display_process_dmps_runs, display_process_works_runs
    from dmpworks.scheduler.dynamodb_store import scan_all_process_dmps_runs, scan_all_process_works_runs

    display_process_works_runs(records=scan_all_process_works_runs(start_date=start_date, end_date=end_date))
    display_process_dmps_runs(records=scan_all_process_dmps_runs(start_date=start_date, end_date=end_date))


@show_app.command(name="new-versions")
def show_new_versions_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
) -> None:
    """Run the version checker in dry-run mode to discover new dataset versions."""
    import json

    import boto3

    from dmpworks.pipeline.aws import get_lambda_function_name
    from dmpworks.pipeline.display import display_discovered_versions

    function_name = get_lambda_function_name(env=env, function="version-checker")
    print(f"Invoking {function_name} (dry_run=true)...")

    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps({"dry_run": True}),
    )
    result = json.loads(response["Payload"].read())
    display_discovered_versions(result=result)


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------


@runs_app.command(name="list")
def runs_list_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    start_date: Annotated[
        str | None,
        Parameter(help="Start date filter (YYYY-MM-DD). Defaults to 3 months ago."),
    ] = None,
    end_date: Annotated[
        str | None,
        Parameter(help="End date filter (YYYY-MM-DD). Defaults to today."),
    ] = None,
    status: Annotated[
        str | None,
        Parameter(help="Filter by status (RUNNING, SUCCEEDED, FAILED, TIMED_OUT, ABORTED)."),
    ] = None,
) -> None:
    """Show Step Functions execution history with nested child executions."""
    from dmpworks.pipeline.aws import list_sfn_executions
    from dmpworks.pipeline.display import display_executions

    start_dt, end_dt = parse_date_range(start_date=start_date, end_date=end_date)

    all_executions = list_sfn_executions(env=env, start_dt=start_dt, end_dt=end_dt, status_filter=status)
    display_executions(executions=all_executions, start_dt=start_dt, end_dt=end_dt)


@start_app.command(name="ingest")
def start_ingest_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    bucket_name: Annotated[
        str | None,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name. Defaults to dmpworks-{env}-s3."),
    ] = None,
) -> None:
    """Interactively start a dataset ingest SFN execution."""
    from dmpworks.pipeline.aws import resolve_bucket_name, set_env

    set_env(env=env)
    bucket_name = resolve_bucket_name(env=env, bucket_name=bucket_name)

    from dmpworks.pipeline.interactive import run_ingest_wizard

    run_ingest_wizard(env=env, bucket_name=bucket_name)


@start_app.command(name="process-works")
def start_process_works_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    bucket_name: Annotated[
        str | None,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name. Defaults to dmpworks-{env}-s3."),
    ] = None,
) -> None:
    """Interactively start a process-works SFN execution."""
    from dmpworks.pipeline.aws import resolve_bucket_name, set_env

    set_env(env=env)
    bucket_name = resolve_bucket_name(env=env, bucket_name=bucket_name)

    from dmpworks.pipeline.interactive import run_process_works_wizard

    run_process_works_wizard(env=env, bucket_name=bucket_name)


@start_app.command(name="process-dmps")
def start_process_dmps_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    bucket_name: Annotated[
        str | None,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name. Defaults to dmpworks-{env}-s3."),
    ] = None,
) -> None:
    """Interactively start a process-dmps SFN execution."""
    from dmpworks.pipeline.aws import resolve_bucket_name, set_env

    set_env(env=env)
    bucket_name = resolve_bucket_name(env=env, bucket_name=bucket_name)

    from dmpworks.pipeline.interactive import run_process_dmps_wizard

    run_process_dmps_wizard(env=env, bucket_name=bucket_name)


@runs_app.command(name="approve-retry")
def approve_retry_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
) -> None:
    """Approve a failed child workflow for retry.

    Browse runs awaiting approval, select one, and send approval so the parent
    re-invokes the child with a fresh run_id.
    """
    from dmpworks.pipeline.aws import set_env

    set_env(env=env)

    from dmpworks.pipeline.interactive import run_approve_retry_wizard

    run_approve_retry_wizard()


# ---------------------------------------------------------------------------
# schedules
# ---------------------------------------------------------------------------


@schedules_app.command(name="list")
def schedules_list_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
) -> None:
    """List all EventBridge schedule rules."""
    from dmpworks.pipeline.aws import fetch_schedule_rules
    from dmpworks.pipeline.display import display_schedules

    display_schedules(rules=fetch_schedule_rules(env=env))


@schedules_app.command(name="pause")
def schedules_pause_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    rule: Annotated[
        str | None,
        Parameter(help="Specific rule suffix to pause (e.g., version-checker-schedule)."),
    ] = None,
) -> None:
    """Disable EventBridge schedule rules."""
    from dmpworks.pipeline.aws import fetch_schedule_rules, toggle_schedule_rules
    from dmpworks.pipeline.display import display_schedules

    toggle_schedule_rules(env=env, rule=rule, enable=False)
    display_schedules(rules=fetch_schedule_rules(env=env))


@schedules_app.command(name="resume")
def schedules_resume_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    rule: Annotated[
        str | None,
        Parameter(help="Specific rule suffix to resume (e.g., version-checker-schedule)."),
    ] = None,
) -> None:
    """Enable EventBridge schedule rules."""
    from dmpworks.pipeline.aws import fetch_schedule_rules, toggle_schedule_rules
    from dmpworks.pipeline.display import display_schedules

    toggle_schedule_rules(env=env, rule=rule, enable=True)
    display_schedules(rules=fetch_schedule_rules(env=env))


# ---------------------------------------------------------------------------
# admin
# ---------------------------------------------------------------------------


@admin_app.command(name="cleanup-s3")
def admin_cleanup_s3_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
) -> None:
    """Show the S3 cleanup plan and optionally apply it."""
    import json

    import boto3
    import questionary

    from dmpworks.pipeline.aws import get_lambda_function_name
    from dmpworks.pipeline.display import display_cleanup_plan

    function_name = get_lambda_function_name(env=env, function="s3-cleanup")
    lambda_client = boto3.client("lambda")

    print(f"Invoking {function_name} (dry_run=true)...")
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps({"dry_run": True}),
    )
    result = json.loads(response["Payload"].read())
    plan = result.get("plan", [])

    if not plan:
        print("No stale prefixes found — nothing to clean up.")
        return

    display_cleanup_plan(plan=plan)

    if not questionary.confirm("Apply cleanup? This will schedule these prefixes for S3 expiry.").ask():
        print("Cancelled.")
        return

    print(f"Invoking {function_name} (applying cleanup)...")
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps({"dry_run": False}),
    )
    result = json.loads(response["Payload"].read())
    print(f"Scheduled {result.get('scheduled_count', 0)} prefixes for expiry.")


@admin_app.command(name="delete-checkpoints")
def admin_delete_checkpoints_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
) -> None:
    """Interactively select and delete task checkpoints."""
    from dmpworks.pipeline.aws import set_env

    set_env(env=env)

    import questionary

    from dmpworks.pipeline.display import display_task_checkpoints
    from dmpworks.pipeline.interactive import delete_checkpoint
    from dmpworks.scheduler.dynamodb_store import scan_task_checkpoints

    all_checkpoints = scan_all_checkpoints(scan_task_checkpoints=scan_task_checkpoints)

    if not all_checkpoints:
        print("No checkpoints found.")
        return

    display_task_checkpoints(records=all_checkpoints)

    # Build choices.
    choices = []
    for cp in all_checkpoints:
        parts = cp.task_key.split("#", 1)
        task_name = parts[0]
        date = parts[1] if len(parts) > 1 else ""
        label = f"{cp.workflow_key}/{task_name}#{date}  (run_id={cp.run_id})"
        choices.append(questionary.Choice(label, value=cp))

    selected = questionary.checkbox("Select checkpoints to delete:", choices=choices).ask()
    if not selected:
        print("No checkpoints selected.")
        return

    if not questionary.confirm(f"Delete {len(selected)} checkpoint(s)?").ask():
        print("Cancelled.")
        return

    for cp in selected:
        parts = cp.task_key.split("#", 1)
        task_name = parts[0]
        date = parts[1] if len(parts) > 1 else ""
        delete_checkpoint(workflow_key=cp.workflow_key, task_name=task_name, date=date)

    print(f"Deleted {len(selected)} checkpoint(s).")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def parse_date_range(*, start_date: str | None, end_date: str | None) -> tuple:
    """Parse optional date strings into a (start_dt, end_dt) datetime range.

    Args:
        start_date: Start date as YYYY-MM-DD, or None for 90 days before end_dt.
        end_date: End date as YYYY-MM-DD, or None for today. The returned end_dt
            is set to the start of the following day for inclusive filtering.

    Returns:
        Tuple of (start_dt, end_dt) as timezone-aware datetimes.
    """
    from datetime import UTC, datetime, timedelta

    end_dt = (
        datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC) + timedelta(days=1)
        if end_date
        else datetime.now(UTC) + timedelta(days=1)
    )
    start_dt = (
        datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC) if start_date else end_dt - timedelta(days=90)
    )
    return start_dt, end_dt


def scan_all_checkpoints(*, scan_task_checkpoints, start_date=None, end_date=None):
    """Scan all known workflow/task combos for task checkpoints.

    Args:
        scan_task_checkpoints: The scan function from dynamodb_store.
        start_date: Optional start date filter.
        end_date: Optional end date filter.

    Returns:
        List of TaskCheckpointRecord instances.
    """
    kwargs = {}
    if start_date is not None:
        kwargs["start_date"] = start_date
    if end_date is not None:
        kwargs["end_date"] = end_date

    checkpoints = []
    for wk in DATASET_WORKFLOW_KEYS:
        for tn in DATASET_TASK_NAMES:
            checkpoints.extend(scan_task_checkpoints(workflow_key=wk, task_name=tn, **kwargs))
    for tn in PROCESS_WORKS_TASK_NAMES:
        checkpoints.extend(scan_task_checkpoints(workflow_key="process-works", task_name=tn, **kwargs))
    for tn in PROCESS_DMPS_TASK_NAMES:
        checkpoints.extend(scan_task_checkpoints(workflow_key="process-dmps", task_name=tn, **kwargs))
    return checkpoints
