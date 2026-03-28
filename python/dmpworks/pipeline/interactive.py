"""Interactive wizards for triggering Step Function executions via questionary prompts."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging

import boto3
import questionary
from rich.console import Console
from rich.table import Table

from dmpworks.pipeline.aws import (
    SCHEDULE_RULES,
    get_eventbridge_rule_name,
    get_state_machine_arn,
)
from dmpworks.pipeline.cli import DATASET_WORKFLOW_KEYS
from dmpworks.scheduler.batch_params import generate_run_id
from dmpworks.scheduler.config import load_lambda_config
from dmpworks.scheduler.dynamodb_store import (
    DatasetReleaseRecord,
    clear_approval_token,
    delete_task_checkpoint,
    get_runs_awaiting_approval,
    get_task_checkpoint,
    scan_all_process_works_runs,
)

log = logging.getLogger(__name__)
console = Console()


def _check_running_executions(*, sm_arn: str) -> bool:
    """Check for running executions and warn the user. Returns True if user wants to continue."""
    sfn = boto3.client("stepfunctions")
    response = sfn.list_executions(stateMachineArn=sm_arn, statusFilter="RUNNING", maxResults=5)
    running = response.get("executions", [])
    if running:
        console.print(f"\n[yellow]Warning: {len(running)} execution(s) currently RUNNING:[/yellow]")
        for ex in running:
            console.print(f"  {ex['name']}  (started {ex['startDate']})")
        return questionary.confirm("Continue anyway?", default=False).ask()
    return True


def _check_schedules_enabled(*, env: str) -> bool:
    """Check if any EventBridge schedules are enabled and warn. Returns True if user wants to continue."""
    events = boto3.client("events")
    enabled_rules = []
    for rule_suffix in SCHEDULE_RULES:
        rule_name = get_eventbridge_rule_name(env=env, rule=rule_suffix)
        try:
            resp = events.describe_rule(Name=rule_name)
            if resp.get("State") == "ENABLED":
                enabled_rules.append(rule_name)
        except events.exceptions.ResourceNotFoundException:
            pass
    if enabled_rules:
        console.print(f"\n[yellow]Warning: {len(enabled_rules)} EventBridge schedule(s) are ENABLED:[/yellow]")
        for r in enabled_rules:
            console.print(f"  {r}")
        console.print("[dim]Consider running 'dmpworks pipeline schedules --pause' before manual runs.[/dim]")
        return questionary.confirm("Continue with schedules enabled?", default=True).ask()
    return True


def _delete_checkpoint(*, workflow_key: str, task_name: str, date: str) -> None:
    """Delete a task checkpoint record from DynamoDB."""
    old_record = delete_task_checkpoint(workflow_key=workflow_key, task_name=task_name, date=date)
    if old_record is None:
        return
    console.print(
        f"  Deleted checkpoint: {workflow_key}/{task_name}#{date} (run_id={old_record.run_id}, completed_at={old_record.completed_at})"
    )


def _start_execution(*, sm_arn: str, execution_name: str, sfn_input: dict) -> str:
    """Start a Step Functions execution and return the execution ARN."""
    sfn = boto3.client("stepfunctions")
    response = sfn.start_execution(
        stateMachineArn=sm_arn,
        name=execution_name,
        input=json.dumps(sfn_input),
    )
    return response["executionArn"]


def _confirm_and_start(*, sm_arn: str, execution_name: str, sfn_input: dict, hide_keys: set[str] | None = None) -> None:
    """Display execution summary, confirm with user, and start the SFN execution.

    Args:
        sm_arn: State machine ARN.
        execution_name: Name for the SFN execution.
        sfn_input: Input dict for the SFN execution.
        hide_keys: Optional set of keys to hide from the summary display.
    """
    console.print(f"\n[bold]Start New Execution:[/bold] {execution_name}")
    console.print("\n[bold]Execution Summary:[/bold]")
    table = Table()
    table.add_column("Field")
    table.add_column("Value")
    for k, v in sorted(sfn_input.items(), key=lambda x: x[0].lower()):
        if hide_keys and k in hide_keys:
            continue
        table.add_row(k, str(v))
    console.print(table)

    if not questionary.confirm("Start execution?").ask():
        console.print("Cancelled.")
        return

    arn = _start_execution(sm_arn=sm_arn, execution_name=execution_name, sfn_input=sfn_input)
    console.print(f"\n[green]Started execution:[/green] {arn}")


def run_ingest_wizard(*, env: str, bucket_name: str) -> None:
    """Interactive wizard for starting a dataset ingest SFN execution.

    Args:
        env: AWS environment (dev, stg, prd).
        bucket_name: S3 bucket name for the execution input.
    """
    # 1. List datasets with releases.
    dataset = questionary.select("Select dataset:", choices=list(DATASET_WORKFLOW_KEYS)).ask()
    if dataset is None:
        return

    # 2. List publication dates for this dataset.
    releases = list(DatasetReleaseRecord.query(dataset, scan_index_forward=False, limit=10))
    if not releases:
        console.print(f"[red]No releases found for {dataset}.[/red]")
        return

    date_choices = [f"{r.publication_date}  ({r.status})" for r in releases]
    selected = questionary.select("Select publication date:", choices=date_choices).ask()
    if selected is None:
        return
    publication_date = selected.split()[0]
    release = next(r for r in releases if r.publication_date == publication_date)

    # 3. Show existing checkpoints.
    task_names = ["download", "subset", "transform"]
    checkpoints = {}
    for tn in task_names:
        cp = get_task_checkpoint(workflow_key=dataset, task_name=tn, date=publication_date)
        if cp:
            checkpoints[tn] = cp

    console.print("\nCheckpoint status:")
    for tn in task_names:
        if tn in checkpoints:
            cp = checkpoints[tn]
            console.print(f"  [green]{tn}[/green]: run_id={cp.run_id} completed_at={cp.completed_at}")
        else:
            console.print(f"  [yellow]{tn}[/yellow]: no checkpoint")

    # 4. Select steps to re-run (pre-select those without checkpoints).
    default_rerun = [tn for tn in task_names if tn not in checkpoints]
    rerun_choices = questionary.checkbox(
        "Steps to re-run (selected steps will run, others will be skipped):",
        choices=[questionary.Choice(tn, checked=(tn in default_rerun)) for tn in task_names],
    ).ask()
    if rerun_choices is None:
        return

    skip_download = "download" not in rerun_choices
    skip_subset = "subset" not in rerun_choices
    skip_transform = "transform" not in rerun_choices

    # 5. Validations.
    if not skip_transform and skip_download:
        cp = get_task_checkpoint(workflow_key=dataset, task_name="download", date=publication_date)
        if not cp:
            console.print(
                "[red]Error: Cannot run transform without a download checkpoint (transform needs predecessor run_id).[/red]"
            )
            return

    if not skip_download:
        latest = list(DatasetReleaseRecord.query(dataset, scan_index_forward=False, limit=1))
        if latest and latest[0].publication_date != publication_date and dataset in ("openalex-works", "datacite"):
            console.print(
                f"[yellow]Warning: Re-downloading {dataset} for {publication_date} — source URLs may be stale (latest is {latest[0].publication_date}).[/yellow]"
            )

    # 6. Delete checkpoints for steps being re-run.
    for tn in rerun_choices:
        if tn in checkpoints:
            _delete_checkpoint(workflow_key=dataset, task_name=tn, date=publication_date)

    # 7. Pre-flight checks.
    sm_arn = get_state_machine_arn(env=env, workflow="dataset-ingest")
    if not _check_running_executions(sm_arn=sm_arn):
        return
    if not _check_schedules_enabled(env=env):
        return

    # 8. Build SFN input.
    run_id = generate_run_id()
    # Determine use_subset from SSM config if possible.
    try:
        config = load_lambda_config(env)
        use_subset = config.dataset_subset.enable
    except Exception:
        use_subset = False

    sfn_input = {
        "workflow_key": dataset,
        "publication_date": publication_date,
        "run_id": run_id,
        "aws_env": env,
        "bucket_name": bucket_name,
        "download_url": release.download_url,
        "file_hash": release.file_hash,
        "file_name": release.file_name,
        "use_subset": use_subset,
        "skip_download": skip_download,
        "skip_subset": skip_subset,
        "skip_transform": skip_transform,
    }

    # 9. Confirm and start.
    execution_name = f"{dataset}-{publication_date}-{run_id}"
    _confirm_and_start(sm_arn=sm_arn, execution_name=execution_name, sfn_input=sfn_input)


def run_process_works_wizard(*, env: str, bucket_name: str) -> None:
    """Interactive wizard for starting a process-works SFN execution.

    Args:
        env: AWS environment (dev, stg, prd).
        bucket_name: S3 bucket name for the execution input.
    """
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    run_date = questionary.text("Run date (YYYY-MM-DD):", default=today).ask()
    if run_date is None:
        return

    # Show checkpoint status.
    task_names = ["sqlmesh", "sync-works"]
    checkpoints = {}
    for tn in task_names:
        cp = get_task_checkpoint(workflow_key="process-works", task_name=tn, date=run_date)
        if cp:
            checkpoints[tn] = cp

    console.print("\nCheckpoint status:")
    for tn in task_names:
        if tn in checkpoints:
            cp = checkpoints[tn]
            console.print(f"  [green]{tn}[/green]: run_id={cp.run_id} completed_at={cp.completed_at}")
        else:
            console.print(f"  [yellow]{tn}[/yellow]: no checkpoint")

    # Select steps to re-run — skip prompt if no checkpoints exist (all must run).
    if checkpoints:
        default_rerun = [tn for tn in task_names if tn not in checkpoints]
        rerun_choices = questionary.checkbox(
            "Steps to re-run:",
            choices=[questionary.Choice(tn, checked=(tn in default_rerun)) for tn in task_names],
        ).ask()
        if rerun_choices is None:
            return

        # Validate: skipped steps must have checkpoints (ExtractRunId needs them).
        if "sqlmesh" not in rerun_choices and "sqlmesh" not in checkpoints:
            console.print(
                "[red]Error: Cannot skip sqlmesh — no checkpoint exists. The workflow needs a sqlmesh run_id.[/red]"
            )
            return
    else:
        console.print("[dim]No checkpoints found — all steps will run.[/dim]")
        rerun_choices = list(task_names)

    skip_sqlmesh = "sqlmesh" not in rerun_choices
    skip_sync_works = "sync-works" not in rerun_choices

    # Show which datasets will be used.
    dataset_table = Table(title="Datasets")
    dataset_table.add_column("Dataset", style="cyan")
    dataset_table.add_column("Date")
    dataset_table.add_column("Run ID", max_width=30)
    missing_datasets = []
    for wk in DATASET_WORKFLOW_KEYS:
        task = "download" if wk in ("ror", "data-citation-corpus") else "transform"
        cp = get_task_checkpoint(workflow_key=wk, task_name=task)
        if cp:
            parts = cp.task_key.split("#", 1)
            date = parts[1] if len(parts) > 1 else ""
            dataset_table.add_row(wk, date, cp.run_id)
        else:
            dataset_table.add_row(wk, "[yellow]missing[/yellow]", "")
            missing_datasets.append(wk)
    console.print(dataset_table)

    if missing_datasets:
        console.print(f"[yellow]Warning: Missing dataset checkpoints: {', '.join(missing_datasets)}[/yellow]")
        console.print("[dim]The workflow will poll until all datasets are ready (up to 1 week).[/dim]")

    # Delete checkpoints for re-run steps.
    for tn in rerun_choices:
        if tn in checkpoints:
            _delete_checkpoint(workflow_key="process-works", task_name=tn, date=run_date)

    # Pre-flight checks.
    sm_arn = get_state_machine_arn(env=env, workflow="process-works")
    if not _check_running_executions(sm_arn=sm_arn):
        return
    if not _check_schedules_enabled(env=env):
        return

    # Build SFN input.
    sfn_input = {
        "workflow_key": "process-works",
        "publication_date": run_date,
        "run_date": run_date,
        "aws_env": env,
        "bucket_name": bucket_name,
        "skip_sqlmesh": skip_sqlmesh,
        "skip_sync_works": skip_sync_works,
    }

    run_id = generate_run_id()
    execution_name = f"process-works-{run_date}-{run_id}"

    _confirm_and_start(
        sm_arn=sm_arn, execution_name=execution_name, sfn_input=sfn_input, hide_keys={"publication_date"}
    )


def run_process_dmps_wizard(*, env: str, bucket_name: str) -> None:
    """Interactive wizard for starting a process-dmps SFN execution.

    Args:
        env: AWS environment (dev, stg, prd).
        bucket_name: S3 bucket name for the execution input.
    """
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    run_date = questionary.text("Run date (YYYY-MM-DD):", default=today).ask()
    if run_date is None:
        return

    # Show checkpoint status.
    task_names = ["sync-dmps", "enrich-dmps", "dmp-works-search", "merge-related-works"]
    checkpoints = {}
    for tn in task_names:
        cp = get_task_checkpoint(workflow_key="process-dmps", task_name=tn, date=run_date)
        if cp:
            checkpoints[tn] = cp

    console.print("\nCheckpoint status:")
    for tn in task_names:
        if tn in checkpoints:
            cp = checkpoints[tn]
            console.print(f"  [green]{tn}[/green]: run_id={cp.run_id} completed_at={cp.completed_at}")
        else:
            console.print(f"  [yellow]{tn}[/yellow]: no checkpoint")

    # Select steps to re-run — skip prompt if no checkpoints exist (all must run).
    if checkpoints:
        default_rerun = [tn for tn in task_names if tn not in checkpoints]
        rerun_choices = questionary.checkbox(
            "Steps to re-run:",
            choices=[questionary.Choice(tn, checked=(tn in default_rerun)) for tn in task_names],
        ).ask()
        if rerun_choices is None:
            return

        # Validate: skipped steps must have checkpoints (ExtractRunId needs them).
        for tn in task_names:
            if tn not in rerun_choices and tn not in checkpoints:
                console.print(
                    f"[red]Error: Cannot skip {tn} — no checkpoint exists. The workflow needs its run_id.[/red]"
                )
                return
    else:
        console.print("[dim]No checkpoints found — all steps will run.[/dim]")
        rerun_choices = list(task_names)

    skip_sync_dmps = "sync-dmps" not in rerun_choices
    skip_enrich_dmps = "enrich-dmps" not in rerun_choices
    skip_dmp_works_search = "dmp-works-search" not in rerun_choices
    skip_merge_related_works = "merge-related-works" not in rerun_choices

    # Validate: check that a completed process-works run exists.
    works_runs = scan_all_process_works_runs()
    completed_works = [r for r in works_runs if r.status == "COMPLETED"]
    if not completed_works:
        console.print(
            "[yellow]Warning: No completed process-works runs found. The works index may not be up to date.[/yellow]"
        )
        if not questionary.confirm("Continue anyway?", default=False).ask():
            return

    # run_all_dmps.
    run_all_dmps = questionary.confirm("Run all DMPs? (default: yes for manual runs)", default=True).ask()
    if run_all_dmps is None:
        return

    # Delete checkpoints for re-run steps.
    for tn in rerun_choices:
        if tn in checkpoints:
            _delete_checkpoint(workflow_key="process-dmps", task_name=tn, date=run_date)

    # Pre-flight checks.
    sm_arn = get_state_machine_arn(env=env, workflow="process-dmps")
    if not _check_running_executions(sm_arn=sm_arn):
        return
    if not _check_schedules_enabled(env=env):
        return

    # Build SFN input.
    sfn_input = {
        "workflow_key": "process-dmps",
        "publication_date": run_date,
        "run_date": run_date,
        "aws_env": env,
        "bucket_name": bucket_name,
        "run_all_dmps": run_all_dmps,
        "skip_sync_dmps": skip_sync_dmps,
        "skip_enrich_dmps": skip_enrich_dmps,
        "skip_dmp_works_search": skip_dmp_works_search,
        "skip_merge_related_works": skip_merge_related_works,
    }

    run_id = generate_run_id()
    execution_name = f"process-dmps-{run_date}-{run_id}"

    _confirm_and_start(
        sm_arn=sm_arn, execution_name=execution_name, sfn_input=sfn_input, hide_keys={"publication_date"}
    )


def run_approve_retry_wizard() -> None:
    """Interactive wizard for approving a failed child workflow for retry.

    Queries DynamoDB for run records with non-null approval_token (parents waiting
    for manual approval), lets the user pick one, then sends SendTaskSuccess to the
    parent so it re-invokes the child with a fresh run_id.
    """
    console.print("[dim]Searching for runs awaiting approval...[/dim]")

    awaiting = get_runs_awaiting_approval()

    if not awaiting:
        console.print("[yellow]No runs are currently awaiting retry approval.[/yellow]")
        return

    choices = []
    for run in awaiting:
        if "publication_date" in run:
            label = (
                f"{run['workflow_key']}  task={run['approval_task_name']}  publication_date={run['publication_date']}"
            )
        else:
            label = f"{run['workflow_key']}  task={run['approval_task_name']}  run_date={run['run_date']}  run_id={run['run_id']}"
        choices.append(questionary.Choice(label, value=run))

    selected = questionary.select("Select run to approve for retry:", choices=choices).ask()
    if selected is None:
        return

    if not questionary.confirm(f"Send approval to retry {selected['approval_task_name']}?", default=True).ask():
        return

    sfn = boto3.client("stepfunctions")
    sfn.send_task_success(taskToken=selected["approval_token"], output="{}")
    console.print(
        f"[green]Approved retry for {selected['approval_task_name']}. Parent will re-invoke the child.[/green]"
    )

    # Clear the approval token from the record.
    clear_kwargs = {"workflow_key": selected["workflow_key"]}
    if "publication_date" in selected:
        clear_kwargs["dataset"] = selected["dataset"]
        clear_kwargs["publication_date"] = selected["publication_date"]
    else:
        clear_kwargs["run_date"] = selected["run_date"]
        clear_kwargs["run_id"] = selected["run_id"]
    clear_approval_token(**clear_kwargs)
