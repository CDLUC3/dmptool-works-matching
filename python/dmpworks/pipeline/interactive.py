"""Interactive wizards for triggering Step Function executions via questionary prompts."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Callable

    from dmpworks.scheduler.dynamodb_store import TaskCheckpointRecord

import boto3
import pendulum
import questionary
from rich.console import Console
from rich.table import Table

from dmpworks.pipeline.aws import (
    SCHEDULE_RULES,
    build_execution_dict,
    fetch_child_executions,
    get_eventbridge_rule_name,
    get_state_machine_arn,
)
from dmpworks.pipeline.cli import DATASET_WORKFLOW_KEYS, PROCESS_DMPS_TASK_NAMES
from dmpworks.pipeline.display import build_execution_tree
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
from dmpworks.utils import thread_map

log = logging.getLogger(__name__)
console = Console()


def check_running_executions(*, sm_arn: str, execution_prefix: str | None = None) -> bool:
    """Check for running executions and warn the user. Returns True if user wants to continue.

    Args:
        sm_arn: State machine ARN to check.
        execution_prefix: If provided, only warn about executions whose name starts with this prefix.
    """
    sfn = boto3.client("stepfunctions")
    response = sfn.list_executions(stateMachineArn=sm_arn, statusFilter="RUNNING", maxResults=5)
    running = response.get("executions", [])
    if execution_prefix:
        running = [ex for ex in running if ex["name"].startswith(f"{execution_prefix}-")]
    if running:
        console.print(f"\n[yellow]Warning: {len(running)} execution(s) currently RUNNING:[/yellow]")
        for ex in running:
            console.print(f"  {ex['name']}  (started {ex['startDate']})")
        return questionary.confirm("Continue anyway?", default=False, auto_enter=False).ask()
    return True


def validate_date(value: str) -> bool | str:
    """Validate that a string is a valid YYYY-MM-DD date."""
    try:
        pendulum.from_format(value, "YYYY-MM-DD")
    except ValueError:
        return "Invalid date — expected YYYY-MM-DD format."
    return True


def check_schedules_enabled(*, env: str) -> bool:
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
        return questionary.confirm("Continue with schedules enabled?", default=True, auto_enter=False).ask()
    return True


def delete_checkpoint(*, workflow_key: str, task_name: str, date: str) -> None:
    """Delete a task checkpoint record from DynamoDB."""
    old_record = delete_task_checkpoint(workflow_key=workflow_key, task_name=task_name, date=date)
    if old_record is None:
        return
    console.print(
        f"  Deleted checkpoint: {workflow_key}/{task_name}#{date} (run_id={old_record.run_id}, completed_at={old_record.completed_at})"
    )


def start_execution(*, sm_arn: str, execution_name: str, sfn_input: dict) -> str:
    """Start a Step Functions execution and return the execution ARN."""
    sfn = boto3.client("stepfunctions")
    response = sfn.start_execution(
        stateMachineArn=sm_arn,
        name=execution_name,
        input=json.dumps(sfn_input),
    )
    return response["executionArn"]


def confirm_and_start(
    *,
    sm_arn: str,
    execution_name: str,
    sfn_input: dict,
    on_confirmed: Callable[[], None] | None = None,
) -> None:
    """Display execution summary, confirm with user, and start the SFN execution.

    Args:
        sm_arn: State machine ARN.
        execution_name: Name for the SFN execution.
        sfn_input: Input dict for the SFN execution.
        on_confirmed: Optional callback to run after the user confirms (e.g. delete checkpoints).
    """
    console.print(f"\n[bold]Start New Execution:[/bold] {execution_name}")
    console.print("\n[bold]Execution Summary:[/bold]")
    table = Table()
    table.add_column("Field")
    table.add_column("Value")
    for k, v in sorted(sfn_input.items(), key=lambda x: x[0].lower()):
        table.add_row(k, str(v))
    console.print(table)

    if not questionary.confirm("Start execution?", auto_enter=False).ask():
        console.print("Cancelled.")
        return

    if on_confirmed:
        on_confirmed()

    arn = start_execution(sm_arn=sm_arn, execution_name=execution_name, sfn_input=sfn_input)
    console.print(f"\n[green]Started execution:[/green] {arn}")


WORKFLOW_CHOICES = ["ingest", "process-works", "process-dmps"]


def prompt_dmp_scope(*, env: str) -> bool | None:
    """Prompt the user to select which DMPs to process.

    Args:
        env: AWS environment, used to load the modification window config from SSM.

    Returns:
        True for all DMPs, False for recently modified only, None if cancelled.
    """
    modification_window_days = None
    try:
        config = load_lambda_config(env)
        modification_window_days = config.dmp_works_search_config.dmp_modification_window_days
    except Exception:
        log.debug("Could not load modification window config", exc_info=True)
    if modification_window_days is not None:
        daily_label = f"Only DMPs modified in the last {modification_window_days} days (daily update)"
    else:
        daily_label = "Only recently modified DMPs (daily update)"
    dmp_scope = questionary.select(
        "Which DMPs to process:",
        choices=["All DMPs", daily_label],
    ).ask()
    if dmp_scope is None:
        return None
    return dmp_scope == "All DMPs"


class DmpsStepSelection(NamedTuple):
    """Result of a process-dmps step selection prompt."""

    rerun_choices: list[str]
    checkpoints: dict[str, TaskCheckpointRecord]
    skip_sync_dmps: bool
    skip_enrich_dmps: bool
    skip_dmp_works_search: bool
    skip_merge_related_works: bool


def prompt_dmps_step_selection(*, release_date: str) -> DmpsStepSelection | None:
    """Show process-dmps checkpoint status and prompt for steps to re-run.

    Args:
        release_date: Release date string (YYYY-MM-DD).

    Returns:
        DmpsStepSelection with rerun choices, checkpoints, and skip flags, or None if cancelled.
    """
    task_names = list(PROCESS_DMPS_TASK_NAMES)
    checkpoints = {}
    for tn in task_names:
        cp = get_task_checkpoint(workflow_key="process-dmps", task_name=tn, date=release_date)
        if cp:
            checkpoints[tn] = cp

    console.print("\nProcess-dmps checkpoint status:")
    for tn in task_names:
        if tn in checkpoints:
            cp = checkpoints[tn]
            console.print(f"  [green]{tn}[/green]: run_id={cp.run_id} completed_at={cp.completed_at}")
        else:
            console.print(f"  [yellow]{tn}[/yellow]: no checkpoint")

    if checkpoints:
        default_rerun = [tn for tn in task_names if tn not in checkpoints]
        rerun_choices = questionary.checkbox(
            "Process-dmps steps to re-run:",
            choices=[questionary.Choice(tn, checked=(tn in default_rerun)) for tn in task_names],
        ).ask()
        if rerun_choices is None:
            return None

        for tn in task_names:
            if tn not in rerun_choices and tn not in checkpoints:
                console.print(
                    f"[red]Error: Cannot skip {tn} — no checkpoint exists. The workflow needs its run_id.[/red]"
                )
                return None
    else:
        console.print("[dim]No process-dmps checkpoints — all steps will run.[/dim]")
        rerun_choices = list(task_names)

    return DmpsStepSelection(
        rerun_choices=rerun_choices,
        checkpoints=checkpoints,
        skip_sync_dmps="sync-dmps" not in rerun_choices,
        skip_enrich_dmps="enrich-dmps" not in rerun_choices,
        skip_dmp_works_search="dmp-works-search" not in rerun_choices,
        skip_merge_related_works="merge-related-works" not in rerun_choices,
    )


def run_start_wizard(*, env: str, bucket_name: str) -> None:
    """Interactive wizard for selecting and starting a Step Function execution.

    Args:
        env: AWS environment (dev, stg, prd).
        bucket_name: S3 bucket name for the execution input.
    """
    workflow = questionary.select("Select workflow:", choices=WORKFLOW_CHOICES).ask()
    if workflow is None:
        return
    if workflow == "ingest":
        run_ingest_wizard(env=env, bucket_name=bucket_name)
    elif workflow == "process-works":
        run_process_works_wizard(env=env, bucket_name=bucket_name)
    else:
        run_process_dmps_wizard(env=env, bucket_name=bucket_name)


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

    # 2. List release dates for this dataset.
    releases = list(DatasetReleaseRecord.query(dataset, scan_index_forward=False, limit=10))
    if not releases:
        console.print(f"[red]No releases found for {dataset}.[/red]")
        return

    date_choices = [f"{r.release_date}  ({r.status})" for r in releases]
    selected = questionary.select("Select release date:", choices=date_choices).ask()
    if selected is None:
        return
    release_date = selected.split()[0]
    release = next(r for r in releases if r.release_date == release_date)

    # 3. Build task list based on dataset capabilities.
    if dataset in ("ror", "data-citation-corpus"):
        task_names = ["download"]
        use_subset = False
    else:
        try:
            config = load_lambda_config(env)
            use_subset = config.dataset_subset.enable
        except Exception:
            log.debug("Could not load subset config", exc_info=True)
            use_subset = False
        task_names = ["download", "subset", "transform"] if use_subset else ["download", "transform"]
    checkpoints = {}
    for tn in task_names:
        cp = get_task_checkpoint(workflow_key=dataset, task_name=tn, date=release_date)
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
        cp = get_task_checkpoint(workflow_key=dataset, task_name="download", date=release_date)
        if not cp:
            console.print(
                "[red]Error: Cannot run transform without a download checkpoint (transform needs predecessor run_id).[/red]"
            )
            return

    if not skip_download:
        latest = list(DatasetReleaseRecord.query(dataset, scan_index_forward=False, limit=1))
        if latest and latest[0].release_date != release_date and dataset in ("openalex-works", "datacite"):
            console.print(
                f"[yellow]Warning: Re-downloading {dataset} for {release_date} — source URLs may be stale (latest is {latest[0].release_date}).[/yellow]"
            )

    # 6. Pre-flight checks.
    sm_arn = get_state_machine_arn(env=env, workflow="dataset-ingest")
    if not check_running_executions(sm_arn=sm_arn, execution_prefix=dataset):
        return
    if not check_schedules_enabled(env=env):
        return

    # 7. Build SFN input.
    run_id = generate_run_id()
    sfn_input = {
        "workflow_key": dataset,
        "release_date": release_date,
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

    # 8. Confirm and start — checkpoints are deleted only after user confirms.
    def delete_checkpoints() -> None:
        for tn in rerun_choices:
            if tn in checkpoints:
                delete_checkpoint(workflow_key=dataset, task_name=tn, date=release_date)

    execution_name = f"{dataset}-{release_date}-{run_id}"
    confirm_and_start(
        sm_arn=sm_arn, execution_name=execution_name, sfn_input=sfn_input, on_confirmed=delete_checkpoints
    )


def run_process_works_wizard(*, env: str, bucket_name: str) -> None:
    """Interactive wizard for starting a process-works SFN execution.

    Args:
        env: AWS environment (dev, stg, prd).
        bucket_name: S3 bucket name for the execution input.
    """
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    release_date = questionary.text("Release date (YYYY-MM-DD):", default=today, validate=validate_date).ask()
    if release_date is None:
        return

    # Show checkpoint status.
    task_names = ["sqlmesh", "sync-works"]
    checkpoints = {}
    for tn in task_names:
        cp = get_task_checkpoint(workflow_key="process-works", task_name=tn, date=release_date)
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

    # Pre-flight checks.
    sm_arn = get_state_machine_arn(env=env, workflow="process-works")
    if not check_running_executions(sm_arn=sm_arn):
        return
    if not check_schedules_enabled(env=env):
        return

    # Ask whether to chain into process-dmps after completion.
    start_process_dmps = questionary.confirm(
        "Start process-dmps after completion?", default=True, auto_enter=False
    ).ask()
    if start_process_dmps is None:
        return
    if start_process_dmps:
        run_all_dmps = prompt_dmp_scope(env=env)
        if run_all_dmps is None:
            return
        dmps_selection = prompt_dmps_step_selection(release_date=release_date)
        if dmps_selection is None:
            return
    else:
        run_all_dmps = True
        dmps_selection = None

    # Build SFN input.
    run_id = generate_run_id()
    sfn_input = {
        "workflow_key": "process-works",
        "release_date": release_date,
        "run_id": run_id,
        "aws_env": env,
        "bucket_name": bucket_name,
        "skip_sqlmesh": skip_sqlmesh,
        "skip_sync_works": skip_sync_works,
        "start_process_dmps": start_process_dmps,
        "run_all_dmps": run_all_dmps,
        "skip_sync_dmps": dmps_selection.skip_sync_dmps if dmps_selection else True,
        "skip_enrich_dmps": dmps_selection.skip_enrich_dmps if dmps_selection else True,
        "skip_dmp_works_search": dmps_selection.skip_dmp_works_search if dmps_selection else True,
        "skip_merge_related_works": dmps_selection.skip_merge_related_works if dmps_selection else True,
    }

    def delete_checkpoints() -> None:
        for tn in rerun_choices:
            if tn in checkpoints:
                delete_checkpoint(workflow_key="process-works", task_name=tn, date=release_date)
        if dmps_selection:
            for tn in dmps_selection.rerun_choices:
                if tn in dmps_selection.checkpoints:
                    delete_checkpoint(workflow_key="process-dmps", task_name=tn, date=release_date)

    execution_name = f"process-works-{release_date}-{run_id}"
    confirm_and_start(
        sm_arn=sm_arn, execution_name=execution_name, sfn_input=sfn_input, on_confirmed=delete_checkpoints
    )


def run_process_dmps_wizard(*, env: str, bucket_name: str) -> None:
    """Interactive wizard for starting a process-dmps SFN execution.

    Args:
        env: AWS environment (dev, stg, prd).
        bucket_name: S3 bucket name for the execution input.
    """
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    release_date = questionary.text("Release date (YYYY-MM-DD):", default=today, validate=validate_date).ask()
    if release_date is None:
        return

    selection = prompt_dmps_step_selection(release_date=release_date)
    if selection is None:
        return

    # Show latest completed process-works run so the user knows what data the works index contains.
    works_runs = scan_all_process_works_runs()
    completed_works = sorted(
        [r for r in works_runs if r.status == "COMPLETED"],
        key=lambda r: (r.release_date, r.run_id),
        reverse=True,
    )
    if completed_works:
        latest = completed_works[0]
        pw_table = Table(title="Latest process-works run", show_header=True)
        pw_table.add_column("Field", style="cyan")
        pw_table.add_column("Value")
        pw_table.add_row("release_date", latest.release_date)
        pw_table.add_row("run_id", latest.run_id)
        pw_table.add_row("updated_at", latest.updated_at)
        dataset_release_dates = [
            ("openalex-works", latest.release_date_openalex_works),
            ("datacite", latest.release_date_datacite),
            ("crossref-metadata", latest.release_date_crossref_metadata),
            ("ror", latest.release_date_ror),
            ("data-citation-corpus", latest.release_date_data_citation_corpus),
        ]
        for dataset, release_date_val in dataset_release_dates:
            pw_table.add_row(dataset, release_date_val or "[dim]—[/dim]")
        console.print(pw_table)
    else:
        console.print(
            "[yellow]Warning: No completed process-works runs found. The works index may not be up to date.[/yellow]"
        )
        if not questionary.confirm("Continue anyway?", default=False, auto_enter=False).ask():
            return

    # Which DMPs to process.
    run_all_dmps = prompt_dmp_scope(env=env)
    if run_all_dmps is None:
        return

    # Pre-flight checks.
    sm_arn = get_state_machine_arn(env=env, workflow="process-dmps")
    if not check_running_executions(sm_arn=sm_arn):
        return
    if not check_schedules_enabled(env=env):
        return

    # Build SFN input.
    run_id = generate_run_id()
    sfn_input = {
        "workflow_key": "process-dmps",
        "release_date": release_date,
        "run_id": run_id,
        "aws_env": env,
        "bucket_name": bucket_name,
        "run_all_dmps": run_all_dmps,
        "skip_sync_dmps": selection.skip_sync_dmps,
        "skip_enrich_dmps": selection.skip_enrich_dmps,
        "skip_dmp_works_search": selection.skip_dmp_works_search,
        "skip_merge_related_works": selection.skip_merge_related_works,
    }

    def delete_checkpoints() -> None:
        for tn in selection.rerun_choices:
            if tn in selection.checkpoints:
                delete_checkpoint(workflow_key="process-dmps", task_name=tn, date=release_date)

    execution_name = f"process-dmps-{release_date}-{run_id}"
    confirm_and_start(
        sm_arn=sm_arn, execution_name=execution_name, sfn_input=sfn_input, on_confirmed=delete_checkpoints
    )


def extract_run_id_from_execution_name(*, execution_name: str, workflow_key: str, release_date: str) -> str | None:
    """Extract the run_id from a Step Functions execution name.

    Execution names follow the pattern ``{workflow_key}-{release_date}-{run_id}``.

    Args:
        execution_name: The SFN execution name.
        workflow_key: The workflow key prefix.
        release_date: The release date component.

    Returns:
        The run_id suffix, or None if the name doesn't match the expected pattern.
    """
    prefix = f"{workflow_key}-{release_date}-"
    result = execution_name.removeprefix(prefix)
    return result if result != execution_name else None


def clear_approval(run: dict) -> None:
    """Clear approval_token from a run record dict.

    Args:
        run: A run dict from get_runs_awaiting_approval().
    """
    clear_kwargs: dict = {"workflow_key": run["workflow_key"]}
    if "dataset" in run:
        clear_kwargs["dataset"] = run["dataset"]
        clear_kwargs["release_date"] = run["release_date"]
    else:
        clear_kwargs["release_date"] = run["release_date"]
        clear_kwargs["run_id"] = run["run_id"]
    clear_approval_token(**clear_kwargs)


def run_approve_retry_wizard() -> None:
    """Interactive wizard for approving a failed child workflow for retry.

    Queries DynamoDB for run records with non-null approval_token (parents waiting
    for manual approval), validates each parent execution is still RUNNING,
    auto-clears stale tokens from dead parents, and presents a tree view of the
    retryable executions before prompting the user to select one.
    """
    console.print("[dim]Searching for runs awaiting approval...[/dim]")

    awaiting = get_runs_awaiting_approval()

    if not awaiting:
        console.print("[yellow]No runs are currently awaiting retry approval.[/yellow]")
        return

    # Validate parent executions are still RUNNING; store describe_execution response alongside each run.
    sfn = boto3.client("stepfunctions")
    live: list[tuple[dict, dict | None]] = []
    for run in awaiting:
        arn = run.get("step_function_execution_arn")
        if not arn:
            live.append((run, None))
            continue
        try:
            resp = sfn.describe_execution(executionArn=arn)
            if resp["status"] == "RUNNING":
                # For dataset-ingest records (no run_id field), extract run_id from execution name.
                if "dataset" in run and "run_id" not in run:
                    run_id = extract_run_id_from_execution_name(
                        execution_name=resp["name"],
                        workflow_key=run["workflow_key"],
                        release_date=run["release_date"],
                    )
                    if run_id:
                        run["run_id"] = run_id
                live.append((run, resp))
            else:
                console.print(
                    f"[dim]Clearing stale token: {run['workflow_key']} "
                    f"release_date={run['release_date']} (parent {resp['status']})[/dim]"
                )
                clear_approval(run)
        except (sfn.exceptions.ExecutionDoesNotExist, sfn.exceptions.InvalidArn):
            log.warning(f"Execution not found: {arn}")
            clear_approval(run)
        except Exception:
            log.warning(f"Failed to describe execution {arn}", exc_info=True)
            live.append((run, None))

    if not live:
        console.print("[yellow]No runs are currently awaiting retry approval.[/yellow]")
        return

    # Build execution tree structures, deduplicated by parent ARN.
    parent_executions: dict[str, dict] = {}
    retryable_children: set[str] = set()
    run_by_child_name: dict[str, dict] = {}
    runs_without_parent: list[tuple[None, dict]] = []

    # Collect unique parent ARNs with their run/resp data.
    parents_to_fetch: dict[str, tuple[dict, dict]] = {}
    for run, parent_resp in live:
        if parent_resp is None:
            runs_without_parent.append((None, run))
            continue
        arn = run["step_function_execution_arn"]
        if arn not in parents_to_fetch:
            parents_to_fetch[arn] = (run, parent_resp)

    # Fetch children in parallel.
    arns = list(parents_to_fetch.keys())
    children_lists = thread_map(lambda arn: fetch_child_executions(sfn_client=sfn, parent_arn=arn), arns)

    for arn, children in zip(arns, children_lists, strict=True):
        run, parent_resp = parents_to_fetch[arn]
        parent_executions[arn] = build_execution_dict(
            workflow=run["workflow_key"], execution=parent_resp, children=children
        )

    # Match retryable children — one approval token per run, so only keep the latest failure per token.
    candidates: dict[str, tuple[dict, dict]] = {}  # token → (latest_child, run)
    for run, parent_resp in live:
        if parent_resp is None:
            continue
        arn = run["step_function_execution_arn"]
        token = run["approval_token"]
        for child in parent_executions[arn]["children"]:
            if child["status"] == "FAILED" and run["approval_task_name"] in child["name"]:
                prev = candidates.get(token)
                if prev is None or child["start_date"] > prev[0]["start_date"]:
                    candidates[token] = (child, run)

    for child, run in candidates.values():
        retryable_children.add(child["name"])
        run_by_child_name[child["name"]] = run

    if parent_executions:
        console.print(
            build_execution_tree(
                title="State Machine Executions",
                executions=list(parent_executions.values()),
                retryable_children=frozenset(retryable_children),
            )
        )
        console.print(f"[dim]{len(parent_executions)} execution(s) shown.[/dim]")

    all_retryable: list[tuple[str | None, dict]] = list(run_by_child_name.items()) + runs_without_parent

    if not all_retryable:
        console.print("[yellow]No retryable children found.[/yellow]")
        return

    if len(all_retryable) == 1:
        child_name, selected = all_retryable[0]
        task_name = selected["approval_task_name"]
        prompt = f"Retry {task_name} ({child_name})?" if child_name else f"Retry {task_name}?"
        if not questionary.confirm(prompt, default=True, auto_enter=False).ask():
            return
    else:
        choices = [
            questionary.Choice(
                child_name if child_name else run["approval_task_name"],
                value=(child_name, run),
            )
            for child_name, run in all_retryable
        ]
        result = questionary.select("Select execution to retry:", choices=choices).ask()
        if result is None:
            return
        child_name, selected = result
        if not questionary.confirm(
            f"Send approval to retry {selected['approval_task_name']}?", default=True, auto_enter=False
        ).ask():
            return

    sfn.send_task_success(taskToken=selected["approval_token"], output="{}")
    console.print(
        f"[green]Approved retry for {selected['approval_task_name']}. Parent will re-invoke the child.[/green]"
    )
    clear_approval(selected)
