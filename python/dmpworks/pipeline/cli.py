"""Pipeline CLI for inspecting state, managing schedules, and triggering Step Function runs."""

from __future__ import annotations

from typing import Annotated, Literal

from cyclopts import App, Parameter

app = App(name="pipeline", help="Interactive pipeline management commands.")
run_app = App(name="run", help="Start a Step Function execution interactively.")
app.command(run_app)

EnvTypes = Literal["dev", "stg", "prd"]

# Known workflow keys for checkpoint scanning.
DATASET_WORKFLOW_KEYS = (
    "openalex-works",
    "datacite",
    "crossref-metadata",
    "ror",
    "data-citation-corpus",
)
PIPELINE_WORKFLOW_KEYS = (
    "process-works",
    "process-dmps",
)

# Known task names per workflow for checkpoint scanning.
DATASET_TASK_NAMES = ("download", "subset", "transform")
PROCESS_WORKS_TASK_NAMES = ("sqlmesh", "sync-works")
PROCESS_DMPS_TASK_NAMES = ("sync-dmps", "enrich-dmps", "dmp-works-search", "merge-related-works")


@app.command(name="status")
def status_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
) -> None:
    """Display the current pipeline state from DynamoDB."""
    import os

    os.environ["AWS_ENV"] = env
    os.environ.setdefault("AWS_REGION", "us-west-2")

    from dmpworks.pipeline.display import (
        display_dataset_releases,
        display_process_dmps_runs,
        display_process_works_runs,
        display_task_checkpoints,
    )
    from dmpworks.scheduler.dynamodb_store import (
        DatasetReleaseRecord,
        scan_all_process_dmps_runs,
        scan_all_process_works_runs,
        scan_task_checkpoints,
    )

    # Dataset releases — query latest per dataset.
    releases = []
    for dataset in DATASET_WORKFLOW_KEYS:
        releases.extend(DatasetReleaseRecord.query(dataset, scan_index_forward=False, limit=3))
    display_dataset_releases(records=releases)

    # Task checkpoints — scan all known workflow/task combos.
    checkpoints = []
    for wk in DATASET_WORKFLOW_KEYS:
        for tn in DATASET_TASK_NAMES:
            checkpoints.extend(scan_task_checkpoints(workflow_key=wk, task_name=tn))
    for tn in PROCESS_WORKS_TASK_NAMES:
        checkpoints.extend(scan_task_checkpoints(workflow_key="process-works", task_name=tn))
    for tn in PROCESS_DMPS_TASK_NAMES:
        checkpoints.extend(scan_task_checkpoints(workflow_key="process-dmps", task_name=tn))
    display_task_checkpoints(records=checkpoints)

    # Process runs.
    display_process_works_runs(records=scan_all_process_works_runs())
    display_process_dmps_runs(records=scan_all_process_dmps_runs())


@app.command(name="schedules")
def schedules_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    pause: Annotated[
        bool,
        Parameter(help="Disable all (or the specified) EventBridge rules."),
    ] = False,
    resume: Annotated[
        bool,
        Parameter(help="Enable all (or the specified) EventBridge rules."),
    ] = False,
    rule: Annotated[
        str | None,
        Parameter(help="Specific rule suffix to pause/resume (e.g., version-checker-schedule)."),
    ] = None,
) -> None:
    """Show, pause, or resume EventBridge schedule rules."""
    import boto3

    from dmpworks.pipeline.aws import SCHEDULE_RULES, get_eventbridge_rule_name
    from dmpworks.pipeline.display import display_schedules

    events = boto3.client("events")

    target_rules = [rule] if rule else list(SCHEDULE_RULES)

    if pause or resume:
        for r in target_rules:
            rule_name = get_eventbridge_rule_name(env=env, rule=r)
            if pause:
                events.disable_rule(Name=rule_name)
                print(f"Disabled: {rule_name}")
            else:
                events.enable_rule(Name=rule_name)
                print(f"Enabled: {rule_name}")

    # Always show current state after any action.
    rules_info = []
    for r in SCHEDULE_RULES:
        rule_name = get_eventbridge_rule_name(env=env, rule=r)
        try:
            resp = events.describe_rule(Name=rule_name)
            rules_info.append(
                {
                    "name": rule_name,
                    "schedule_expression": resp.get("ScheduleExpression", ""),
                    "state": resp.get("State", "UNKNOWN"),
                    "description": resp.get("Description", ""),
                }
            )
        except events.exceptions.ResourceNotFoundException:
            rules_info.append(
                {
                    "name": rule_name,
                    "schedule_expression": "",
                    "state": "NOT FOUND",
                    "description": "",
                }
            )
    display_schedules(rules=rules_info)


@app.command(name="check-versions")
def check_versions_cmd(
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


@app.command(name="cleanup")
def cleanup_cmd(
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


@run_app.command(name="ingest")
def run_ingest_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    bucket_name: Annotated[
        str,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name."),
    ],
) -> None:
    """Interactively start a dataset ingest SFN execution."""
    import os

    os.environ["AWS_ENV"] = env
    os.environ.setdefault("AWS_REGION", "us-west-2")

    from dmpworks.pipeline.interactive import run_ingest_wizard

    run_ingest_wizard(env=env, bucket_name=bucket_name)


@run_app.command(name="process-works")
def run_process_works_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    bucket_name: Annotated[
        str,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name."),
    ],
) -> None:
    """Interactively start a process-works SFN execution."""
    import os

    os.environ["AWS_ENV"] = env
    os.environ.setdefault("AWS_REGION", "us-west-2")

    from dmpworks.pipeline.interactive import run_process_works_wizard

    run_process_works_wizard(env=env, bucket_name=bucket_name)


@run_app.command(name="process-dmps")
def run_process_dmps_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    bucket_name: Annotated[
        str,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name."),
    ],
) -> None:
    """Interactively start a process-dmps SFN execution."""
    import os

    os.environ["AWS_ENV"] = env
    os.environ.setdefault("AWS_REGION", "us-west-2")

    from dmpworks.pipeline.interactive import run_process_dmps_wizard

    run_process_dmps_wizard(env=env, bucket_name=bucket_name)


@app.command(name="delete-checkpoints")
def delete_checkpoints_cmd(
    env: Annotated[
        EnvTypes,
        Parameter(env_var="AWS_ENV", help="Environment (e.g., dev, stg, prd)."),
    ],
    bucket_name: Annotated[
        str,
        Parameter(env_var="BUCKET_NAME", help="S3 bucket name (for scheduling old prefix expiry)."),
    ],
) -> None:
    """Interactively select and delete task checkpoints."""
    import os

    os.environ["AWS_ENV"] = env
    os.environ.setdefault("AWS_REGION", "us-west-2")

    import questionary

    from dmpworks.pipeline.display import display_task_checkpoints
    from dmpworks.pipeline.interactive import _delete_checkpoint_and_cleanup
    from dmpworks.scheduler.dynamodb_store import scan_task_checkpoints

    # Scan all known checkpoints.
    all_checkpoints = []
    for wk in DATASET_WORKFLOW_KEYS:
        for tn in DATASET_TASK_NAMES:
            all_checkpoints.extend(scan_task_checkpoints(workflow_key=wk, task_name=tn))
    for tn in PROCESS_WORKS_TASK_NAMES:
        all_checkpoints.extend(scan_task_checkpoints(workflow_key="process-works", task_name=tn))
    for tn in PROCESS_DMPS_TASK_NAMES:
        all_checkpoints.extend(scan_task_checkpoints(workflow_key="process-dmps", task_name=tn))

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
        _delete_checkpoint_and_cleanup(
            workflow_key=cp.workflow_key, task_name=task_name, date=date, bucket_name=bucket_name
        )

    print(f"Deleted {len(selected)} checkpoint(s).")
