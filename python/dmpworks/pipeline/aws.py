"""Infrastructure discovery helpers for resolving AWS resource names and ARNs by environment."""

from __future__ import annotations

from functools import lru_cache
import json
import logging
import os
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from datetime import datetime

log = logging.getLogger(__name__)

# Known EventBridge schedule rule suffixes.
SCHEDULE_RULES = (
    "version-checker-schedule",
    "process-works-schedule",
    "process-dmps-schedule",
    "s3-cleanup-schedule",
)

# State machine workflow name suffixes used in ARN construction.
STATE_MACHINE_WORKFLOWS = {
    "dataset-ingest": "dataset-ingest",
    "process-works": "process-works",
    "process-dmps": "process-dmps",
}


@lru_cache(maxsize=1)
def _get_account_and_region() -> tuple[str, str]:
    """Return (account_id, region) from the current AWS session."""
    sts = boto3.client("sts")
    identity = sts.get_caller_identity()
    region = boto3.session.Session().region_name
    return identity["Account"], region


def get_state_machine_arn(*, env: str, workflow: str) -> str:
    """Construct the ARN for a dmpworks state machine.

    Args:
        env: Environment name (dev, stg, prd).
        workflow: Workflow suffix (e.g. "dataset-ingest", "process-works", "process-dmps").

    Returns:
        Full state machine ARN.
    """
    account_id, region = _get_account_and_region()
    name = f"dmpworks-{env}-{workflow}"
    return f"arn:aws:states:{region}:{account_id}:stateMachine:{name}"


def get_lambda_function_name(*, env: str, function: str) -> str:
    """Return the Lambda function name by convention.

    Args:
        env: Environment name (dev, stg, prd).
        function: Function suffix (e.g. "version-checker", "s3-cleanup").

    Returns:
        Lambda function name string.
    """
    return f"dmpworks-{env}-{function}"


def get_bucket_name(*, env: str) -> str:
    """Return the S3 bucket name by convention.

    Args:
        env: Environment name (dev, stg, prd).

    Returns:
        S3 bucket name string.
    """
    return f"dmpworks-{env}-s3"


def get_eventbridge_rule_name(*, env: str, rule: str) -> str:
    """Return the EventBridge rule name by convention.

    Args:
        env: Environment name (dev, stg, prd).
        rule: Rule suffix (e.g. "version-checker-schedule").

    Returns:
        EventBridge rule name string.
    """
    return f"dmpworks-{env}-{rule}"


def set_env(*, env: str) -> None:
    """Set AWS environment variables for PynamoDB and other AWS SDK consumers.

    Args:
        env: Environment name (dev, stg, prd).
    """
    region = os.environ.get("AWS_REGION") or boto3.session.Session().region_name
    if not region:
        raise SystemExit(
            "AWS region could not be determined. Set AWS_REGION or configure a default region in ~/.aws/config."
        )
    os.environ["AWS_REGION"] = region
    os.environ["AWS_ENV"] = env


def resolve_bucket_name(*, env: str, bucket_name: str | None) -> str:
    """Resolve the bucket name, falling back to convention if not provided.

    Args:
        env: Environment name (dev, stg, prd).
        bucket_name: Explicit bucket name, or None to use convention.

    Returns:
        Resolved bucket name.
    """
    resolved = bucket_name or get_bucket_name(env=env)
    print(f"Using bucket: {resolved}")
    return resolved


def fetch_schedule_rules(*, env: str) -> list[dict[str, str]]:
    """Fetch the current state of all EventBridge schedule rules.

    Args:
        env: Environment name (dev, stg, prd).

    Returns:
        List of dicts with keys name, schedule_expression, state, description.
    """
    events = boto3.client("events")
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
    return rules_info


def toggle_schedule_rules(*, env: str, rule: str | None, enable: bool) -> None:
    """Enable or disable EventBridge schedule rules.

    Args:
        env: Environment name (dev, stg, prd).
        rule: Specific rule suffix, or None for all rules.
        enable: True to enable, False to disable.
    """
    events = boto3.client("events")
    target_rules = [rule] if rule else list(SCHEDULE_RULES)
    action = "enable_rule" if enable else "disable_rule"
    verb = "Enabled" if enable else "Disabled"
    for r in target_rules:
        rule_name = get_eventbridge_rule_name(env=env, rule=r)
        getattr(events, action)(Name=rule_name)
        print(f"{verb}: {rule_name}")


def fetch_child_executions(*, sfn_client: object, parent_arn: str) -> list[dict]:
    """Fetch child state machine executions for a parent execution.

    Paginates through the execution history to find all TaskSubmitted events
    for nested state machine invocations, then describes each child execution.

    Args:
        sfn_client: boto3 Step Functions client.
        parent_arn: ARN of the parent execution.

    Returns:
        List of dicts with keys name, status, start_date, stop_date, execution_arn,
        state_machine_arn, input.
    """
    children = []
    history_kwargs = {"executionArn": parent_arn, "maxResults": 200, "includeExecutionData": True}
    while True:
        history = sfn_client.get_execution_history(**history_kwargs)
        for event in history.get("events", []):
            if event.get("type") != "TaskSubmitted":
                continue
            details = event.get("taskSubmittedEventDetails", {})
            if details.get("resourceType") != "states":
                continue
            try:
                output = json.loads(details.get("output", "{}"))
                child_arn = output.get("ExecutionArn")
                if child_arn:
                    child_exec = sfn_client.describe_execution(executionArn=child_arn)
                    children.append(
                        {
                            "name": child_exec["name"],
                            "status": child_exec["status"],
                            "start_date": child_exec["startDate"],
                            "stop_date": child_exec.get("stopDate"),
                            "execution_arn": child_arn,
                            "state_machine_arn": child_exec["stateMachineArn"],
                            "input": json.loads(child_exec.get("input", "{}")),
                        }
                    )
            except (json.JSONDecodeError, KeyError):
                log.warning(f"Failed to parse child execution from event in {parent_arn}")
        if "nextToken" not in history:
            break
        history_kwargs["nextToken"] = history["nextToken"]
    return children


def list_sfn_executions(
    *, env: str, start_dt: datetime, end_dt: datetime, status_filter: str | None = None
) -> list[dict]:
    """List Step Functions executions across all workflows with child details.

    Args:
        env: Environment name (dev, stg, prd).
        start_dt: Start of the date range filter (inclusive).
        end_dt: End of the date range filter (inclusive).
        status_filter: Optional SFN status filter (e.g. RUNNING, SUCCEEDED).

    Returns:
        List of execution dicts with workflow, name, status, start_date, stop_date, children.
    """
    sfn = boto3.client("stepfunctions")
    all_executions = []

    for workflow in STATE_MACHINE_WORKFLOWS:
        sm_arn = get_state_machine_arn(env=env, workflow=workflow)
        list_kwargs: dict = {"stateMachineArn": sm_arn, "maxResults": 100}
        if status_filter:
            list_kwargs["statusFilter"] = status_filter.upper()

        parent_executions = []
        while True:
            response = sfn.list_executions(**list_kwargs)
            for ex in response.get("executions", []):
                if ex["startDate"] < start_dt:
                    break
                if ex["startDate"] <= end_dt:
                    parent_executions.append(ex)
            else:
                next_token = response.get("nextToken")
                if next_token:
                    list_kwargs["nextToken"] = next_token
                    continue
            break

        for parent in parent_executions:
            children = fetch_child_executions(sfn_client=sfn, parent_arn=parent["executionArn"])
            all_executions.append(
                {
                    "workflow": workflow,
                    "name": parent["name"],
                    "status": parent["status"],
                    "start_date": parent["startDate"],
                    "stop_date": parent.get("stopDate"),
                    "children": children,
                }
            )

    return all_executions
