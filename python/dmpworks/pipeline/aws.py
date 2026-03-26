"""Infrastructure discovery helpers for resolving AWS resource names and ARNs by environment."""

from __future__ import annotations

from functools import lru_cache
import logging

import boto3

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


def get_eventbridge_rule_name(*, env: str, rule: str) -> str:
    """Return the EventBridge rule name by convention.

    Args:
        env: Environment name (dev, stg, prd).
        rule: Rule suffix (e.g. "version-checker-schedule").

    Returns:
        EventBridge rule name string.
    """
    return f"dmpworks-{env}-{rule}"
