"""S3 lifecycle rule helpers for cleaning up orphaned run prefixes."""

from __future__ import annotations

import logging

import boto3

from dmpworks.scheduler.s3_cleanup import DATASET_TASKS, SQLMESH_TASK

log = logging.getLogger(__name__)

# Map (workflow_key, task_name) -> s3_prefix_type for all tasks that produce S3 output.
PREFIX_TYPE_MAP: dict[tuple[str, str], str] = {(wk, tn): prefix_type for wk, tn, prefix_type, _ in DATASET_TASKS}
PREFIX_TYPE_MAP[(SQLMESH_TASK[0], SQLMESH_TASK[1])] = SQLMESH_TASK[2]
PREFIX_TYPE_MAP[("process-dmps", "dmp-works-search")] = "dmp-works-search"


def get_prefix_type(*, workflow_key: str, task_name: str) -> str | None:
    """Return the S3 prefix type for a task, or None if the task has no S3 output.

    Args:
        workflow_key: e.g. "openalex-works", "process-works", "process-dmps".
        task_name: e.g. "download", "sqlmesh", "dmp-works-search".

    Returns:
        The S3 prefix type string, or None if this task doesn't produce S3 data.
    """
    return PREFIX_TYPE_MAP.get((workflow_key, task_name))


def schedule_prefix_expiry(*, bucket_name: str, prefix_type: str, run_id: str) -> None:
    """Write an S3 lifecycle rule to expire a stale run prefix within 24 hours.

    Uses the same rule ID and filter format as the monthly S3 cleanup Lambda, so the
    next cleanup run will naturally clean up these rules.

    Args:
        bucket_name: The S3 bucket containing the run data.
        prefix_type: The S3 prefix type (e.g. "openalex-works-download", "sqlmesh").
        run_id: The run ID whose prefix should be expired.
    """
    s3 = boto3.client("s3")

    try:
        existing = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        rules = existing["Rules"]
    except s3.exceptions.from_code("NoSuchLifecycleConfiguration"):
        rules = []

    rule_id = f"cleanup-{prefix_type}-{run_id}"

    # Don't add a duplicate rule.
    if any(r["ID"] == rule_id for r in rules):
        log.info(f"S3 lifecycle rule already exists: {rule_id}")
        return

    rules.append(
        {
            "ID": rule_id,
            "Filter": {"Prefix": f"{prefix_type}/{run_id}/"},
            "Status": "Enabled",
            "Expiration": {"Days": 1},
        }
    )

    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={"Rules": rules},
    )
    log.info(f"Scheduled S3 prefix for expiry: s3://{bucket_name}/{prefix_type}/{run_id}/")
