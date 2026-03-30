"""Lambda entry point for scheduling stale S3 run data for expiry via lifecycle rules."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

import boto3

from dmpworks.scheduler.config import S3CleanupEnvSettings
from dmpworks.scheduler.dynamodb_store import mark_cleanup_scheduled
from dmpworks.scheduler.s3_cleanup import build_cleanup_plan

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)


def remove_stale_lifecycle_rules(*, bucket_name: str, s3_client) -> int:
    """Remove cleanup-* lifecycle rules whose S3 prefixes no longer contain objects.

    Checks each cleanup-* rule by listing objects under its prefix. If the prefix is
    empty (objects have already been expired by S3), the rule is removed. Non-cleanup
    rules (including the CloudFormation-managed ICMU rule) are always preserved.

    Args:
        bucket_name: The S3 bucket to check.
        s3_client: A boto3 S3 client.

    Returns:
        The number of stale rules removed.
    """
    try:
        existing = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        rules = existing["Rules"]
    except s3_client.exceptions.from_code("NoSuchLifecycleConfiguration"):
        return 0

    keep_rules = []
    removed_count = 0

    for rule in rules:
        if not rule["ID"].startswith("cleanup-"):
            keep_rules.append(rule)
            continue

        prefix = rule.get("Filter", {}).get("Prefix", "")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        if response.get("KeyCount", 0) > 0:
            keep_rules.append(rule)
        else:
            removed_count += 1
            log.info(f"Removing stale lifecycle rule: {rule['ID']} (prefix {prefix!r} is empty)")

    if removed_count > 0:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={"Rules": keep_rules},
        )
        log.info(f"Removed {removed_count} stale lifecycle rules")

    return removed_count


def s3_cleanup_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    """Build S3 cleanup plan from DynamoDB and schedule stale prefixes for expiry via lifecycle rules.

    Identifies process-works and dmp-works-search S3 prefixes belonging to old completed
    runs, then writes S3 lifecycle rules so AWS expires those objects within 24 hours.
    Previously written cleanup rules are removed on each run to keep the configuration tidy.

    When ``dry_run`` is true in the event payload, the plan is computed and returned
    but lifecycle rules are not written.

    Args:
        event: EventBridge scheduled event or manual invocation with optional ``dry_run`` field.
        context: Lambda context.

    Returns:
        Dict with scheduled_count, and plan list when dry_run is true.
    """
    dry_run = event.get("dry_run", False)
    settings = S3CleanupEnvSettings()
    stale_prefixes = build_cleanup_plan(bucket_name=settings.bucket_name)

    log.info(f"S3 cleanup plan: {len(stale_prefixes)} stale prefixes identified")
    for item in stale_prefixes:
        log.info(f"Stale prefix: s3://{item['bucket_name']}/{item['prefix_type']}/{item['run_id']}/")

    if dry_run:
        log.info(f"Dry run — returning plan without applying lifecycle rules ({len(stale_prefixes)} prefixes)")
        return {"scheduled_count": 0, "dry_run": True, "plan": stale_prefixes}

    if not stale_prefixes:
        log.info("No stale prefixes found — nothing to schedule")
        return {"scheduled_count": 0}

    s3 = boto3.client("s3")
    try:
        existing = s3.get_bucket_lifecycle_configuration(Bucket=settings.bucket_name)
        rules = [r for r in existing["Rules"] if not r["ID"].startswith("cleanup-")]
    except s3.exceptions.from_code("NoSuchLifecycleConfiguration"):
        rules = []

    for item in stale_prefixes:
        rules.append(
            {
                "ID": f"cleanup-{item['prefix_type']}-{item['run_id']}",
                "Filter": {"Prefix": f"{item['prefix_type']}/{item['run_id']}/"},
                "Status": "Enabled",
                "Expiration": {"Days": 1},
            }
        )

    s3.put_bucket_lifecycle_configuration(
        Bucket=settings.bucket_name,
        LifecycleConfiguration={"Rules": rules},
    )
    log.info(f"Scheduled {len(stale_prefixes)} prefixes for S3 expiry via lifecycle rules")

    mark_cleanup_scheduled(items=stale_prefixes)
    stale_removed = remove_stale_lifecycle_rules(bucket_name=settings.bucket_name, s3_client=s3)
    return {"scheduled_count": len(stale_prefixes), "stale_rules_removed": stale_removed}
