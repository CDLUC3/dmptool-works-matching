"""Lambda entry point for scheduling stale S3 run data for expiry via lifecycle rules."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

import boto3

from dmpworks.scheduler.config import S3CleanupEnvSettings
from dmpworks.scheduler.s3_cleanup import build_cleanup_plan

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)


def s3_cleanup_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    """Build S3 cleanup plan from DynamoDB and schedule stale prefixes for expiry via lifecycle rules.

    Identifies process-works and dmp-works-search S3 prefixes belonging to old completed
    runs, then writes S3 lifecycle rules so AWS expires those objects within 24 hours.
    Previously written cleanup rules are removed on each run to keep the configuration tidy.

    Args:
        event: EventBridge scheduled event (no required fields used).
        context: Lambda context.

    Returns:
        Dict with scheduled_count indicating how many prefixes were scheduled for expiry.
    """
    settings = S3CleanupEnvSettings()
    stale_prefixes = build_cleanup_plan(bucket_name=settings.bucket_name)

    log.info(f"S3 cleanup plan: {len(stale_prefixes)} stale prefixes identified")
    for item in stale_prefixes:
        log.info(f"Stale prefix: s3://{item['bucket_name']}/{item['prefix_type']}/{item['run_id']}/")

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
    return {"scheduled_count": len(stale_prefixes)}
