"""Workflow helpers for building Batch job parameters from Step Functions events."""

from __future__ import annotations

from datetime import UTC, datetime
import secrets
from typing import TYPE_CHECKING, Any

from dmpworks.batch_submit.job_registry import JOB_FACTORIES
from dmpworks.scheduler.dynamodb_store import get_task_checkpoint

if TYPE_CHECKING:
    from dmpworks.scheduler.config import LambdaConfig


def generate_run_id() -> str:
    """Generate a unique run ID for a task run execution.

    Format: {YYYYMMDDTHHMMSS}-{8-char hex}
    e.g. "20250101T060012-a1b2c3d4"

    Returns:
        A unique run ID string.
    """
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    return f"{ts}-{secrets.token_hex(4)}"


def compute_batch_params(event: dict[str, Any], config: LambdaConfig) -> dict[str, Any]:
    """Build Batch job parameters for a task run using JOB_FACTORIES.

    Supports single-predecessor via predecessor_task_name (existing ingest pipeline).

    Args:
        event: Workflow event containing workflow_key, task_type, release_date,
            bucket_name, aws_env, and optional predecessor fields.
        config: Lambda config loaded from SSM.

    Returns:
        Dict with run_id, run_name, and batch_params keys.
    """
    workflow_key = event["workflow_key"]
    task_type = event["task_type"]
    run_id = event.get("run_id") or generate_run_id()

    # Build flat keyword args: config env vars (lowercased) + event fields
    factory_kwargs: dict[str, Any] = {k.lower(): v for k, v in config.to_env_dict().items() if v is not None}
    factory_kwargs.update({k: v for k, v in event.items() if v is not None})
    factory_kwargs["run_id"] = run_id
    factory_kwargs["env"] = event["aws_env"]  # align SFN "aws_env" to factory "env" parameter
    factory_kwargs["dataset"] = workflow_key  # align SFN "workflow_key" to factory "dataset" parameter (subset jobs)

    # Single-predecessor (ingest pipeline): predecessor_task_name → prev_job_run_id
    predecessor_task_name = event.get("predecessor_task_name")
    if predecessor_task_name:
        checkpoint = get_task_checkpoint(
            workflow_key=workflow_key, task_name=predecessor_task_name, date=event["release_date"]
        )
        factory_kwargs["prev_job_run_id"] = checkpoint.run_id if checkpoint else None
    else:
        factory_kwargs.setdefault("prev_job_run_id", None)

    # Align SFN event key "use_subset" to factory parameter "dataset_subset_enable" (subset jobs)
    if "use_subset" in factory_kwargs:
        factory_kwargs.setdefault("dataset_subset_enable", factory_kwargs["use_subset"])

    factory = JOB_FACTORIES[(workflow_key, task_type)]
    result = factory(**factory_kwargs)
    run_name = result.pop("run_name")  # remove non-AWS field; use as DynamoDB run_name
    result["JobName"] = f"{run_name}-{event['release_date']}-{run_id}"

    return {
        "run_id": run_id,
        "run_name": run_name,
        "batch_params": result,
    }
