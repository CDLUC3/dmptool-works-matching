"""DynamoDB persistence and discovery service for dataset releases."""

from __future__ import annotations

from datetime import UTC, datetime
import logging
import os
from typing import TYPE_CHECKING, Literal

from pynamodb.attributes import MapAttribute, UnicodeAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.indexes import GlobalSecondaryIndex, KeysOnlyProjection
from pynamodb.models import Model

SQLMESH_INITIAL_RUN_ID = "INITIAL"

if TYPE_CHECKING:
    from collections.abc import Callable

    from dmpworks.model.dataset_version_model import DatasetRelease

log = logging.getLogger(__name__)


class DatasetReleaseRecord(Model):
    """PynamoDB model for a discovered dataset release.

    Attributes:
        dataset: The dataset identifier (hash key).
        release_date: ISO date string "YYYY-MM-DD" (range key, sortable).
        status: Lifecycle status — DISCOVERED | STARTED | COMPLETED | FAILED | WAITING_FOR_APPROVAL.
        file_name: File to download, if applicable.
        download_url: Direct download URL, if applicable.
        file_hash: MD5 checksum for the file, if applicable.
        metadata: Arbitrary extra key/value pairs.
        step_function_execution_arn: ARN of the associated Step Functions execution, if started.
        approval_token: Task token for the parent SM's approval wait state, if awaiting retry approval.
        approval_task_name: Name of the child task awaiting retry approval.
        created_at: ISO datetime string of record creation.
        updated_at: ISO datetime string of last update.
    """

    class Meta:
        """PynamoDB table configuration."""

        table_name = f"dmpworks-{os.environ.get('AWS_ENV', '')}-dataset-releases"
        billing_mode = "PAY_PER_REQUEST"
        region = os.environ.get("AWS_REGION", "")

    dataset = UnicodeAttribute(hash_key=True)
    release_date = UnicodeAttribute(range_key=True)
    status = UnicodeAttribute(default="DISCOVERED")
    file_name = UnicodeAttribute(null=True)
    download_url = UnicodeAttribute(null=True)
    file_hash = UnicodeAttribute(null=True)
    metadata = MapAttribute(default=dict)
    step_function_execution_arn = UnicodeAttribute(null=True)
    approval_token = UnicodeAttribute(null=True)
    approval_task_name = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute()


class StepFunctionExecutionArnIndex(GlobalSecondaryIndex):
    """GSI for looking up TaskRunRecords by their Step Functions execution ARN.

    Enables EventBridge failure handlers to find the task run associated with
    a specific child SM execution without knowing run_name/run_id in advance.
    """

    class Meta:
        """GSI configuration."""

        index_name = "step-function-execution-arn-index"
        billing_mode = "PAY_PER_REQUEST"
        projection = KeysOnlyProjection()

    step_function_execution_arn = UnicodeAttribute(hash_key=True)


class TaskRunRecord(Model):
    """One record per Batch stage execution. Generic across all pipeline types.

    Attributes:
        run_name: Job name matching the factory run_name, e.g. "openalex-works-download".
        run_id: Unique execution ID, e.g. "2025-01-01T060000_a1b2c3d4".
        status: STARTED | COMPLETED | FAILED.
        step_function_execution_arn: ARN of the child SM execution that created this task run.
            Indexed via StepFunctionExecutionArnIndex for EventBridge-based failure lookups.
        metadata: Type-specific context (dataset, release_date, etc.).
        error: Error message if FAILED.
        created_at: ISO datetime string of record creation.
        updated_at: ISO datetime string of last update.
    """

    class Meta:
        """PynamoDB table configuration."""

        table_name = f"dmpworks-{os.environ.get('AWS_ENV', '')}-task-runs"
        billing_mode = "PAY_PER_REQUEST"
        region = os.environ.get("AWS_REGION", "")

    run_name = UnicodeAttribute(hash_key=True)
    run_id = UnicodeAttribute(range_key=True)
    status = UnicodeAttribute(default="STARTED")
    step_function_execution_arn = UnicodeAttribute(null=True)
    step_function_execution_arn_index = StepFunctionExecutionArnIndex()
    error = UnicodeAttribute(null=True)
    metadata = MapAttribute(default=dict)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute()


class TaskCheckpointRecord(Model):
    """Records the last completed run for a workflow task, used for skip-on-retry.

    Keyed by (workflow_key, task_key). Written when a task completes successfully;
    read at the start of each task to determine whether it can be skipped on re-run.

    Attributes:
        workflow_key: Workflow type identifier, e.g. "openalex-works", "process-works".
        task_key: Composite range key "{task_name}#{date}", e.g. "transform#2025-01-01".
            ISO dates sort lexicographically, enabling reverse-scan for latest-task queries.
        run_id: Run ID of the completed execution, e.g. "20250101T060000-abc123".
        completed_at: ISO datetime string when the task completed.
    """

    class Meta:
        """PynamoDB table configuration."""

        table_name = f"dmpworks-{os.environ.get('AWS_ENV', '')}-task-checkpoints"
        billing_mode = "PAY_PER_REQUEST"
        region = os.environ.get("AWS_REGION", "")

    workflow_key = UnicodeAttribute(hash_key=True)
    task_key = UnicodeAttribute(range_key=True)
    run_id = UnicodeAttribute()
    completed_at = UnicodeAttribute()


def get_latest(model_cls: type[Model], hash_key: str) -> Model | None:
    """Return the most recent record for a hash key (reverse range-key scan, limit 1).

    Args:
        model_cls: PynamoDB model class to query.
        hash_key: Hash key value.

    Returns:
        The most recent record, or None if no records exist.
    """
    return next(model_cls.query(hash_key, scan_index_forward=False, limit=1), None)


def scan_by_date_range(
    model_cls: type[Model], *, start_date: str | None = None, end_date: str | None = None
) -> list[Model]:
    """Scan a table with an optional release_date range filter.

    Args:
        model_cls: PynamoDB model class to scan (must have a release_date attribute).
        start_date: Optional ISO date lower bound (inclusive).
        end_date: Optional ISO date upper bound (inclusive).

    Returns:
        List of model instances.
    """
    if start_date and end_date:
        condition = model_cls.release_date.between(start_date, end_date)
    elif start_date:
        condition = model_cls.release_date >= start_date
    elif end_date:
        condition = model_cls.release_date <= end_date
    else:
        return list(model_cls.scan())
    return list(model_cls.scan(filter_condition=condition))


def update_record(model_cls: type[Model], *keys, status: str | None = None, **kwargs) -> None:
    """Fetch a record by keys and apply update actions.

    Always sets updated_at. Optionally sets status and any additional
    attributes passed as kwargs.

    Args:
        model_cls: PynamoDB model class.
        *keys: Positional hash/range key values passed to model_cls.get().
        status: If not None, set the record's status attribute.
        **kwargs: Additional attribute name/value pairs to set.
    """
    record = model_cls.get(*keys)
    actions = [model_cls.updated_at.set(datetime.now(UTC).isoformat())]
    if status is not None:
        actions.append(model_cls.status.set(status))
    for key, value in kwargs.items():
        actions.append(getattr(model_cls, key).set(value))
    record.update(actions=actions)


def get_latest_known_release(*, dataset: str) -> DatasetReleaseRecord | None:
    """Return the most recently published release record for a dataset.

    Args:
        dataset: The dataset identifier.

    Returns:
        The most recent DatasetReleaseRecord, or None if no records exist.
    """
    return get_latest(DatasetReleaseRecord, dataset)


def persist_discovered_release(*, dataset: str, release: DatasetRelease) -> DatasetReleaseRecord | None:
    """Persist a newly discovered release, deduplicating by (dataset, release_date).

    Args:
        dataset: The dataset identifier.
        release: The DatasetRelease returned by a detector function.

    Returns:
        The persisted DatasetReleaseRecord, or None if a record for this
        (dataset, release_date) already exists.
    """
    release_date_str = release.release_date.to_date_string()

    try:
        DatasetReleaseRecord.get(dataset, release_date_str)
    except DoesNotExist:
        pass
    else:
        log.debug(f"Release already exists: dataset={dataset} release_date={release_date_str}")
        return None

    now = datetime.now(UTC).isoformat()
    record = DatasetReleaseRecord(
        dataset=dataset,
        release_date=release_date_str,
        status="DISCOVERED",
        file_name=release.file_name,
        download_url=release.download_url,
        file_hash=release.file_hash,
        metadata=release.metadata,
        created_at=now,
        updated_at=now,
    )
    record.save()
    log.info(f"Persisted new release: dataset={dataset} release_date={release_date_str}")
    return record


def discover_latest_release(
    *,
    dataset: str,
    detector: Callable[..., DatasetRelease | None],
    detector_kwargs: dict,
) -> DatasetReleaseRecord | None:
    """Run a detector and persist the result if it represents a new release.

    Args:
        dataset: The dataset identifier.
        detector: A callable that returns a DatasetRelease or None.
        detector_kwargs: Keyword arguments to pass to the detector.

    Returns:
        The persisted DatasetReleaseRecord if a new release was discovered and
        not a duplicate, or None otherwise.
    """
    release = detector(**detector_kwargs)
    if release is None:
        log.info(f"Detector returned None for dataset={dataset}")
        return None

    log.info(f"Detector found release: dataset={dataset} release_date={release.release_date}")
    return persist_discovered_release(dataset=dataset, release=release)


def create_task_run(*, run_name: str, run_id: str, execution_arn: str, metadata: dict) -> TaskRunRecord:
    """Create a new TaskRunRecord with STARTED status.

    Args:
        run_name: Job name matching the factory run_name, e.g. "openalex-works-download".
        run_id: Unique execution ID for this stage run.
        execution_arn: ARN of the enclosing Step Functions execution.
        metadata: Type-specific context dict, e.g. {"dataset": ..., "release_date": ...}.

    Returns:
        The persisted TaskRunRecord.
    """
    now = datetime.now(UTC).isoformat()
    record = TaskRunRecord(
        run_name=run_name,
        run_id=run_id,
        status="STARTED",
        step_function_execution_arn=execution_arn,
        metadata=metadata,
        created_at=now,
        updated_at=now,
    )
    record.save()
    log.info(f"Created task run: run_name={run_name} run_id={run_id}")
    return record


def set_task_run_status(
    *, run_name: str, run_id: str, status: Literal["COMPLETED", "FAILED"], error: str | None = None
) -> None:
    """Update the status of a TaskRunRecord.

    Args:
        run_name: Hash key identifying the run name.
        run_id: Range key identifying the specific run.
        status: Target status — COMPLETED or FAILED.
        error: Optional error message or cause string (only meaningful when status is FAILED).
    """
    record = TaskRunRecord.get(run_name, run_id)
    actions = [
        TaskRunRecord.status.set(status),
        TaskRunRecord.updated_at.set(datetime.now(UTC).isoformat()),
    ]
    if error is not None:
        actions.append(TaskRunRecord.error.set(error))
    record.update(actions=actions)
    log.info(f"Set task run status: run_name={run_name} run_id={run_id} status={status}")


def set_task_checkpoint(*, workflow_key: str, task_name: str, date: str, run_id: str) -> None:
    """Write a completed task checkpoint to task-checkpoints.

    Creates or overwrites the checkpoint record for (workflow_key, task_name, date).
    Should be called only after the task has completed successfully.

    Args:
        workflow_key: Workflow type identifier, e.g. "openalex-works".
        task_name: Task within the workflow, e.g. "download".
        date: ISO date string identifying the workflow run, e.g. "2025-01-01".
        run_id: Run ID of the completed execution.
    """
    record = TaskCheckpointRecord(
        workflow_key=workflow_key,
        task_key=f"{task_name}#{date}",
        run_id=run_id,
        completed_at=datetime.now(UTC).isoformat(),
    )
    record.save()
    log.info(f"Set task checkpoint: workflow_key={workflow_key} task_name={task_name} date={date} run_id={run_id}")


def get_task_checkpoint(*, workflow_key: str, task_name: str, date: str | None = None) -> TaskCheckpointRecord | None:
    """Return the checkpoint for a workflow task, or None if it has not completed.

    If date is supplied, performs an exact lookup by (workflow_key, task_name, date).
    If omitted, returns the most recently completed checkpoint for the task across all dates.

    Args:
        workflow_key: Workflow type identifier, e.g. "openalex-works".
        task_name: Task within the workflow, e.g. "download".
        date: ISO date string identifying the workflow run. If None, returns the latest.

    Returns:
        The TaskCheckpointRecord if the task has completed, or None.
    """
    if date is not None:
        try:
            return TaskCheckpointRecord.get(workflow_key, f"{task_name}#{date}")
        except DoesNotExist:
            return None
    results = TaskCheckpointRecord.query(
        workflow_key,
        TaskCheckpointRecord.task_key.startswith(f"{task_name}#"),
        scan_index_forward=False,
        limit=1,
    )
    return next(iter(results), None)


def delete_task_checkpoint(*, workflow_key: str, task_name: str, date: str) -> TaskCheckpointRecord | None:
    """Delete a task checkpoint and return the deleted record for logging/cleanup.

    Args:
        workflow_key: Workflow type identifier, e.g. "openalex-works".
        task_name: Task within the workflow, e.g. "download".
        date: Release date (YYYY-MM-DD).

    Returns:
        The deleted TaskCheckpointRecord, or None if it did not exist.
    """
    try:
        record = TaskCheckpointRecord.get(workflow_key, f"{task_name}#{date}")
    except DoesNotExist:
        return None
    record.delete()
    log.info(
        f"Deleted task checkpoint: workflow_key={workflow_key} task_name={task_name} date={date} run_id={record.run_id}"
    )
    return record


def scan_task_checkpoints(
    *, workflow_key: str, task_name: str, start_date: str | None = None, end_date: str | None = None
) -> list[TaskCheckpointRecord]:
    """Return checkpoints for a workflow task, optionally filtered by date range.

    Args:
        workflow_key: Workflow type identifier, e.g. "openalex-works".
        task_name: Task within the workflow, e.g. "download".
        start_date: Optional ISO date lower bound (inclusive).
        end_date: Optional ISO date upper bound (inclusive).

    Returns:
        List of TaskCheckpointRecord instances for the given task.
    """
    if start_date and end_date:
        range_condition = TaskCheckpointRecord.task_key.between(f"{task_name}#{start_date}", f"{task_name}#{end_date}")
    elif start_date:
        range_condition = TaskCheckpointRecord.task_key >= f"{task_name}#{start_date}"
    elif end_date:
        range_condition = TaskCheckpointRecord.task_key.between(f"{task_name}#", f"{task_name}#{end_date}")
    else:
        range_condition = TaskCheckpointRecord.task_key.startswith(f"{task_name}#")
    return list(TaskCheckpointRecord.query(workflow_key, range_condition))


class ProcessWorksRunRecord(Model):
    """One record per process-works pipeline run.

    Keyed by (release_date, run_id). Created when all prerequisite dataset checkpoints
    are confirmed ready; updated as the pipeline progresses through SQLMesh and
    OpenSearch sync stages.

    Attributes:
        release_date: ISO date string "YYYY-MM-DD" identifying the monthly run (hash key).
        run_id: Unique execution ID (range key), e.g. "20250101T060000-abc123".
        status: Lifecycle status — STARTED | COMPLETED | FAILED.
        step_function_execution_arn: ARN of the enclosing Step Functions execution.
        run_id_sqlmesh_prev: Run ID of the prior SQLMesh execution (for incremental runs).
        run_id_openalex_works: Run ID of the OpenAlex Works transform checkpoint.
        run_id_datacite: Run ID of the DataCite transform checkpoint.
        run_id_crossref_metadata: Run ID of the Crossref Metadata transform checkpoint.
        run_id_ror: Run ID of the ROR download checkpoint.
        run_id_data_citation_corpus: Run ID of the Data Citation Corpus download checkpoint.
        run_id_sqlmesh: Run ID of the completed SQLMesh execution (set after sqlmesh completes).
        release_date_openalex_works: Release date of the OpenAlex Works release used.
        release_date_datacite: Release date of the DataCite release used.
        release_date_crossref_metadata: Release date of the Crossref Metadata release used.
        release_date_ror: Release date of the ROR release used.
        release_date_data_citation_corpus: Release date of the Data Citation Corpus release used.
        approval_token: Task token for the parent SM's approval wait state, if awaiting retry approval.
        approval_task_name: Name of the child task awaiting retry approval.
        error: Error message if FAILED.
        created_at: ISO datetime string of record creation.
        updated_at: ISO datetime string of last update.
    """

    class Meta:
        """PynamoDB table configuration."""

        table_name = f"dmpworks-{os.environ.get('AWS_ENV', '')}-process-works-runs"
        billing_mode = "PAY_PER_REQUEST"
        region = os.environ.get("AWS_REGION", "")

    release_date = UnicodeAttribute(hash_key=True)
    run_id = UnicodeAttribute(range_key=True)
    status = UnicodeAttribute(default="STARTED")
    step_function_execution_arn = UnicodeAttribute(null=True)
    run_id_sqlmesh_prev = UnicodeAttribute(null=True)
    run_id_openalex_works = UnicodeAttribute(null=True)
    run_id_datacite = UnicodeAttribute(null=True)
    run_id_crossref_metadata = UnicodeAttribute(null=True)
    run_id_ror = UnicodeAttribute(null=True)
    run_id_data_citation_corpus = UnicodeAttribute(null=True)
    run_id_sqlmesh = UnicodeAttribute(null=True)
    release_date_openalex_works = UnicodeAttribute(null=True)
    release_date_datacite = UnicodeAttribute(null=True)
    release_date_crossref_metadata = UnicodeAttribute(null=True)
    release_date_ror = UnicodeAttribute(null=True)
    release_date_data_citation_corpus = UnicodeAttribute(null=True)
    approval_token = UnicodeAttribute(null=True)
    approval_task_name = UnicodeAttribute(null=True)
    error = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute()


def update_release_status(
    *,
    dataset: str,
    release_date: str,
    status: Literal["DISCOVERED", "STARTED", "COMPLETED", "FAILED", "WAITING_FOR_APPROVAL"],
    **kwargs,
) -> None:
    """Update the status and timestamp of a DatasetReleaseRecord.

    Args:
        dataset: The dataset identifier.
        release_date: ISO date string "YYYY-MM-DD".
        status: Target lifecycle status — DISCOVERED | STARTED | COMPLETED | FAILED.
        **kwargs: Additional DatasetReleaseRecord attribute names and values to set,
            e.g. step_function_execution_arn="arn:...".
    """
    update_record(DatasetReleaseRecord, dataset, release_date, status=status, **kwargs)
    log.info(f"Marked release {status}: dataset={dataset} release_date={release_date}")


def create_process_works_run(
    *,
    release_date: str,
    run_id: str,
    execution_arn: str,
    run_id_sqlmesh_prev: str,
    run_id_openalex_works: str,
    run_id_datacite: str,
    run_id_crossref_metadata: str,
    run_id_ror: str,
    run_id_data_citation_corpus: str,
    release_date_openalex_works: str,
    release_date_datacite: str,
    release_date_crossref_metadata: str,
    release_date_ror: str,
    release_date_data_citation_corpus: str,
) -> ProcessWorksRunRecord:
    """Create a new ProcessWorksRunRecord with STARTED status.

    Args:
        release_date: ISO date string "YYYY-MM-DD" identifying the monthly run.
        run_id: Unique execution ID for this run.
        execution_arn: ARN of the enclosing Step Functions execution.
        run_id_sqlmesh_prev: Run ID of the prior SQLMesh execution.
        run_id_openalex_works: Run ID of the OpenAlex Works transform checkpoint.
        run_id_datacite: Run ID of the DataCite transform checkpoint.
        run_id_crossref_metadata: Run ID of the Crossref Metadata transform checkpoint.
        run_id_ror: Run ID of the ROR download checkpoint.
        run_id_data_citation_corpus: Run ID of the Data Citation Corpus download checkpoint.
        release_date_openalex_works: Release date of the OpenAlex Works release used.
        release_date_datacite: Release date of the DataCite release used.
        release_date_crossref_metadata: Release date of the Crossref Metadata release used.
        release_date_ror: Release date of the ROR release used.
        release_date_data_citation_corpus: Release date of the Data Citation Corpus release used.

    Returns:
        The persisted ProcessWorksRunRecord.
    """
    now = datetime.now(UTC).isoformat()
    record = ProcessWorksRunRecord(
        release_date=release_date,
        run_id=run_id,
        status="STARTED",
        step_function_execution_arn=execution_arn,
        run_id_sqlmesh_prev=run_id_sqlmesh_prev,
        run_id_openalex_works=run_id_openalex_works,
        run_id_datacite=run_id_datacite,
        run_id_crossref_metadata=run_id_crossref_metadata,
        run_id_ror=run_id_ror,
        run_id_data_citation_corpus=run_id_data_citation_corpus,
        release_date_openalex_works=release_date_openalex_works,
        release_date_datacite=release_date_datacite,
        release_date_crossref_metadata=release_date_crossref_metadata,
        release_date_ror=release_date_ror,
        release_date_data_citation_corpus=release_date_data_citation_corpus,
        created_at=now,
        updated_at=now,
    )
    record.save()
    log.info(f"Created process works run: release_date={release_date} run_id={run_id}")
    return record


def get_latest_process_works_run(*, release_date: str) -> ProcessWorksRunRecord | None:
    """Return the most recent ProcessWorksRunRecord for a given release_date.

    Args:
        release_date: ISO date string "YYYY-MM-DD" identifying the monthly run.

    Returns:
        The most recent ProcessWorksRunRecord, or None if no records exist.
    """
    return get_latest(ProcessWorksRunRecord, release_date)


def set_process_works_run_status(
    *,
    release_date: str,
    run_id: str,
    status: Literal["STARTED", "COMPLETED", "FAILED", "WAITING_FOR_APPROVAL"],
    **kwargs,
) -> None:
    """Update the status and timestamp of a ProcessWorksRunRecord.

    Args:
        release_date: Hash key identifying the monthly run date.
        run_id: Range key identifying the specific run.
        status: Target lifecycle status — STARTED | COMPLETED | FAILED | WAITING_FOR_APPROVAL.
        **kwargs: Additional ProcessWorksRunRecord attribute names and values to set,
            e.g. step_function_execution_arn="arn:...", run_id_sqlmesh="20250101T...", or error="...".
    """
    update_record(ProcessWorksRunRecord, release_date, run_id, status=status, **kwargs)
    log.info(f"Set process works run status: release_date={release_date} run_id={run_id} status={status}")


class ProcessDMPsRunRecord(Model):
    """One record per process-dmps pipeline run. Keyed by (release_date, run_id).

    Created when the pipeline starts and updated as each task completes.
    Task run IDs are recorded as each stage finishes for observability.

    Attributes:
        release_date: ISO date string "YYYY-MM-DD" identifying the daily run (hash key).
        run_id: Unique execution ID (range key), e.g. "20250101T060000-abc123".
        status: Lifecycle status — STARTED | COMPLETED | FAILED | WAITING_FOR_APPROVAL.
        step_function_execution_arn: ARN of the enclosing Step Functions execution.
        approval_token: Task token for the parent SM's approval wait state, if awaiting retry approval.
        approval_task_name: Name of the child task awaiting retry approval.
        error: Error message if FAILED.
        run_id_sync_dmps: Run ID of the sync-dmps task.
        run_id_enrich_dmps: Run ID of the enrich-dmps task.
        run_id_dmp_works_search: Run ID of the dmp-works-search task.
        run_id_merge_related_works: Run ID of the merge-related-works task.
        created_at: ISO datetime string of record creation.
        updated_at: ISO datetime string of last update.
    """

    class Meta:
        """PynamoDB table configuration."""

        table_name = f"dmpworks-{os.environ.get('AWS_ENV', '')}-process-dmps-runs"
        billing_mode = "PAY_PER_REQUEST"
        region = os.environ.get("AWS_REGION", "")

    release_date = UnicodeAttribute(hash_key=True)
    run_id = UnicodeAttribute(range_key=True)
    status = UnicodeAttribute(default="STARTED")
    step_function_execution_arn = UnicodeAttribute(null=True)
    approval_token = UnicodeAttribute(null=True)
    approval_task_name = UnicodeAttribute(null=True)
    error = UnicodeAttribute(null=True)
    run_id_sync_dmps = UnicodeAttribute(null=True)
    run_id_enrich_dmps = UnicodeAttribute(null=True)
    run_id_dmp_works_search = UnicodeAttribute(null=True)
    run_id_merge_related_works = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute()


def create_process_dmps_run(
    *,
    release_date: str,
    run_id: str,
    execution_arn: str,
) -> ProcessDMPsRunRecord:
    """Create a new ProcessDMPsRunRecord with STARTED status.

    Args:
        release_date: ISO date string "YYYY-MM-DD" identifying the daily run.
        run_id: Unique execution ID for this run.
        execution_arn: ARN of the enclosing Step Functions execution.

    Returns:
        The persisted ProcessDMPsRunRecord.
    """
    now = datetime.now(UTC).isoformat()
    record = ProcessDMPsRunRecord(
        release_date=release_date,
        run_id=run_id,
        status="STARTED",
        step_function_execution_arn=execution_arn,
        created_at=now,
        updated_at=now,
    )
    record.save()
    log.info(f"Created process DMPs run: release_date={release_date} run_id={run_id}")
    return record


def scan_all_process_works_runs(
    *, start_date: str | None = None, end_date: str | None = None
) -> list[ProcessWorksRunRecord]:
    """Return ProcessWorksRunRecord entries, optionally filtered by release_date range.

    Args:
        start_date: Optional ISO date lower bound (inclusive).
        end_date: Optional ISO date upper bound (inclusive).

    Returns:
        List of ProcessWorksRunRecord instances.
    """
    return scan_by_date_range(ProcessWorksRunRecord, start_date=start_date, end_date=end_date)


def scan_all_process_dmps_runs(
    *, start_date: str | None = None, end_date: str | None = None
) -> list[ProcessDMPsRunRecord]:
    """Return ProcessDMPsRunRecord entries, optionally filtered by release_date range.

    Args:
        start_date: Optional ISO date lower bound (inclusive).
        end_date: Optional ISO date upper bound (inclusive).

    Returns:
        List of ProcessDMPsRunRecord instances.
    """
    return scan_by_date_range(ProcessDMPsRunRecord, start_date=start_date, end_date=end_date)


def get_latest_process_dmps_run(*, release_date: str) -> ProcessDMPsRunRecord | None:
    """Return the most recent ProcessDMPsRunRecord for a given release_date.

    Args:
        release_date: ISO date string "YYYY-MM-DD" identifying the daily run.

    Returns:
        The most recent ProcessDMPsRunRecord, or None if no records exist.
    """
    return get_latest(ProcessDMPsRunRecord, release_date)


def set_process_dmps_run_status(
    *,
    release_date: str,
    run_id: str,
    status: Literal["STARTED", "COMPLETED", "FAILED", "WAITING_FOR_APPROVAL"] | None = None,
    **kwargs,
) -> None:
    """Update fields on a ProcessDMPsRunRecord.

    Only the provided (non-None) arguments are updated; omitted fields are left unchanged.
    This allows both full status updates and partial task run_id recording in a single function.

    Args:
        release_date: Hash key identifying the daily run date.
        run_id: Range key identifying the specific run.
        status: Target lifecycle status. If None, status is not updated.
        **kwargs: Additional ProcessDMPsRunRecord attribute names and values to set,
            e.g. run_id_sync_dmps="20250101T...", error="...", approval_token="...".
    """
    update_record(ProcessDMPsRunRecord, release_date, run_id, status=status, **kwargs)
    log.info(f"Set process DMPs run status: release_date={release_date} run_id={run_id} status={status}")


def clear_approval_token(*, workflow_key: str, **keys) -> None:
    """Clear approval_token and approval_task_name from a run record.

    Routes to the correct model based on workflow_key. For dataset-ingest workflows,
    keys should include dataset and release_date. For process-works/process-dmps,
    keys should include release_date and run_id.

    Args:
        workflow_key: Workflow identifier (e.g. "openalex-works", "process-works", "process-dmps").
        **keys: Primary key fields for the record.
    """
    if workflow_key == "process-dmps":
        model_cls = ProcessDMPsRunRecord
        record = model_cls.get(keys["release_date"], keys["run_id"])
    elif workflow_key == "process-works":
        model_cls = ProcessWorksRunRecord
        record = model_cls.get(keys["release_date"], keys["run_id"])
    else:
        model_cls = DatasetReleaseRecord
        record = model_cls.get(keys["dataset"], keys["release_date"])

    now = datetime.now(UTC).isoformat()
    record.update(
        actions=[
            model_cls.approval_token.remove(),
            model_cls.approval_task_name.remove(),
            model_cls.updated_at.set(now),
        ]
    )
    log.info(f"Cleared approval token: workflow_key={workflow_key} keys={keys}")


def get_runs_awaiting_approval() -> list[dict]:
    """Scan all run record tables for records with a non-null approval_token.

    Returns:
        List of dicts with keys: workflow_key, approval_token, approval_task_name,
        and the primary key fields for each record type.
    """
    results = [
        {
            "workflow_key": record.dataset,
            "dataset": record.dataset,
            "release_date": record.release_date,
            "approval_token": record.approval_token,
            "approval_task_name": record.approval_task_name,
        }
        for record in DatasetReleaseRecord.scan(
            filter_condition=DatasetReleaseRecord.approval_token.exists(),
        )
    ]

    results.extend(
        {
            "workflow_key": "process-works",
            "release_date": record.release_date,
            "run_id": record.run_id,
            "approval_token": record.approval_token,
            "approval_task_name": record.approval_task_name,
        }
        for record in ProcessWorksRunRecord.scan(
            filter_condition=ProcessWorksRunRecord.approval_token.exists(),
        )
    )

    results.extend(
        {
            "workflow_key": "process-dmps",
            "release_date": record.release_date,
            "run_id": record.run_id,
            "approval_token": record.approval_token,
            "approval_task_name": record.approval_task_name,
        }
        for record in ProcessDMPsRunRecord.scan(
            filter_condition=ProcessDMPsRunRecord.approval_token.exists(),
        )
    )

    return results


def scan_task_runs_by_run_name(*, run_name: str) -> list[TaskRunRecord]:
    """Query all TaskRunRecords for a given run_name.

    Args:
        run_name: The run_name hash key (e.g. "openalex-works-download").

    Returns:
        List of TaskRunRecord instances.
    """
    return list(TaskRunRecord.query(run_name))
