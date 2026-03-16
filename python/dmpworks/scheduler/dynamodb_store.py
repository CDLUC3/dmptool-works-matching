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
        publication_date: ISO date string "YYYY-MM-DD" (range key, sortable).
        status: Lifecycle status — DISCOVERED | STARTED | COMPLETED | FAILED.
        file_name: File to download, if applicable.
        download_url: Direct download URL, if applicable.
        file_hash: MD5 checksum for the file, if applicable.
        metadata: Arbitrary extra key/value pairs.
        step_function_execution_arn: ARN of the associated Step Functions execution, if started.
        created_at: ISO datetime string of record creation.
        updated_at: ISO datetime string of last update.
    """

    class Meta:
        """PynamoDB table configuration."""

        table_name = f"dmpworks-{os.environ.get('AWS_ENV', '')}-dataset-releases"
        billing_mode = "PAY_PER_REQUEST"
        region = os.environ.get("AWS_REGION", "")

    dataset = UnicodeAttribute(hash_key=True)
    publication_date = UnicodeAttribute(range_key=True)
    status = UnicodeAttribute(default="DISCOVERED")
    file_name = UnicodeAttribute(null=True)
    download_url = UnicodeAttribute(null=True)
    file_hash = UnicodeAttribute(null=True)
    metadata = MapAttribute(default=dict)
    step_function_execution_arn = UnicodeAttribute(null=True)
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
        metadata: Type-specific context (dataset, publication_date, etc.).
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


def get_latest_known_release(*, dataset: str) -> DatasetReleaseRecord | None:
    """Return the most recently published release record for a dataset.

    Args:
        dataset: The dataset identifier.

    Returns:
        The most recent DatasetReleaseRecord, or None if no records exist.
    """
    return next(
        DatasetReleaseRecord.query(
            dataset,
            scan_index_forward=False,
            limit=1,
        ),
        None,
    )


def persist_discovered_release(*, dataset: str, release: DatasetRelease) -> DatasetReleaseRecord | None:
    """Persist a newly discovered release, deduplicating by (dataset, publication_date).

    Args:
        dataset: The dataset identifier.
        release: The DatasetRelease returned by a detector function.

    Returns:
        The persisted DatasetReleaseRecord, or None if a record for this
        (dataset, publication_date) already exists.
    """
    publication_date_str = release.publication_date.to_date_string()

    try:
        DatasetReleaseRecord.get(dataset, publication_date_str)
    except DoesNotExist:
        pass
    else:
        log.debug(f"Release already exists: dataset={dataset} publication_date={publication_date_str}")
        return None

    now = datetime.now(UTC).isoformat()
    record = DatasetReleaseRecord(
        dataset=dataset,
        publication_date=publication_date_str,
        status="DISCOVERED",
        file_name=release.file_name,
        download_url=release.download_url,
        file_hash=release.file_hash,
        metadata=release.metadata,
        created_at=now,
        updated_at=now,
    )
    record.save()
    log.info(f"Persisted new release: dataset={dataset} publication_date={publication_date_str}")
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

    log.info(f"Detector found release: dataset={dataset} publication_date={release.publication_date}")
    return persist_discovered_release(dataset=dataset, release=release)


def create_task_run(*, run_name: str, run_id: str, execution_arn: str, metadata: dict) -> TaskRunRecord:
    """Create a new TaskRunRecord with STARTED status.

    Args:
        run_name: Job name matching the factory run_name, e.g. "openalex-works-download".
        run_id: Unique execution ID for this stage run.
        execution_arn: ARN of the enclosing Step Functions execution.
        metadata: Type-specific context dict, e.g. {"dataset": ..., "publication_date": ...}.

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


def scan_task_checkpoints(*, workflow_key: str, task_name: str) -> list[TaskCheckpointRecord]:
    """Return all checkpoints for a workflow task across all dates.

    Args:
        workflow_key: Workflow type identifier, e.g. "openalex-works".
        task_name: Task within the workflow, e.g. "download".

    Returns:
        List of all TaskCheckpointRecord instances for the given task.
    """
    return list(
        TaskCheckpointRecord.query(
            workflow_key,
            TaskCheckpointRecord.task_key.startswith(f"{task_name}#"),
        )
    )


class ProcessWorksRunRecord(Model):
    """One record per process-works pipeline run.

    Keyed by (run_date, run_id). Created when all prerequisite dataset checkpoints
    are confirmed ready; updated as the pipeline progresses through SQLMesh and
    OpenSearch sync stages.

    Attributes:
        run_date: ISO date string "YYYY-MM-DD" identifying the monthly run (hash key).
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
        publication_date_openalex_works: Publication date of the OpenAlex Works release used.
        publication_date_datacite: Publication date of the DataCite release used.
        publication_date_crossref_metadata: Publication date of the Crossref Metadata release used.
        publication_date_ror: Publication date of the ROR release used.
        publication_date_data_citation_corpus: Publication date of the Data Citation Corpus release used.
        error: Error message if FAILED.
        created_at: ISO datetime string of record creation.
        updated_at: ISO datetime string of last update.
    """

    class Meta:
        """PynamoDB table configuration."""

        table_name = f"dmpworks-{os.environ.get('AWS_ENV', '')}-process-works-runs"
        billing_mode = "PAY_PER_REQUEST"
        region = os.environ.get("AWS_REGION", "")

    run_date = UnicodeAttribute(hash_key=True)
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
    publication_date_openalex_works = UnicodeAttribute(null=True)
    publication_date_datacite = UnicodeAttribute(null=True)
    publication_date_crossref_metadata = UnicodeAttribute(null=True)
    publication_date_ror = UnicodeAttribute(null=True)
    publication_date_data_citation_corpus = UnicodeAttribute(null=True)
    error = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute()


def update_release_status(
    *,
    dataset: str,
    publication_date: str,
    status: Literal["DISCOVERED", "STARTED", "COMPLETED", "FAILED"],
    **kwargs,
) -> None:
    """Update the status and timestamp of a DatasetReleaseRecord.

    Args:
        dataset: The dataset identifier.
        publication_date: ISO date string "YYYY-MM-DD".
        status: Target lifecycle status — DISCOVERED | STARTED | COMPLETED | FAILED.
        **kwargs: Additional DatasetReleaseRecord attribute names and values to set,
            e.g. step_function_execution_arn="arn:...".
    """
    record = DatasetReleaseRecord.get(dataset, publication_date)
    actions = [
        DatasetReleaseRecord.status.set(status),
        DatasetReleaseRecord.updated_at.set(datetime.now(UTC).isoformat()),
    ]
    for key, value in kwargs.items():
        actions.append(getattr(DatasetReleaseRecord, key).set(value))

    record.update(actions=actions)
    log.info(f"Marked release {status}: dataset={dataset} publication_date={publication_date}")


def create_process_works_run(
    *,
    run_date: str,
    run_id: str,
    execution_arn: str,
    run_id_sqlmesh_prev: str,
    run_id_openalex_works: str,
    run_id_datacite: str,
    run_id_crossref_metadata: str,
    run_id_ror: str,
    run_id_data_citation_corpus: str,
    publication_date_openalex_works: str,
    publication_date_datacite: str,
    publication_date_crossref_metadata: str,
    publication_date_ror: str,
    publication_date_data_citation_corpus: str,
) -> ProcessWorksRunRecord:
    """Create a new ProcessWorksRunRecord with STARTED status.

    Args:
        run_date: ISO date string "YYYY-MM-DD" identifying the monthly run.
        run_id: Unique execution ID for this run.
        execution_arn: ARN of the enclosing Step Functions execution.
        run_id_sqlmesh_prev: Run ID of the prior SQLMesh execution.
        run_id_openalex_works: Run ID of the OpenAlex Works transform checkpoint.
        run_id_datacite: Run ID of the DataCite transform checkpoint.
        run_id_crossref_metadata: Run ID of the Crossref Metadata transform checkpoint.
        run_id_ror: Run ID of the ROR download checkpoint.
        run_id_data_citation_corpus: Run ID of the Data Citation Corpus download checkpoint.
        publication_date_openalex_works: Publication date of the OpenAlex Works release used.
        publication_date_datacite: Publication date of the DataCite release used.
        publication_date_crossref_metadata: Publication date of the Crossref Metadata release used.
        publication_date_ror: Publication date of the ROR release used.
        publication_date_data_citation_corpus: Publication date of the Data Citation Corpus release used.

    Returns:
        The persisted ProcessWorksRunRecord.
    """
    now = datetime.now(UTC).isoformat()
    record = ProcessWorksRunRecord(
        run_date=run_date,
        run_id=run_id,
        status="STARTED",
        step_function_execution_arn=execution_arn,
        run_id_sqlmesh_prev=run_id_sqlmesh_prev,
        run_id_openalex_works=run_id_openalex_works,
        run_id_datacite=run_id_datacite,
        run_id_crossref_metadata=run_id_crossref_metadata,
        run_id_ror=run_id_ror,
        run_id_data_citation_corpus=run_id_data_citation_corpus,
        publication_date_openalex_works=publication_date_openalex_works,
        publication_date_datacite=publication_date_datacite,
        publication_date_crossref_metadata=publication_date_crossref_metadata,
        publication_date_ror=publication_date_ror,
        publication_date_data_citation_corpus=publication_date_data_citation_corpus,
        created_at=now,
        updated_at=now,
    )
    record.save()
    log.info(f"Created process works run: run_date={run_date} run_id={run_id}")
    return record


def get_latest_process_works_run(*, run_date: str) -> ProcessWorksRunRecord | None:
    """Return the most recent ProcessWorksRunRecord for a given run_date.

    Args:
        run_date: ISO date string "YYYY-MM-DD" identifying the monthly run.

    Returns:
        The most recent ProcessWorksRunRecord, or None if no records exist.
    """
    return next(
        ProcessWorksRunRecord.query(
            run_date,
            scan_index_forward=False,
            limit=1,
        ),
        None,
    )


def set_process_works_run_status(
    *,
    run_date: str,
    run_id: str,
    status: Literal["STARTED", "COMPLETED", "FAILED"],
    **kwargs,
) -> None:
    """Update the status and timestamp of a ProcessWorksRunRecord.

    Args:
        run_date: Hash key identifying the monthly run date.
        run_id: Range key identifying the specific run.
        status: Target lifecycle status — STARTED | COMPLETED | FAILED.
        **kwargs: Additional ProcessWorksRunRecord attribute names and values to set,
            e.g. step_function_execution_arn="arn:...", run_id_sqlmesh="20250101T...", or error="...".
    """
    record = ProcessWorksRunRecord.get(run_date, run_id)
    actions = [
        ProcessWorksRunRecord.status.set(status),
        ProcessWorksRunRecord.updated_at.set(datetime.now(UTC).isoformat()),
    ]
    for key, value in kwargs.items():
        actions.append(getattr(ProcessWorksRunRecord, key).set(value))

    record.update(actions=actions)
    log.info(f"Set process works run status: run_date={run_date} run_id={run_id} status={status}")


class ProcessDMPsRunRecord(Model):
    """One record per process-dmps pipeline run. Keyed by (run_date, run_id).

    Created when the pipeline starts and updated as each task completes.
    Task run IDs are recorded as each stage finishes for observability.

    Attributes:
        run_date: ISO date string "YYYY-MM-DD" identifying the daily run (hash key).
        run_id: Unique execution ID (range key), e.g. "20250101T060000-abc123".
        status: Lifecycle status — STARTED | COMPLETED | FAILED.
        step_function_execution_arn: ARN of the enclosing Step Functions execution.
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

    run_date = UnicodeAttribute(hash_key=True)
    run_id = UnicodeAttribute(range_key=True)
    status = UnicodeAttribute(default="STARTED")
    step_function_execution_arn = UnicodeAttribute(null=True)
    error = UnicodeAttribute(null=True)
    run_id_sync_dmps = UnicodeAttribute(null=True)
    run_id_enrich_dmps = UnicodeAttribute(null=True)
    run_id_dmp_works_search = UnicodeAttribute(null=True)
    run_id_merge_related_works = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute()


def create_process_dmps_run(
    *,
    run_date: str,
    run_id: str,
    execution_arn: str,
) -> ProcessDMPsRunRecord:
    """Create a new ProcessDMPsRunRecord with STARTED status.

    Args:
        run_date: ISO date string "YYYY-MM-DD" identifying the daily run.
        run_id: Unique execution ID for this run.
        execution_arn: ARN of the enclosing Step Functions execution.

    Returns:
        The persisted ProcessDMPsRunRecord.
    """
    now = datetime.now(UTC).isoformat()
    record = ProcessDMPsRunRecord(
        run_date=run_date,
        run_id=run_id,
        status="STARTED",
        step_function_execution_arn=execution_arn,
        created_at=now,
        updated_at=now,
    )
    record.save()
    log.info(f"Created process DMPs run: run_date={run_date} run_id={run_id}")
    return record


def scan_all_process_works_runs() -> list[ProcessWorksRunRecord]:
    """Return all ProcessWorksRunRecord entries across all run dates.

    Returns:
        List of all ProcessWorksRunRecord instances.
    """
    return list(ProcessWorksRunRecord.scan())


def scan_all_process_dmps_runs() -> list[ProcessDMPsRunRecord]:
    """Return all ProcessDMPsRunRecord entries across all run dates.

    Returns:
        List of all ProcessDMPsRunRecord instances.
    """
    return list(ProcessDMPsRunRecord.scan())


def get_latest_process_dmps_run(*, run_date: str) -> ProcessDMPsRunRecord | None:
    """Return the most recent ProcessDMPsRunRecord for a given run_date.

    Args:
        run_date: ISO date string "YYYY-MM-DD" identifying the daily run.

    Returns:
        The most recent ProcessDMPsRunRecord, or None if no records exist.
    """
    return next(
        ProcessDMPsRunRecord.query(
            run_date,
            scan_index_forward=False,
            limit=1,
        ),
        None,
    )


def set_process_dmps_run_status(
    *,
    run_date: str,
    run_id: str,
    status: Literal["STARTED", "COMPLETED", "FAILED"] | None = None,
    error: str | None = None,
    run_id_sync_dmps: str | None = None,
    run_id_enrich_dmps: str | None = None,
    run_id_dmp_works_search: str | None = None,
    run_id_merge_related_works: str | None = None,
) -> None:
    """Update fields on a ProcessDMPsRunRecord.

    Only the provided (non-None) arguments are updated; omitted fields are left unchanged.
    This allows both full status updates and partial task run_id recording in a single function.

    Args:
        run_date: Hash key identifying the daily run date.
        run_id: Range key identifying the specific run.
        status: Target lifecycle status — STARTED | COMPLETED | FAILED. If None, not updated.
        error: Error message for FAILED status. If None, not updated.
        run_id_sync_dmps: Run ID of the sync-dmps task. If None, not updated.
        run_id_enrich_dmps: Run ID of the enrich-dmps task. If None, not updated.
        run_id_dmp_works_search: Run ID of the dmp-works-search task. If None, not updated.
        run_id_merge_related_works: Run ID of the merge-related-works task. If None, not updated.
    """
    record = ProcessDMPsRunRecord.get(run_date, run_id)
    actions = [ProcessDMPsRunRecord.updated_at.set(datetime.now(UTC).isoformat())]
    if status is not None:
        actions.append(ProcessDMPsRunRecord.status.set(status))
    if error is not None:
        actions.append(ProcessDMPsRunRecord.error.set(error))
    if run_id_sync_dmps is not None:
        actions.append(ProcessDMPsRunRecord.run_id_sync_dmps.set(run_id_sync_dmps))
    if run_id_enrich_dmps is not None:
        actions.append(ProcessDMPsRunRecord.run_id_enrich_dmps.set(run_id_enrich_dmps))
    if run_id_dmp_works_search is not None:
        actions.append(ProcessDMPsRunRecord.run_id_dmp_works_search.set(run_id_dmp_works_search))
    if run_id_merge_related_works is not None:
        actions.append(ProcessDMPsRunRecord.run_id_merge_related_works.set(run_id_merge_related_works))
    record.update(actions=actions)
    log.info(f"Set process DMPs run status: run_date={run_date} run_id={run_id} status={status}")
