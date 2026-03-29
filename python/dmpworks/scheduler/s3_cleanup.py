"""Logic for building the monthly S3 cleanup plan from DynamoDB checkpoint records."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging

from dmpworks.batch_submit.job_registry import (
    CROSSREF_METADATA_DOWNLOAD,
    CROSSREF_METADATA_SUBSET,
    CROSSREF_METADATA_TRANSFORM,
    DATA_CITATION_CORPUS_DOWNLOAD,
    DATACITE_DOWNLOAD,
    DATACITE_SUBSET,
    DATACITE_TRANSFORM,
    OPENALEX_WORKS_DOWNLOAD,
    OPENALEX_WORKS_SUBSET,
    OPENALEX_WORKS_TRANSFORM,
    PROCESS_DMPS_DMP_WORKS_SEARCH,
    PROCESS_WORKS_SQLMESH,
    ROR_DOWNLOAD,
)
from dmpworks.scheduler.dynamodb_store import (
    get_task_checkpoint,
    scan_all_process_dmps_runs,
    scan_all_process_works_runs,
    scan_task_checkpoints,
    scan_task_runs_by_run_name,
)

log = logging.getLogger(__name__)

# Maps each dataset ingest task to its S3 prefix type and the ProcessWorksRunRecord
# attribute holding the dataset's release_date (used for checkpoint lookup).
# Download/subset/transform each have their own TaskCheckpointRecord and their own run_id.
DATASET_TASKS: list[tuple[str, str, str, str]] = [
    # (workflow_key, task_name, run_name, release_date_attr)
    ("crossref-metadata", "download", CROSSREF_METADATA_DOWNLOAD, "release_date_crossref_metadata"),
    ("crossref-metadata", "subset", CROSSREF_METADATA_SUBSET, "release_date_crossref_metadata"),
    ("crossref-metadata", "transform", CROSSREF_METADATA_TRANSFORM, "release_date_crossref_metadata"),
    ("openalex-works", "download", OPENALEX_WORKS_DOWNLOAD, "release_date_openalex_works"),
    ("openalex-works", "subset", OPENALEX_WORKS_SUBSET, "release_date_openalex_works"),
    ("openalex-works", "transform", OPENALEX_WORKS_TRANSFORM, "release_date_openalex_works"),
    ("datacite", "download", DATACITE_DOWNLOAD, "release_date_datacite"),
    ("datacite", "subset", DATACITE_SUBSET, "release_date_datacite"),
    ("datacite", "transform", DATACITE_TRANSFORM, "release_date_datacite"),
    ("ror", "download", ROR_DOWNLOAD, "release_date_ror"),
    ("data-citation-corpus", "download", DATA_CITATION_CORPUS_DOWNLOAD, "release_date_data_citation_corpus"),
]

# SQLMesh checkpoint date matches the process-works release_date directly.
SQLMESH_TASK: tuple[str, str, str] = ("process-works", "sqlmesh", PROCESS_WORKS_SQLMESH)

# All run_names that produce S3 data — used for TaskRunRecord-based cleanup.
S3_RUN_NAMES: list[str] = [run_name for _, _, run_name, _ in DATASET_TASKS] + [
    PROCESS_WORKS_SQLMESH,
    PROCESS_DMPS_DMP_WORKS_SEARCH,
]

# STARTED TaskRunRecords older than this are considered zombies and eligible for cleanup.
ZOMBIE_THRESHOLD_DAYS = 14


def _collect_protected_run_ids(*, records):
    """Build the set of run_ids that must not be deleted.

    Looks up checkpoints for each dataset task using the record's release_date_*
    fields, and for sqlmesh using the record's release_date.

    Args:
        records: ProcessWorksRunRecord instances to protect.

    Returns:
        Set of protected run_id strings.
    """
    protected: set[str] = set()
    for record in records:
        for wk, tn, _, release_date_attr in DATASET_TASKS:
            rel_date = getattr(record, release_date_attr, None)
            if rel_date is not None:
                cp = get_task_checkpoint(workflow_key=wk, task_name=tn, date=rel_date)
                if cp:
                    protected.add(cp.run_id)
        cp = get_task_checkpoint(workflow_key=SQLMESH_TASK[0], task_name=SQLMESH_TASK[1], date=record.release_date)
        if cp:
            protected.add(cp.run_id)
    return protected


def build_cleanup_plan(*, bucket_name: str) -> list[dict[str, str]]:
    """Return stale S3 prefixes to schedule for expiry.

    Scans ProcessWorksRunRecord to find the latest completed run, then scans all
    task checkpoints to identify run_ids that are no longer in use. This catches
    both data from older process-works runs and orphaned dataset releases that were
    superseded before any process-works run consumed them.

    For dataset ingest tasks (download/subset/transform), checkpoint dates are the
    dataset's release_date. The release dates stored on ProcessWorksRunRecord are
    used for correct checkpoint lookup.

    For dmp-works-search, all entries predating the latest completed process-works
    release_date are included — those results were generated against stale works data.

    Args:
        bucket_name: The S3 bucket containing the run data.

    Returns:
        List of dicts with keys prefix_type, run_id, bucket_name.
    """
    stale_prefixes: list[dict[str, str]] = []

    # --- Process-works tasks (download/subset/transform + sqlmesh) ---
    all_works = scan_all_process_works_runs()
    completed_works = sorted(
        [r for r in all_works if r.status == "COMPLETED"],
        key=lambda r: (r.release_date, r.run_id),
        reverse=True,
    )

    if not completed_works:
        return []

    keep_record = completed_works[0]
    started_works = [r for r in all_works if r.status == "STARTED"]

    protected_run_ids = _collect_protected_run_ids(records=[keep_record, *started_works])
    log.info(f"Process-works keep date: {keep_record.release_date} ({len(protected_run_ids)} run IDs to protect)")

    for wk, tn, prefix_type, _ in DATASET_TASKS:
        stale_prefixes.extend(
            {"prefix_type": prefix_type, "run_id": cp.run_id, "bucket_name": bucket_name}
            for cp in scan_task_checkpoints(workflow_key=wk, task_name=tn)
            if cp.run_id not in protected_run_ids
        )

    stale_prefixes.extend(
        {"prefix_type": SQLMESH_TASK[2], "run_id": cp.run_id, "bucket_name": bucket_name}
        for cp in scan_task_checkpoints(workflow_key=SQLMESH_TASK[0], task_name=SQLMESH_TASK[1])
        if cp.run_id not in protected_run_ids
    )

    # --- dmp-works-search: keep all results from the current process-works cycle ---
    keep_date = keep_record.release_date
    all_dmps = scan_all_process_dmps_runs()
    stale_dmps = [r for r in all_dmps if r.status == "COMPLETED" and r.release_date < keep_date]
    for record in stale_dmps:
        cp = get_task_checkpoint(workflow_key="process-dmps", task_name="dmp-works-search", date=record.release_date)
        if cp:
            stale_prefixes.append(
                {"prefix_type": PROCESS_DMPS_DMP_WORKS_SEARCH, "run_id": cp.run_id, "bucket_name": bucket_name}
            )
    log.info(f"dmp-works-search stale entries (before {keep_date}): {len(stale_dmps)}")

    # --- TaskRunRecord scan: catch FAILED and zombie-STARTED runs ---
    already_scheduled = {(e["prefix_type"], e["run_id"]) for e in stale_prefixes}
    zombie_cutoff = (datetime.now(tz=UTC) - timedelta(days=ZOMBIE_THRESHOLD_DAYS)).isoformat()
    task_run_count = 0

    for run_name in S3_RUN_NAMES:
        for record in scan_task_runs_by_run_name(run_name=run_name):
            if record.run_id in protected_run_ids:
                continue
            if (run_name, record.run_id) in already_scheduled:
                continue
            is_failed = record.status == "FAILED"
            is_zombie = record.status == "STARTED" and record.created_at < zombie_cutoff
            if is_failed or is_zombie:
                stale_prefixes.append({"prefix_type": run_name, "run_id": record.run_id, "bucket_name": bucket_name})
                task_run_count += 1

    log.info(f"TaskRunRecord scan: {task_run_count} additional stale prefixes (FAILED + zombie STARTED)")

    return stale_prefixes
