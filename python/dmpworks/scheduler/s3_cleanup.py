"""Logic for building the monthly S3 cleanup plan from DynamoDB checkpoint records."""

from __future__ import annotations

import logging

from dmpworks.scheduler.dynamodb_store import (
    get_task_checkpoint,
    scan_all_process_dmps_runs,
    scan_all_process_works_runs,
    scan_task_checkpoints,
)

log = logging.getLogger(__name__)

# Maps each dataset ingest task to its S3 prefix type and the ProcessWorksRunRecord
# attribute holding the dataset's publication_date (used for checkpoint lookup).
# Download/subset/transform each have their own TaskCheckpointRecord and their own run_id.
DATASET_TASKS: list[tuple[str, str, str, str]] = [
    # (workflow_key, task_name, s3_prefix_type, publication_date_attr)
    ("crossref-metadata", "download", "crossref-metadata-download", "publication_date_crossref_metadata"),
    ("crossref-metadata", "subset", "crossref-metadata-subset", "publication_date_crossref_metadata"),
    ("crossref-metadata", "transform", "crossref-metadata-transform", "publication_date_crossref_metadata"),
    ("openalex-works", "download", "openalex-works-download", "publication_date_openalex_works"),
    ("openalex-works", "subset", "openalex-works-subset", "publication_date_openalex_works"),
    ("openalex-works", "transform", "openalex-works-transform", "publication_date_openalex_works"),
    ("datacite", "download", "datacite-download", "publication_date_datacite"),
    ("datacite", "subset", "datacite-subset", "publication_date_datacite"),
    ("datacite", "transform", "datacite-transform", "publication_date_datacite"),
    ("ror", "download", "ror-download", "publication_date_ror"),
    ("data-citation-corpus", "download", "data-citation-corpus-download", "publication_date_data_citation_corpus"),
]

# SQLMesh checkpoint date matches the process-works run_date directly.
SQLMESH_TASK: tuple[str, str, str] = ("process-works", "sqlmesh", "sqlmesh")


def _collect_protected_run_ids(*, records, dataset_tasks, sqlmesh_task):
    """Build the set of run_ids that must not be deleted.

    Looks up checkpoints for each dataset task using the record's publication_date_*
    fields, and for sqlmesh using the record's run_date.

    Args:
        records: ProcessWorksRunRecord instances to protect.
        dataset_tasks: The DATASET_TASKS list.
        sqlmesh_task: The SQLMESH_TASK tuple.

    Returns:
        Set of protected run_id strings.
    """
    protected: set[str] = set()
    for record in records:
        for wk, tn, _, pub_date_attr in dataset_tasks:
            pub_date = getattr(record, pub_date_attr, None)
            if pub_date is not None:
                cp = get_task_checkpoint(workflow_key=wk, task_name=tn, date=pub_date)
                if cp:
                    protected.add(cp.run_id)
        cp = get_task_checkpoint(workflow_key=sqlmesh_task[0], task_name=sqlmesh_task[1], date=record.run_date)
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
    dataset's publication_date, not the process-works run_date. The publication dates
    stored on ProcessWorksRunRecord are used for correct checkpoint lookup.

    For dmp-works-search, all entries predating the latest completed process-works
    run_date are included — those results were generated against stale works data.

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
        key=lambda r: (r.run_date, r.run_id),
        reverse=True,
    )

    if not completed_works:
        return []

    keep_record = completed_works[0]
    started_works = [r for r in all_works if r.status == "STARTED"]

    protected_run_ids = _collect_protected_run_ids(
        records=[keep_record, *started_works],
        dataset_tasks=DATASET_TASKS,
        sqlmesh_task=SQLMESH_TASK,
    )
    log.info(f"Process-works keep date: {keep_record.run_date} ({len(protected_run_ids)} run IDs to protect)")

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
    keep_date = keep_record.run_date
    all_dmps = scan_all_process_dmps_runs()
    stale_dmps = [r for r in all_dmps if r.status == "COMPLETED" and r.run_date < keep_date]
    for record in stale_dmps:
        cp = get_task_checkpoint(workflow_key="process-dmps", task_name="dmp-works-search", date=record.run_date)
        if cp:
            stale_prefixes.append({"prefix_type": "dmp-works-search", "run_id": cp.run_id, "bucket_name": bucket_name})
    log.info(f"dmp-works-search stale entries (before {keep_date}): {len(stale_dmps)}")

    return stale_prefixes
