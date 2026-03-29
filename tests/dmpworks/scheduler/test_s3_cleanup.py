"""Tests for build_cleanup_plan."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from dmpworks.scheduler.s3_cleanup import (
    DATASET_TASKS,
    S3_RUN_NAMES,
    SQLMESH_TASK,
    ZOMBIE_THRESHOLD_DAYS,
    build_cleanup_plan,
)

PATCH_BASE = "dmpworks.scheduler.s3_cleanup"
BUCKET = "my-bucket"


@pytest.fixture(autouse=True)
def _no_task_run_scan(monkeypatch):
    """Patch scan_task_runs_by_run_name to return [] by default for all tests."""
    monkeypatch.setattr(f"{PATCH_BASE}.scan_task_runs_by_run_name", lambda *, run_name: [])


# Default release dates for test records.
KEEP_RELEASE_DATE = "2025-02-01"
STALE_RELEASE_DATE = "2025-01-01"


def make_works_run(
    release_date: str,
    run_id: str,
    status: str = "COMPLETED",
    *,
    release_date_openalex_works: str | None = None,
    release_date_datacite: str | None = None,
    release_date_crossref_metadata: str | None = None,
    release_date_ror: str | None = None,
    release_date_data_citation_corpus: str | None = None,
) -> MagicMock:
    """Build a minimal mock ProcessWorksRunRecord."""
    r = MagicMock()
    r.release_date = release_date
    r.run_id = run_id
    r.status = status
    r.release_date_openalex_works = release_date_openalex_works
    r.release_date_datacite = release_date_datacite
    r.release_date_crossref_metadata = release_date_crossref_metadata
    r.release_date_ror = release_date_ror
    r.release_date_data_citation_corpus = release_date_data_citation_corpus
    return r


def make_dmps_run(release_date: str, run_id: str, status: str = "COMPLETED") -> MagicMock:
    """Build a minimal mock ProcessDMPsRunRecord."""
    r = MagicMock()
    r.release_date = release_date
    r.run_id = run_id
    r.status = status
    return r


def make_checkpoint(run_id: str) -> MagicMock:
    """Build a minimal mock TaskCheckpointRecord."""
    cp = MagicMock()
    cp.run_id = run_id
    return cp


def make_task_run(run_id: str, status: str, *, created_at: str | None = None) -> MagicMock:
    """Build a minimal mock TaskRunRecord."""
    r = MagicMock()
    r.run_id = run_id
    r.status = status
    r.created_at = created_at or datetime.now(tz=UTC).isoformat()
    return r


def run_id_for(workflow_key: str, task_name: str, date: str) -> str:
    """Return a deterministic run_id for a given task + date combination."""
    return f"{workflow_key}-{task_name}-{date}"


def default_release_dates(release_date: str) -> dict:
    """Return kwargs to set all release_date_* fields to the same date."""
    return {
        "release_date_openalex_works": release_date,
        "release_date_datacite": release_date,
        "release_date_crossref_metadata": release_date,
        "release_date_ror": release_date,
        "release_date_data_citation_corpus": release_date,
    }


class TestNoCompletedWorksRecords:
    def test_returns_empty_plan(self):
        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", return_value=None),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        assert result == []

    def test_single_completed_record_no_stale_checkpoints(self):
        keep = make_works_run("2025-01-13", "run-1", **default_release_dates(KEEP_RELEASE_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        assert result == []


class TestDatasetTasksCleanup:
    def test_stale_checkpoints_included_in_plan(self):
        keep = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        stale = make_works_run("2025-01-13", "run-1", **default_release_dates(STALE_RELEASE_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_RELEASE_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, STALE_RELEASE_DATE)),
            ]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, stale]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        # Each dataset task for the stale date should appear, plus sqlmesh
        prefix_types = [item["prefix_type"] for item in result]
        for _, _, prefix_type, _ in DATASET_TASKS:
            assert prefix_type in prefix_types
        assert SQLMESH_TASK[2] in prefix_types

        for item in result:
            assert item["bucket_name"] == BUCKET

    def test_stale_run_ids_use_release_dates_not_run_date(self):
        """Dataset tasks must use release_date for checkpoint lookup, not run_date."""
        keep = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        stale = make_works_run("2025-01-13", "run-1", **default_release_dates(STALE_RELEASE_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == SQLMESH_TASK[0] and task_name == SQLMESH_TASK[1]:
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-01-13")),
                ]
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_RELEASE_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, STALE_RELEASE_DATE)),
            ]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, stale]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        run_ids = {item["run_id"] for item in result}
        # Dataset tasks should be looked up at STALE_RELEASE_DATE, not release_date "2025-01-13"
        for wk, tn, _, _ in DATASET_TASKS:
            assert run_id_for(wk, tn, STALE_RELEASE_DATE) in run_ids
        # SQLMesh should use release_date "2025-01-13"
        assert run_id_for("process-works", "sqlmesh", "2025-01-13") in run_ids

    def test_keep_run_ids_never_deleted(self):
        keep = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        stale = make_works_run("2025-01-13", "run-1", **default_release_dates(STALE_RELEASE_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == SQLMESH_TASK[0] and task_name == SQLMESH_TASK[1]:
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-01-13")),
                ]
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_RELEASE_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, STALE_RELEASE_DATE)),
            ]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, stale]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        run_ids = {item["run_id"] for item in result}
        for wk, tn, _, _ in DATASET_TASKS:
            assert run_id_for(wk, tn, KEEP_RELEASE_DATE) not in run_ids
        assert run_id_for("process-works", "sqlmesh", "2025-02-10") not in run_ids

    def test_missing_checkpoint_skipped(self):
        """A task with no checkpoint for the keep date is silently skipped."""
        keep = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        stale = make_works_run("2025-01-13", "run-1", **default_release_dates(STALE_RELEASE_DATE))

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, stale]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", return_value=None),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        assert result == []


class TestStartedAndFailedRecordsProtected:
    @pytest.mark.parametrize("status", ["STARTED", "FAILED"])
    def test_non_completed_records_not_in_delete_list(self, status):
        active_record = make_works_run("2025-02-10", "run-2", status=status, **default_release_dates(KEEP_RELEASE_DATE))
        completed_record = make_works_run("2025-01-13", "run-1", **default_release_dates(STALE_RELEASE_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[active_record, completed_record]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        # The only completed record is the "keep" — no stale checkpoints
        assert result == []

    def test_started_record_run_ids_protected(self):
        """Data used by a STARTED record must not be deleted, even if not the keep record."""
        keep = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        started = make_works_run("2025-03-10", "run-3", status="STARTED", **default_release_dates("2025-03-01"))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        started_release_date = "2025-03-01"

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == SQLMESH_TASK[0] and task_name == SQLMESH_TASK[1]:
                # SQLMesh uses release_date, not per-dataset release dates
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-03-10")),
                ]
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_RELEASE_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, started_release_date)),
            ]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, started]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        # Both keep and started run_ids are protected — nothing should be deleted
        assert result == []


class TestDmpWorksSearchCleanup:
    def test_dmp_works_search_before_keep_date_included(self):
        keep_works = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        stale_dmps = make_dmps_run("2025-02-08", "dmps-run-old")

        def fake_checkpoint(*, workflow_key, task_name, date):
            if workflow_key == "process-dmps":
                return make_checkpoint(f"dmp-search-{date}")
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep_works]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[stale_dmps]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        dmp_items = [item for item in result if item["prefix_type"] == "process-dmps-dmp-works-search"]
        assert len(dmp_items) == 1
        assert dmp_items[0]["run_id"] == "dmp-search-2025-02-08"

    def test_dmp_works_search_on_or_after_keep_date_not_deleted(self):
        keep_works = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        recent_dmps = make_dmps_run("2025-02-10", "dmps-run-new")
        future_dmps = make_dmps_run("2025-02-15", "dmps-run-newer")

        def fake_checkpoint(*, workflow_key, task_name, date):
            if workflow_key == "process-dmps":
                return make_checkpoint(f"dmp-search-{date}")
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep_works]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[recent_dmps, future_dmps]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        dmp_items = [item for item in result if item["prefix_type"] == "process-dmps-dmp-works-search"]
        assert dmp_items == []

    def test_dmp_works_search_no_checkpoint_skipped(self):
        keep_works = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        stale_dmps = make_dmps_run("2025-01-05", "dmps-run-old")

        def fake_checkpoint(*, workflow_key, task_name, date):
            if workflow_key == "process-dmps":
                return None  # no checkpoint recorded
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep_works]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[stale_dmps]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        dmp_items = [item for item in result if item["prefix_type"] == "process-dmps-dmp-works-search"]
        assert dmp_items == []

    def test_started_failed_dmps_not_deleted(self):
        keep_works = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        started_dmps = make_dmps_run("2025-02-01", "dmps-run", status="STARTED")
        failed_dmps = make_dmps_run("2025-01-15", "dmps-run-2", status="FAILED")

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(f"dmp-search-{date}")

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep_works]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[started_dmps, failed_dmps]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        dmp_items = [item for item in result if item["prefix_type"] == "process-dmps-dmp-works-search"]
        assert dmp_items == []


class TestReleaseDateMismatch:
    """Verify cleanup works when release dates differ from run dates."""

    def test_different_release_dates_per_dataset(self):
        """Each dataset can have a different release date."""
        keep = make_works_run(
            "2025-03-10",
            "run-2",
            release_date_openalex_works="2025-03-01",
            release_date_datacite="2025-02-28",
            release_date_crossref_metadata="2025-03-01",
            release_date_ror="2025-02-15",
            release_date_data_citation_corpus="2025-02-20",
        )
        stale = make_works_run(
            "2025-02-10",
            "run-1",
            release_date_openalex_works="2025-02-01",
            release_date_datacite="2025-01-15",
            release_date_crossref_metadata="2025-02-01",
            release_date_ror="2025-01-10",
            release_date_data_citation_corpus="2025-01-05",
        )

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == "process-works" and task_name == "sqlmesh":
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-03-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                ]
            # For each dataset task, return checkpoints at both keep and stale dates
            keep_dates = {
                "openalex-works": "2025-03-01",
                "datacite": "2025-02-28",
                "crossref-metadata": "2025-03-01",
                "ror": "2025-02-15",
                "data-citation-corpus": "2025-02-20",
            }
            stale_dates = {
                "openalex-works": "2025-02-01",
                "datacite": "2025-01-15",
                "crossref-metadata": "2025-02-01",
                "ror": "2025-01-10",
                "data-citation-corpus": "2025-01-05",
            }
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, keep_dates[workflow_key])),
                make_checkpoint(run_id_for(workflow_key, task_name, stale_dates[workflow_key])),
            ]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, stale]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        run_ids = {item["run_id"] for item in result}
        # Stale openalex-works tasks should use stale pub date "2025-02-01"
        assert run_id_for("openalex-works", "download", "2025-02-01") in run_ids
        assert run_id_for("openalex-works", "transform", "2025-02-01") in run_ids
        # Stale datacite tasks should use stale pub date "2025-01-15"
        assert run_id_for("datacite", "download", "2025-01-15") in run_ids
        # Keep dates should NOT appear
        assert run_id_for("openalex-works", "download", "2025-03-01") not in run_ids
        assert run_id_for("datacite", "download", "2025-02-28") not in run_ids

    def test_same_dataset_reused_across_runs_not_deleted(self):
        """If both keep and stale reference the same release_date, run_id is protected."""
        same_release_date = "2025-01-15"
        keep = make_works_run("2025-02-10", "run-2", **default_release_dates(same_release_date))
        stale = make_works_run("2025-01-13", "run-1", **default_release_dates(same_release_date))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == "process-works" and task_name == "sqlmesh":
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-01-13")),
                ]
            # Only one checkpoint per dataset — same date used by both runs
            return [make_checkpoint(run_id_for(workflow_key, task_name, same_release_date))]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, stale]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        # Dataset tasks are protected — only sqlmesh stale entry should appear
        dataset_items = [item for item in result if item["prefix_type"] != "process-works-sqlmesh"]
        assert dataset_items == []
        sqlmesh_items = [item for item in result if item["prefix_type"] == "process-works-sqlmesh"]
        assert len(sqlmesh_items) == 1

    def test_orphaned_release_cleaned_up(self):
        """A checkpoint at a date not referenced by any ProcessWorksRunRecord is still cleaned up."""
        keep = make_works_run("2025-02-10", "run-1", **default_release_dates(KEEP_RELEASE_DATE))
        orphan_date = "2025-01-15"

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            # Return both the keep checkpoint and the orphaned one
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_RELEASE_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, orphan_date)),
            ]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        run_ids = {item["run_id"] for item in result}
        # Orphaned checkpoints should be scheduled for deletion
        for wk, tn, _, _ in DATASET_TASKS:
            assert run_id_for(wk, tn, orphan_date) in run_ids
        # Keep checkpoints should NOT be deleted
        for wk, tn, _, _ in DATASET_TASKS:
            assert run_id_for(wk, tn, KEEP_RELEASE_DATE) not in run_ids

    def test_old_records_without_release_dates_graceful(self):
        """Records created before this fix (with None release_date_*) are handled gracefully."""
        keep = make_works_run("2025-02-10", "run-2", **default_release_dates(KEEP_RELEASE_DATE))
        # Old record without release_date_* — all set to None by default
        old_record = make_works_run("2025-01-13", "run-1")

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == "process-works" and task_name == "sqlmesh":
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-01-13")),
                ]
            return [make_checkpoint(run_id_for(workflow_key, task_name, KEEP_RELEASE_DATE))]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, old_record]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        # Only sqlmesh should be cleaned for the old record
        prefix_types = [item["prefix_type"] for item in result]
        assert "process-works-sqlmesh" in prefix_types
        dataset_prefixes = [p for p in prefix_types if p != "process-works-sqlmesh"]
        assert dataset_prefixes == []


class TestTaskRunRecordCleanup:
    """Tests for FAILED and zombie-STARTED TaskRunRecord cleanup."""

    def _run_with_task_runs(self, task_runs_by_name, *, protected_run_ids=None):
        """Run build_cleanup_plan with a baseline completed works run and custom TaskRunRecords.

        Args:
            task_runs_by_name: Dict mapping run_name -> list of mock TaskRunRecords.
            protected_run_ids: Optional set of run_ids that should be protected.

        Returns:
            List of stale prefix dicts from build_cleanup_plan.
        """
        keep = make_works_run("2025-02-10", "run-keep", **default_release_dates(KEEP_RELEASE_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_runs(*, run_name):
            return task_runs_by_name.get(run_name, [])

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", return_value=[]),
            patch(f"{PATCH_BASE}.scan_task_runs_by_run_name", side_effect=fake_scan_runs),
        ):
            return build_cleanup_plan(bucket_name=BUCKET)

    def test_failed_run_included(self):
        task_runs = {"openalex-works-download": [make_task_run("failed-run", "FAILED")]}
        result = self._run_with_task_runs(task_runs)
        assert {"prefix_type": "openalex-works-download", "run_id": "failed-run", "bucket_name": BUCKET} in result

    def test_zombie_started_run_included(self):
        old_timestamp = (datetime.now(tz=UTC) - timedelta(days=ZOMBIE_THRESHOLD_DAYS + 1)).isoformat()
        task_runs = {"datacite-download": [make_task_run("zombie-run", "STARTED", created_at=old_timestamp)]}
        result = self._run_with_task_runs(task_runs)
        assert {"prefix_type": "datacite-download", "run_id": "zombie-run", "bucket_name": BUCKET} in result

    def test_recent_started_run_not_included(self):
        recent_timestamp = (datetime.now(tz=UTC) - timedelta(days=1)).isoformat()
        task_runs = {"datacite-download": [make_task_run("active-run", "STARTED", created_at=recent_timestamp)]}
        result = self._run_with_task_runs(task_runs)
        run_ids = [item["run_id"] for item in result]
        assert "active-run" not in run_ids

    def test_completed_run_not_duplicated(self):
        task_runs = {"openalex-works-download": [make_task_run("completed-run", "COMPLETED")]}
        result = self._run_with_task_runs(task_runs)
        run_ids = [item["run_id"] for item in result]
        assert "completed-run" not in run_ids

    def test_protected_run_id_not_cleaned(self):
        """FAILED runs with protected run_ids are not cleaned."""
        # The protected run_id comes from the keep record's checkpoint lookup.
        # Use a run_id that matches what fake_checkpoint would return for the keep record.
        protected_id = run_id_for("openalex-works", "download", KEEP_RELEASE_DATE)
        task_runs = {"openalex-works-download": [make_task_run(protected_id, "FAILED")]}
        result = self._run_with_task_runs(task_runs)
        run_ids = [item["run_id"] for item in result]
        assert protected_id not in run_ids
