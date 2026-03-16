"""Tests for build_cleanup_plan."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dmpworks.scheduler.s3_cleanup import DATASET_TASKS, SQLMESH_TASK, build_cleanup_plan

PATCH_BASE = "dmpworks.scheduler.s3_cleanup"
BUCKET = "my-bucket"

# Default publication dates for test records.
KEEP_PUB_DATE = "2025-02-01"
STALE_PUB_DATE = "2025-01-01"


def make_works_run(
    run_date: str,
    run_id: str,
    status: str = "COMPLETED",
    *,
    publication_date_openalex_works: str | None = None,
    publication_date_datacite: str | None = None,
    publication_date_crossref_metadata: str | None = None,
    publication_date_ror: str | None = None,
    publication_date_data_citation_corpus: str | None = None,
) -> MagicMock:
    """Build a minimal mock ProcessWorksRunRecord."""
    r = MagicMock()
    r.run_date = run_date
    r.run_id = run_id
    r.status = status
    r.publication_date_openalex_works = publication_date_openalex_works
    r.publication_date_datacite = publication_date_datacite
    r.publication_date_crossref_metadata = publication_date_crossref_metadata
    r.publication_date_ror = publication_date_ror
    r.publication_date_data_citation_corpus = publication_date_data_citation_corpus
    return r


def make_dmps_run(run_date: str, run_id: str, status: str = "COMPLETED") -> MagicMock:
    """Build a minimal mock ProcessDMPsRunRecord."""
    r = MagicMock()
    r.run_date = run_date
    r.run_id = run_id
    r.status = status
    return r


def make_checkpoint(run_id: str) -> MagicMock:
    """Build a minimal mock TaskCheckpointRecord."""
    cp = MagicMock()
    cp.run_id = run_id
    return cp


def run_id_for(workflow_key: str, task_name: str, run_date: str) -> str:
    """Return a deterministic run_id for a given task + date combination."""
    return f"{workflow_key}-{task_name}-{run_date}"


def default_pub_dates(pub_date: str) -> dict:
    """Return kwargs to set all publication_date_* fields to the same date."""
    return {
        "publication_date_openalex_works": pub_date,
        "publication_date_datacite": pub_date,
        "publication_date_crossref_metadata": pub_date,
        "publication_date_ror": pub_date,
        "publication_date_data_citation_corpus": pub_date,
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
        keep = make_works_run("2025-01-13", "run-1", **default_pub_dates(KEEP_PUB_DATE))

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
        keep = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
        stale = make_works_run("2025-01-13", "run-1", **default_pub_dates(STALE_PUB_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_PUB_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, STALE_PUB_DATE)),
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

    def test_stale_run_ids_use_publication_dates_not_run_date(self):
        """Dataset tasks must use publication_date for checkpoint lookup, not run_date."""
        keep = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
        stale = make_works_run("2025-01-13", "run-1", **default_pub_dates(STALE_PUB_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == SQLMESH_TASK[0] and task_name == SQLMESH_TASK[1]:
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-01-13")),
                ]
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_PUB_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, STALE_PUB_DATE)),
            ]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, stale]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        run_ids = {item["run_id"] for item in result}
        # Dataset tasks should be looked up at STALE_PUB_DATE, not run_date "2025-01-13"
        for wk, tn, _, _ in DATASET_TASKS:
            assert run_id_for(wk, tn, STALE_PUB_DATE) in run_ids
        # SQLMesh should use run_date "2025-01-13"
        assert run_id_for("process-works", "sqlmesh", "2025-01-13") in run_ids

    def test_keep_run_ids_never_deleted(self):
        keep = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
        stale = make_works_run("2025-01-13", "run-1", **default_pub_dates(STALE_PUB_DATE))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == SQLMESH_TASK[0] and task_name == SQLMESH_TASK[1]:
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-01-13")),
                ]
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_PUB_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, STALE_PUB_DATE)),
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
            assert run_id_for(wk, tn, KEEP_PUB_DATE) not in run_ids
        assert run_id_for("process-works", "sqlmesh", "2025-02-10") not in run_ids

    def test_missing_checkpoint_skipped(self):
        """A task with no checkpoint for the keep date is silently skipped."""
        keep = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
        stale = make_works_run("2025-01-13", "run-1", **default_pub_dates(STALE_PUB_DATE))

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
        active_record = make_works_run("2025-02-10", "run-2", status=status, **default_pub_dates(KEEP_PUB_DATE))
        completed_record = make_works_run("2025-01-13", "run-1", **default_pub_dates(STALE_PUB_DATE))

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
        keep = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
        started = make_works_run("2025-03-10", "run-3", status="STARTED", **default_pub_dates("2025-03-01"))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        started_pub_date = "2025-03-01"

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == SQLMESH_TASK[0] and task_name == SQLMESH_TASK[1]:
                # SQLMesh uses run_date, not publication_date
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-03-10")),
                ]
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_PUB_DATE)),
                make_checkpoint(run_id_for(workflow_key, task_name, started_pub_date)),
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
        keep_works = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
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

        dmp_items = [item for item in result if item["prefix_type"] == "dmp-works-search"]
        assert len(dmp_items) == 1
        assert dmp_items[0]["run_id"] == "dmp-search-2025-02-08"

    def test_dmp_works_search_on_or_after_keep_date_not_deleted(self):
        keep_works = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
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

        dmp_items = [item for item in result if item["prefix_type"] == "dmp-works-search"]
        assert dmp_items == []

    def test_dmp_works_search_no_checkpoint_skipped(self):
        keep_works = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
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

        dmp_items = [item for item in result if item["prefix_type"] == "dmp-works-search"]
        assert dmp_items == []

    def test_started_failed_dmps_not_deleted(self):
        keep_works = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
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

        dmp_items = [item for item in result if item["prefix_type"] == "dmp-works-search"]
        assert dmp_items == []


class TestPublicationDateMismatch:
    """Verify cleanup works when publication dates differ from run dates."""

    def test_different_publication_dates_per_dataset(self):
        """Each dataset can have a different publication date."""
        keep = make_works_run(
            "2025-03-10",
            "run-2",
            publication_date_openalex_works="2025-03-01",
            publication_date_datacite="2025-02-28",
            publication_date_crossref_metadata="2025-03-01",
            publication_date_ror="2025-02-15",
            publication_date_data_citation_corpus="2025-02-20",
        )
        stale = make_works_run(
            "2025-02-10",
            "run-1",
            publication_date_openalex_works="2025-02-01",
            publication_date_datacite="2025-01-15",
            publication_date_crossref_metadata="2025-02-01",
            publication_date_ror="2025-01-10",
            publication_date_data_citation_corpus="2025-01-05",
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
        """If both keep and stale reference the same publication_date, run_id is protected."""
        same_pub_date = "2025-01-15"
        keep = make_works_run("2025-02-10", "run-2", **default_pub_dates(same_pub_date))
        stale = make_works_run("2025-01-13", "run-1", **default_pub_dates(same_pub_date))

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == "process-works" and task_name == "sqlmesh":
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-01-13")),
                ]
            # Only one checkpoint per dataset — same date used by both runs
            return [make_checkpoint(run_id_for(workflow_key, task_name, same_pub_date))]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, stale]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        # Dataset tasks are protected — only sqlmesh stale entry should appear
        dataset_items = [item for item in result if item["prefix_type"] != "sqlmesh"]
        assert dataset_items == []
        sqlmesh_items = [item for item in result if item["prefix_type"] == "sqlmesh"]
        assert len(sqlmesh_items) == 1

    def test_orphaned_release_cleaned_up(self):
        """A checkpoint at a date not referenced by any ProcessWorksRunRecord is still cleaned up."""
        keep = make_works_run("2025-02-10", "run-1", **default_pub_dates(KEEP_PUB_DATE))
        orphan_date = "2025-01-15"

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            # Return both the keep checkpoint and the orphaned one
            return [
                make_checkpoint(run_id_for(workflow_key, task_name, KEEP_PUB_DATE)),
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
            assert run_id_for(wk, tn, KEEP_PUB_DATE) not in run_ids

    def test_old_records_without_publication_dates_graceful(self):
        """Records created before this fix (with None publication_date_*) are handled gracefully."""
        keep = make_works_run("2025-02-10", "run-2", **default_pub_dates(KEEP_PUB_DATE))
        # Old record without publication_date_* — all set to None by default
        old_record = make_works_run("2025-01-13", "run-1")

        def fake_checkpoint(*, workflow_key, task_name, date):
            return make_checkpoint(run_id_for(workflow_key, task_name, date))

        def fake_scan_checkpoints(*, workflow_key, task_name):
            if workflow_key == "process-works" and task_name == "sqlmesh":
                return [
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-02-10")),
                    make_checkpoint(run_id_for(workflow_key, task_name, "2025-01-13")),
                ]
            return [make_checkpoint(run_id_for(workflow_key, task_name, KEEP_PUB_DATE))]

        with (
            patch(f"{PATCH_BASE}.scan_all_process_works_runs", return_value=[keep, old_record]),
            patch(f"{PATCH_BASE}.scan_all_process_dmps_runs", return_value=[]),
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=fake_checkpoint),
            patch(f"{PATCH_BASE}.scan_task_checkpoints", side_effect=fake_scan_checkpoints),
        ):
            result = build_cleanup_plan(bucket_name=BUCKET)

        # Only sqlmesh should be cleaned for the old record
        prefix_types = [item["prefix_type"] for item in result]
        assert "sqlmesh" in prefix_types
        dataset_prefixes = [p for p in prefix_types if p != "sqlmesh"]
        assert dataset_prefixes == []
