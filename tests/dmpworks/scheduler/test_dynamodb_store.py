"""Integration tests for dynamodb_store using DynamoDB Local."""

from __future__ import annotations

from dmpworks.model.dataset_version_model import DatasetRelease
from dmpworks.scheduler.dynamodb_store import (
    DatasetReleaseRecord,
    TaskCheckpointRecord,
    TaskRunRecord,
    create_task_run,
    discover_latest_release,
    get_latest_known_release,
    get_task_checkpoint,
    persist_discovered_release,
    set_task_checkpoint,
    set_task_run_status,
    update_release_status,
)
import pendulum
import pytest

from tests.aws_test_env import (
    dynamodb_local,
    make_release_record,
    release_table,
    task_checkpoint_table,
    task_run_table,
)  # noqa: F401


class TestPersistDiscoveredRelease:
    """Tests for persist_discovered_release."""

    def test_new_release_is_persisted(self, release_table):
        """A new release is saved with all normalized fields."""
        release = DatasetRelease(
            publication_date=pendulum.date(2026, 3, 12),
            download_url="https://zenodo.org/api/records/18985120/files/v2.4-2026-03-12-ror-data.zip/content",
            file_name="v2.4-2026-03-12-ror-data.zip",
            file_hash="md5:b04f7419253f96846365a0a36b5041aa",
        )

        persist_discovered_release(dataset="ror", release=release)

        record = DatasetReleaseRecord.get("ror", "2026-03-12")
        assert record.dataset == "ror"
        assert record.publication_date == "2026-03-12"
        assert record.status == "DISCOVERED"
        assert (
            record.download_url == "https://zenodo.org/api/records/18985120/files/v2.4-2026-03-12-ror-data.zip/content"
        )
        assert record.file_name == "v2.4-2026-03-12-ror-data.zip"
        assert record.file_hash == "md5:b04f7419253f96846365a0a36b5041aa"
        assert record.created_at is not None
        assert record.updated_at is not None

    def test_duplicate_is_noop(self, release_table):
        """A duplicate (same dataset + date) returns None without overwriting."""
        release = DatasetRelease(publication_date=pendulum.date(2026, 3, 12))

        persist_discovered_release(dataset="ror", release=release)
        second = persist_discovered_release(dataset="ror", release=release)

        assert second is None
        assert len(list(DatasetReleaseRecord.query("ror"))) == 1


class TestGetLatestKnownRelease:
    """Tests for get_latest_known_release."""

    def test_returns_most_recent_record(self, release_table):
        """Query returns the record with the latest publication_date."""
        for date_str in ("2024-01-01", "2024-06-01", "2024-03-01"):
            persist_discovered_release(
                dataset="crossref",
                release=DatasetRelease(publication_date=pendulum.date(*[int(p) for p in date_str.split("-")])),
            )

        record = get_latest_known_release(dataset="crossref")

        assert record is not None
        assert record.publication_date == "2024-06-01"

    def test_returns_none_when_no_records(self, release_table):
        """Returns None when no records exist for the dataset."""
        record = get_latest_known_release(dataset="nonexistent-dataset")
        assert record is None


class TestDiscoverLatestRelease:
    """Tests for discover_latest_release."""

    def test_detector_returning_release_is_persisted(self, release_table):
        """When detector returns a DatasetRelease it is persisted."""
        release = DatasetRelease(
            publication_date=pendulum.date(2026, 3, 12),
            download_url="https://zenodo.org/api/records/18985120/files/v2.4-2026-03-12-ror-data.zip/content",
            file_name="v2.4-2026-03-12-ror-data.zip",
            file_hash="md5:b04f7419253f96846365a0a36b5041aa",
        )

        def detector() -> DatasetRelease | None:
            return release

        discover_latest_release(
            dataset="ror",
            detector=detector,
            detector_kwargs={},
        )

        record = DatasetReleaseRecord.get("ror", "2026-03-12")
        assert record.dataset == "ror"
        assert record.publication_date == "2026-03-12"
        assert (
            record.download_url == "https://zenodo.org/api/records/18985120/files/v2.4-2026-03-12-ror-data.zip/content"
        )
        assert record.file_name == "v2.4-2026-03-12-ror-data.zip"
        assert record.file_hash == "md5:b04f7419253f96846365a0a36b5041aa"

    def test_detector_returning_none_is_noop(self, release_table):
        """When detector returns None, discover_latest_release returns None."""

        def detector() -> DatasetRelease | None:
            return None

        record = discover_latest_release(
            dataset="openalex",
            detector=detector,
            detector_kwargs={},
        )

        assert record is None

        # Confirm no records were written
        records = list(DatasetReleaseRecord.query("openalex"))
        assert len(records) == 0


class TestTaskRunRecord:
    """Tests for TaskRunRecord CRUD functions."""

    def test_create_task_run(self, task_run_table):
        """create_task_run persists a STARTED record with metadata."""
        record = create_task_run(
            run_name="ror-download",
            run_id="2025-01-01T060000-a1b2c3d4",
            execution_arn="arn:aws:states:us-east-1:123456789012:execution:test:exec-1",
            metadata={"dataset": "ror", "publication_date": "2025-01-01"},
        )

        assert record.run_name == "ror-download"
        assert record.run_id == "2025-01-01T060000-a1b2c3d4"
        assert record.status == "STARTED"
        assert record.step_function_execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test:exec-1"
        assert record.metadata["dataset"] == "ror"
        assert record.metadata["publication_date"] == "2025-01-01"

        fetched = TaskRunRecord.get("ror-download", "2025-01-01T060000-a1b2c3d4")
        assert fetched.status == "STARTED"

    def test_complete_task_run(self, task_run_table):
        """set_task_run_status sets status to COMPLETED."""
        create_task_run(
            run_name="ror-download",
            run_id="2025-01-01T060000-a1b2c3d4",
            execution_arn="arn:aws:states:us-east-1:123456789012:execution:test:exec-1",
            metadata={},
        )

        set_task_run_status(run_name="ror-download", run_id="2025-01-01T060000-a1b2c3d4", status="COMPLETED")

        record = TaskRunRecord.get("ror-download", "2025-01-01T060000-a1b2c3d4")
        assert record.status == "COMPLETED"

    def test_fail_task_run(self, task_run_table):
        """set_task_run_status sets status to FAILED and stores error."""
        create_task_run(
            run_name="ror-download",
            run_id="2025-01-01T060000-a1b2c3d4",
            execution_arn="arn:aws:states:us-east-1:123456789012:execution:test:exec-1",
            metadata={},
        )

        set_task_run_status(
            run_name="ror-download", run_id="2025-01-01T060000-a1b2c3d4", status="FAILED", error="Container exited 1"
        )

        record = TaskRunRecord.get("ror-download", "2025-01-01T060000-a1b2c3d4")
        assert record.status == "FAILED"
        assert record.error == "Container exited 1"

    def test_fail_task_run_without_error(self, task_run_table):
        """set_task_run_status with no error leaves error field unset."""
        create_task_run(
            run_name="ror-download",
            run_id="2025-01-01T060000-a1b2c3d4",
            execution_arn="arn:aws:states:us-east-1:123456789012:execution:test:exec-1",
            metadata={},
        )

        set_task_run_status(run_name="ror-download", run_id="2025-01-01T060000-a1b2c3d4", status="FAILED")

        record = TaskRunRecord.get("ror-download", "2025-01-01T060000-a1b2c3d4")
        assert record.status == "FAILED"
        assert record.error is None


class TestTaskCheckpointRecord:
    """Tests for TaskCheckpointRecord CRUD functions."""

    def test_set_task_checkpoint_writes_record(self, task_checkpoint_table):
        """set_task_checkpoint persists a record with all fields."""
        set_task_checkpoint(
            workflow_key="openalex-works",
            task_name="download",
            date="2025-01-01",
            run_id="20250101T060000-a1b2c3d4",
        )

        record = TaskCheckpointRecord.get("openalex-works", "download#2025-01-01")
        assert record.workflow_key == "openalex-works"
        assert record.task_key == "download#2025-01-01"
        assert record.run_id == "20250101T060000-a1b2c3d4"
        assert record.completed_at is not None

    def test_get_task_checkpoint_returns_none_when_absent(self, task_checkpoint_table):
        """get_task_checkpoint returns None when no checkpoint exists."""
        result = get_task_checkpoint(workflow_key="openalex-works", task_name="download", date="2025-01-01")
        assert result is None

    def test_get_task_checkpoint_returns_record_after_set(self, task_checkpoint_table):
        """get_task_checkpoint returns the record written by set_task_checkpoint."""
        set_task_checkpoint(
            workflow_key="openalex-works",
            task_name="transform",
            date="2025-01-01",
            run_id="20250101T080000-c3d4e5f6",
        )

        result = get_task_checkpoint(workflow_key="openalex-works", task_name="transform", date="2025-01-01")
        assert result is not None
        assert result.run_id == "20250101T080000-c3d4e5f6"

    def test_get_task_checkpoint_without_date_returns_latest(self, task_checkpoint_table):
        """get_task_checkpoint with no date returns the most recently completed record."""
        set_task_checkpoint(
            workflow_key="openalex-works",
            task_name="transform",
            date="2025-01-01",
            run_id="20250101T080000-a1b2c3d4",
        )
        set_task_checkpoint(
            workflow_key="openalex-works",
            task_name="transform",
            date="2025-02-01",
            run_id="20250201T080000-e5f6a7b8",
        )

        result = get_task_checkpoint(workflow_key="openalex-works", task_name="transform")
        assert result is not None
        assert result.run_id == "20250201T080000-e5f6a7b8"

    def test_get_task_checkpoint_without_date_returns_none_when_absent(self, task_checkpoint_table):
        """get_task_checkpoint with no date returns None when no checkpoints exist."""
        result = get_task_checkpoint(workflow_key="openalex-works", task_name="transform")
        assert result is None


class TestUpdateReleaseStatus:
    """Tests for update_release_status."""

    def test_started_sets_status_and_execution_arn(self, release_table):
        """STARTED sets status and stores execution ARN."""
        make_release_record("ror", "2025-01-01")

        update_release_status(
            dataset="ror",
            publication_date="2025-01-01",
            status="STARTED",
            step_function_execution_arn="arn:aws:states:us-east-1:123456789012:execution:test:exec-1",
        )

        record = DatasetReleaseRecord.get("ror", "2025-01-01")
        assert record.status == "STARTED"
        assert record.step_function_execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test:exec-1"

    def test_completed_sets_status(self, release_table):
        """COMPLETED sets status=COMPLETED."""
        make_release_record("ror", "2025-01-01")

        update_release_status(dataset="ror", publication_date="2025-01-01", status="COMPLETED")

        record = DatasetReleaseRecord.get("ror", "2025-01-01")
        assert record.status == "COMPLETED"

    def test_failed_sets_status(self, release_table):
        """FAILED sets status=FAILED."""
        make_release_record("ror", "2025-01-01")

        update_release_status(dataset="ror", publication_date="2025-01-01", status="FAILED")

        record = DatasetReleaseRecord.get("ror", "2025-01-01")
        assert record.status == "FAILED"
