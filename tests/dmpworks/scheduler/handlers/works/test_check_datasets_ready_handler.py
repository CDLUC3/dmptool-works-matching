"""Unit tests for check_datasets_ready_handler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dmpworks.constants import SQLMESH_INITIAL_RUN_ID
from dmpworks.scheduler.handler.works.check_datasets_ready_handler import (
    REQUIRED_CHECKPOINTS,
    check_datasets_ready_handler,
)

PATCH_BASE = "dmpworks.scheduler.handler.works.check_datasets_ready_handler"

# Maps each checkpoint pool_key to the task_key format used in the checkpoint record.
CHECKPOINT_TASK_KEYS = {
    "run_id_openalex_works": "transform#2025-01-15",
    "run_id_datacite": "transform#2025-01-10",
    "run_id_crossref_metadata": "transform#2025-01-08",
    "run_id_ror": "download#2025-01-05",
    "run_id_data_citation_corpus": "download#2025-01-03",
}


def make_checkpoint(run_id: str, *, task_key: str = "transform#2025-01-15") -> MagicMock:
    """Build a minimal mock TaskCheckpointRecord."""
    cp = MagicMock()
    cp.run_id = run_id
    cp.task_key = task_key
    return cp


def make_release(status: str) -> MagicMock:
    """Build a minimal mock DatasetReleaseRecord."""
    rel = MagicMock()
    rel.status = status
    return rel


CHECKPOINT_RUN_IDS = {
    "run_id_openalex_works": "20250115T060000-a1b2c3d4",
    "run_id_datacite": "20250110T060000-b2c3d4e5",
    "run_id_crossref_metadata": "20250108T060000-c3d4e5f6",
    "run_id_ror": "20250105T060000-d4e5f6a7",
    "run_id_data_citation_corpus": "20250103T060000-e5f6a7b8",
}


class TestAllReady:
    """All datasets have completed checkpoints and none are in-flight."""

    def test_returns_all_ready_with_run_ids_and_release_dates(self):
        sqlmesh_checkpoint = make_checkpoint("20250113T060000-f6a7b8c9", task_key="sqlmesh#2025-01-13")

        def mock_get_checkpoint(*, workflow_key, task_name, **kwargs):
            if workflow_key == "process-works" and task_name == "sqlmesh":
                return sqlmesh_checkpoint
            pool_key = next(k for k, (wk, tn) in REQUIRED_CHECKPOINTS.items() if wk == workflow_key and tn == task_name)
            return make_checkpoint(CHECKPOINT_RUN_IDS[pool_key], task_key=CHECKPOINT_TASK_KEYS[pool_key])

        with (
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=mock_get_checkpoint),
            patch(f"{PATCH_BASE}.get_latest_known_release", return_value=make_release("COMPLETED")),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = check_datasets_ready_handler({}, None)

        assert result["all_ready"] is True
        assert result["run_id_openalex_works"] == "20250115T060000-a1b2c3d4"
        assert result["run_id_datacite"] == "20250110T060000-b2c3d4e5"
        assert result["run_id_crossref_metadata"] == "20250108T060000-c3d4e5f6"
        assert result["run_id_ror"] == "20250105T060000-d4e5f6a7"
        assert result["run_id_data_citation_corpus"] == "20250103T060000-e5f6a7b8"
        assert result["run_id_sqlmesh_prev"] == "20250113T060000-f6a7b8c9"
        assert result["release_date_openalex_works"] == "2025-01-15"
        assert result["release_date_datacite"] == "2025-01-10"
        assert result["release_date_crossref_metadata"] == "2025-01-08"
        assert result["release_date_ror"] == "2025-01-05"
        assert result["release_date_data_citation_corpus"] == "2025-01-03"

    def test_returns_all_ready_when_latest_release_is_none(self):
        """No release record at all is fine — dataset was never started."""

        def mock_get_checkpoint(*, workflow_key, task_name, **kwargs):
            if workflow_key == "process-works":
                return make_checkpoint("20250113T060000-f6a7b8c9", task_key="sqlmesh#2025-01-13")
            pool_key = next(k for k, (wk, tn) in REQUIRED_CHECKPOINTS.items() if wk == workflow_key and tn == task_name)
            return make_checkpoint(CHECKPOINT_RUN_IDS[pool_key], task_key=CHECKPOINT_TASK_KEYS[pool_key])

        with (
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=mock_get_checkpoint),
            patch(f"{PATCH_BASE}.get_latest_known_release", return_value=None),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = check_datasets_ready_handler({}, None)

        assert result["all_ready"] is True


class TestMissingCheckpoint:
    """One or more checkpoints are absent."""

    def test_returns_not_ready_when_checkpoint_missing(self):
        """First missing checkpoint causes early return."""
        with (
            patch(f"{PATCH_BASE}.get_task_checkpoint", return_value=None),
            patch(f"{PATCH_BASE}.get_latest_known_release") as mock_release,
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = check_datasets_ready_handler({}, None)

        assert result == {"all_ready": False}
        mock_release.assert_not_called()


class TestIngestInProgress:
    """All checkpoints exist but one dataset is still being ingested."""

    def test_returns_not_ready_when_release_is_started(self):
        call_count = 0

        def mock_get_checkpoint(*, workflow_key, task_name, **kwargs):
            # Return a valid checkpoint for each required dataset
            if workflow_key == "process-works":
                return None
            pool_key = next(k for k, (wk, tn) in REQUIRED_CHECKPOINTS.items() if wk == workflow_key and tn == task_name)
            return make_checkpoint(CHECKPOINT_RUN_IDS[pool_key], task_key=CHECKPOINT_TASK_KEYS[pool_key])

        def mock_get_release(*, dataset):
            nonlocal call_count
            call_count += 1
            # First dataset's release is in STARTED state
            if call_count == 1:
                return make_release("STARTED")
            return make_release("COMPLETED")

        with (
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=mock_get_checkpoint),
            patch(f"{PATCH_BASE}.get_latest_known_release", side_effect=mock_get_release),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = check_datasets_ready_handler({}, None)

        assert result == {"all_ready": False}


class TestSqlmeshPrevFallback:
    """No previous SQLMesh checkpoint → use SQLMESH_INITIAL_RUN_ID."""

    def test_returns_initial_run_id_when_no_sqlmesh_checkpoint(self):
        def mock_get_checkpoint(*, workflow_key, task_name, **kwargs):
            if workflow_key == "process-works" and task_name == "sqlmesh":
                return None  # no previous sqlmesh run
            pool_key = next(k for k, (wk, tn) in REQUIRED_CHECKPOINTS.items() if wk == workflow_key and tn == task_name)
            return make_checkpoint(CHECKPOINT_RUN_IDS[pool_key], task_key=CHECKPOINT_TASK_KEYS[pool_key])

        with (
            patch(f"{PATCH_BASE}.get_task_checkpoint", side_effect=mock_get_checkpoint),
            patch(f"{PATCH_BASE}.get_latest_known_release", return_value=make_release("COMPLETED")),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = check_datasets_ready_handler({}, None)

        assert result["all_ready"] is True
        assert result["run_id_sqlmesh_prev"] == SQLMESH_INITIAL_RUN_ID
