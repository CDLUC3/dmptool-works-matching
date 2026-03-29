"""Unit tests for check_datasets_ready_handler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dmpworks.scheduler.dynamodb_store import SQLMESH_INITIAL_RUN_ID
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


def make_checkpoint(run_id: str, *, task_key: str = "task#2025-01-15") -> MagicMock:
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
    "run_id_openalex_works": "oa-run",
    "run_id_datacite": "dc-run",
    "run_id_crossref_metadata": "cr-run",
    "run_id_ror": "ror-run",
    "run_id_data_citation_corpus": "dcc-run",
}


class TestAllReady:
    """All datasets have completed checkpoints and none are in-flight."""

    def test_returns_all_ready_with_run_ids_and_release_dates(self):
        sqlmesh_checkpoint = make_checkpoint("sqlmesh-prev-run", task_key="sqlmesh#2025-01-13")

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
        assert result["run_id_openalex_works"] == "oa-run"
        assert result["run_id_datacite"] == "dc-run"
        assert result["run_id_crossref_metadata"] == "cr-run"
        assert result["run_id_ror"] == "ror-run"
        assert result["run_id_data_citation_corpus"] == "dcc-run"
        assert result["run_id_sqlmesh_prev"] == "sqlmesh-prev-run"
        assert result["release_date_openalex_works"] == "2025-01-15"
        assert result["release_date_datacite"] == "2025-01-10"
        assert result["release_date_crossref_metadata"] == "2025-01-08"
        assert result["release_date_ror"] == "2025-01-05"
        assert result["release_date_data_citation_corpus"] == "2025-01-03"

    def test_returns_all_ready_when_latest_release_is_none(self):
        """No release record at all is fine — dataset was never started."""

        def mock_get_checkpoint(*, workflow_key, task_name, **kwargs):
            if workflow_key == "process-works":
                return make_checkpoint("sqlmesh-prev", task_key="sqlmesh#2025-01-13")
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
