"""Integration tests for dataset ingest workflow handlers using DynamoDB Local."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dmpworks.scheduler.config import (
    CrossrefMetadataConfig,
    DataciteConfig,
    LambdaConfig,
    OpenalexWorksConfig,
    OpenSearchClientConfig,
)
from dmpworks.scheduler.handler.task.get_batch_job_params_handler import get_batch_job_params_handler
from dmpworks.scheduler.handler.task.set_task_run_complete_handler import set_task_run_complete_handler
from dmpworks.scheduler.dynamodb_store import (
    DatasetReleaseRecord,
    TaskCheckpointRecord,
    TaskRunRecord,
    create_task_run,
    set_task_checkpoint,
    set_task_run_status,
)
from tests.dmpworks.scheduler.conftest import make_release_record

BASE_EVENT = {
    "workflow_key": "ror",
    "release_date": "2025-01-01",
    "aws_env": "dev",
    "bucket_name": "test-bucket",
    "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:test:exec-1",
    "download_url": "https://example.com/ror.zip",
    "file_hash": "md5:abc123",
    "file_name": None,
    "use_subset": False,
    "log_level": "INFO",
}


class TestGetBatchJobParamsHandler:
    """Tests for get_batch_job_params_handler."""

    @pytest.fixture(autouse=True)
    def mock_settings(self, monkeypatch):
        import importlib

        mod = importlib.import_module("dmpworks.scheduler.handler.task.get_batch_job_params_handler")
        monkeypatch.setattr(mod, "LambdaEnvSettings", MagicMock())
        config = LambdaConfig(
            crossref_metadata_config=CrossrefMetadataConfig(bucket_name="crossref-bucket"),
            datacite_config=DataciteConfig(bucket_name="datacite-bucket", bucket_region="us-east-1"),
            openalex_works_config=OpenalexWorksConfig(bucket_name="openalex-bucket"),
            opensearch_client_config=OpenSearchClientConfig(host="localhost"),
        )
        monkeypatch.setattr(mod, "load_lambda_config", MagicMock(return_value=config))

    def test_generates_unique_run_id(self, dynamodb_tables):
        """Each invocation produces a different run_id."""
        make_release_record("ror", "2025-01-01")
        event = {**BASE_EVENT, "task_type": "download"}

        result1 = get_batch_job_params_handler(event, None)
        result2 = get_batch_job_params_handler(event, None)

        assert result1["run_id"] != result2["run_id"]

    def test_creates_task_run_record(self, dynamodb_tables):
        """get_batch_job_params_handler creates a STARTED TaskRunRecord."""
        make_release_record("ror", "2025-01-01")
        event = {**BASE_EVENT, "task_type": "download"}

        result = get_batch_job_params_handler(event, None)

        record = TaskRunRecord.get(result["run_name"], result["run_id"])
        assert record.status == "STARTED"
        assert record.metadata["workflow_key"] == "ror"
        assert record.metadata["release_date"] == "2025-01-01"

    def test_returns_batch_params(self, dynamodb_tables):
        """get_batch_job_params_handler returns run_id, run_name, and batch_params."""
        make_release_record("ror", "2025-01-01")
        event = {**BASE_EVENT, "task_type": "download"}

        result = get_batch_job_params_handler(event, None)

        assert result["run_name"] == "ror-download"
        assert "run_id" in result
        assert "batch_params" in result
        assert result["batch_params"]["JobName"].startswith("ror-download-")

    def test_subset_boolean_env_var_is_lowercase(self, dynamodb_tables):
        """DATASET_SUBSET_ENABLE is serialized as 'true', not 'True'."""
        make_release_record("datacite", "2025-01-01")
        event = {**BASE_EVENT, "workflow_key": "datacite", "task_type": "subset", "use_subset": True}

        result = get_batch_job_params_handler(event, None)

        env_vars = {e["Name"]: e["Value"] for e in result["batch_params"]["ContainerOverrides"]["Environment"]}
        assert env_vars["DATASET_SUBSET_ENABLE"] == "true"

    def test_subset_path_env_vars_are_present(self, dynamodb_tables):
        """DATASET_SUBSET_INSTITUTIONS_S3_PATH and DATASET_SUBSET_DOIS_S3_PATH come from Lambda config."""
        make_release_record("datacite", "2025-01-01")
        event = {**BASE_EVENT, "workflow_key": "datacite", "task_type": "subset", "use_subset": True}

        result = get_batch_job_params_handler(event, None)

        env_vars = {e["Name"]: e["Value"] for e in result["batch_params"]["ContainerOverrides"]["Environment"]}
        assert env_vars["DATASET_SUBSET_INSTITUTIONS_S3_PATH"] == "meta/institutions.json"
        assert env_vars["DATASET_SUBSET_DOIS_S3_PATH"] == "meta/work_dois.json"

    def test_prev_run_id_is_read_from_dynamodb(self, dynamodb_tables):
        """For transform task type, PREV_JOB_RUN_ID is resolved from DynamoDB."""
        make_release_record("openalex-works", "2025-01-01")
        set_task_checkpoint(
            workflow_key="openalex-works",
            task_name="download",
            date="2025-01-01",
            run_id="20250101T060000-b2c3d4e5",
        )

        event = {
            **BASE_EVENT,
            "workflow_key": "openalex-works",
            "task_type": "transform",
            "predecessor_task_name": "download",
            "use_subset": False,
            "log_level": "INFO",
        }
        result = get_batch_job_params_handler(event, None)

        env_vars = {e["Name"]: e["Value"] for e in result["batch_params"]["ContainerOverrides"]["Environment"]}
        assert env_vars["PREV_JOB_RUN_ID"] == "20250101T060000-b2c3d4e5"


class TestSetTaskRunCompleteHandler:
    """Tests for set_task_run_complete_handler."""

    def test_marks_run_completed_and_writes_run_id(self, dynamodb_tables):
        """Completes TaskRunRecord and writes run_id to DatasetReleaseRecord."""
        make_release_record("ror", "2025-01-01")
        create_task_run(
            run_name="ror-download",
            run_id="20250101T060000-a1b2c3d4",
            execution_arn="arn:aws:states:us-east-1:123456789012:execution:test:exec-1",
            metadata={},
        )

        event = {
            **BASE_EVENT,
            "task_type": "download",
            "current": {"run_name": "ror-download", "run_id": "20250101T060000-a1b2c3d4"},
        }

        set_task_run_complete_handler(event, None)

        run_record = TaskRunRecord.get("ror-download", "20250101T060000-a1b2c3d4")
        assert run_record.status == "COMPLETED"

        checkpoint = TaskCheckpointRecord.get("ror", "download#2025-01-01")
        assert checkpoint.run_id == "20250101T060000-a1b2c3d4"
