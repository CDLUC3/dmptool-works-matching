"""Unit tests for get_batch_job_params_handler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dmpworks.scheduler.handler.task.get_batch_job_params_handler import get_batch_job_params_handler

PATCH_BASE = "dmpworks.scheduler.handler.task.get_batch_job_params_handler"

BASE_EVENT = {
    "workflow_key": "openalex-works",
    "task_type": "download",
    "release_date": "2025-01-15",
    "bucket_name": "dmpworks-dev",
    "aws_env": "dev",
    "execution_arn": "arn:aws:states:us-east-1:123:execution:sm:abc",
}

MOCK_BATCH_RESULT = {
    "run_id": "20250115T060000-aabbccdd",
    "run_name": "openalex-works-download",
    "batch_params": {
        "JobName": "openalex-works-download-2025-01-15-20250115T060000-aabbccdd",
        "JobQueue": "arn:aws:batch:us-east-1:123:job-queue/dmpworks-dev",
        "JobDefinition": "arn:aws:batch:us-east-1:123:job-definition/dmpworks-dev",
        "ContainerOverrides": {
            "Command": ["dmpworks", "batch", "download"],
            "Vcpus": 2,
            "Memory": 4096,
            "Environment": [{"Name": "AWS_ENV", "Value": "dev"}],
        },
    },
}


class TestGetBatchJobParams:
    """get_batch_job_params_handler computes params and creates a TaskRunRecord."""

    def test_creates_task_run_and_returns_merged_result(self):
        with (
            patch(f"{PATCH_BASE}.compute_batch_params", return_value=MOCK_BATCH_RESULT),
            patch(f"{PATCH_BASE}.create_task_run") as mock_create,
            patch(f"{PATCH_BASE}.load_lambda_config", return_value=MagicMock()),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = get_batch_job_params_handler(BASE_EVENT, None)

        mock_create.assert_called_once_with(
            run_name="openalex-works-download",
            run_id="20250115T060000-aabbccdd",
            execution_arn="arn:aws:states:us-east-1:123:execution:sm:abc",
            metadata={
                "workflow_key": "openalex-works",
                "release_date": "2025-01-15",
            },
        )
        assert result["run_id"] == "20250115T060000-aabbccdd"
        assert result["run_name"] == "openalex-works-download"
        assert result["batch_params"] == MOCK_BATCH_RESULT["batch_params"]
        assert result["workflow_key"] == "openalex-works"
