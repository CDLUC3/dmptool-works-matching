"""Unit tests for version_checker_handler."""

from __future__ import annotations

import json
import re
from unittest.mock import MagicMock, patch

import pendulum

from dmpworks.scheduler.config import (
    CrossrefMetadataConfig,
    DataciteConfig,
    LambdaConfig,
    OpenalexWorksConfig,
    OpenSearchClientConfig,
)
from dmpworks.scheduler.dynamodb_store import DatasetReleaseRecord
from dmpworks.scheduler.handler.version_checker_handler import version_checker_handler


def make_release_record(dataset: str, publication_date: str, **kwargs) -> DatasetReleaseRecord:
    """Build a minimal DatasetReleaseRecord without hitting DynamoDB."""
    record = DatasetReleaseRecord()
    record.dataset = dataset
    record.publication_date = publication_date
    record.download_url = kwargs.get("download_url", "https://example.com/file.zip")
    record.file_hash = kwargs.get("file_hash", "md5:abc123")
    record.file_name = kwargs.get("file_name", "file.zip")
    return record


BASE_ENV = {
    "STATE_MACHINE_ARN": "arn:aws:states:us-east-1:123456789012:stateMachine:DatasetIngestStateMachine",
    "AWS_ENV": "dev",
    "AWS_REGION": "us-east-1",
    "BUCKET_NAME": "my-bucket",
    "DATACITE_CREDENTIALS_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:datacite-creds",
}


def make_config(*, enabled_datasets: list[str] | None = None, dataset_subset_enable: bool = False) -> LambdaConfig:
    """Build a minimal LambdaConfig for testing."""
    return LambdaConfig(
        enabled_datasets=enabled_datasets if enabled_datasets is not None else ["ror"],
        crossref_metadata_config=CrossrefMetadataConfig(bucket_name="crossref-bucket"),
        datacite_config=DataciteConfig(bucket_name="datacite-bucket", bucket_region="us-east-1"),
        openalex_works_config=OpenalexWorksConfig(bucket_name="openalex-bucket"),
        opensearch_client_config=OpenSearchClientConfig(host="localhost"),
    )


class TestVersionCheckerHandlerNewRelease:
    """Tests for successful new release detection and SFN trigger."""

    def test_new_release_triggers_sfn_execution(self):
        """When discover returns a record, start_execution is called with the correct payload."""
        release_record = make_release_record("ror", "2026-03-12")

        with (
            patch.dict("os.environ", BASE_ENV),
            patch("dmpworks.scheduler.handler.version_checker_handler.load_lambda_config", return_value=make_config()),
            patch("dmpworks.scheduler.handler.version_checker_handler.get_latest_known_release", return_value=None),
            patch(
                "dmpworks.scheduler.handler.version_checker_handler.discover_latest_release",
                return_value=release_record,
            ),
            patch("dmpworks.scheduler.handler.version_checker_handler.boto3.client") as mock_boto3,
        ):
            mock_sfn = MagicMock()
            mock_boto3.return_value = mock_sfn

            result = version_checker_handler({}, None)

        assert result["triggered"] == [{"dataset": "ror", "publication_date": "2026-03-12"}]
        assert result["dry_run"] is False
        assert len(result["discovered"]) == 1
        assert result["discovered"][0]["dataset"] == "ror"
        mock_sfn.start_execution.assert_called_once()
        call_kwargs = mock_sfn.start_execution.call_args[1]
        assert call_kwargs["stateMachineArn"] == BASE_ENV["STATE_MACHINE_ARN"]
        assert re.match(r"^ror-2026-03-12-\d{8}T\d{6}-[0-9a-f]{8}$", call_kwargs["name"])
        payload = json.loads(call_kwargs["input"])
        assert payload["workflow_key"] == "ror"
        assert payload["publication_date"] == "2026-03-12"
        assert re.match(r"^\d{8}T\d{6}-[0-9a-f]{8}$", payload["run_id"])
        assert payload["aws_env"] == "dev"
        assert payload["bucket_name"] == "my-bucket"
        assert payload["use_subset"] is False


class TestVersionCheckerHandlerNoRelease:
    """Tests for when no new release is discovered."""

    def test_no_new_release_skips_sfn(self):
        """When discover returns None, SFN is not invoked."""
        with (
            patch.dict("os.environ", BASE_ENV),
            patch("dmpworks.scheduler.handler.version_checker_handler.load_lambda_config", return_value=make_config()),
            patch("dmpworks.scheduler.handler.version_checker_handler.get_latest_known_release", return_value=None),
            patch("dmpworks.scheduler.handler.version_checker_handler.discover_latest_release", return_value=None),
            patch("dmpworks.scheduler.handler.version_checker_handler.boto3.client") as mock_boto3,
        ):
            mock_sfn = MagicMock()
            mock_boto3.return_value = mock_sfn
            result = version_checker_handler({}, None)

        assert result["triggered"] == []
        assert result["discovered"] == []
        assert result["dry_run"] is False
        mock_sfn.start_execution.assert_not_called()


class TestVersionCheckerHandlerUnknownDataset:
    """Tests for datasets not registered in DATASET_DETECTORS."""

    def test_unknown_dataset_is_skipped(self):
        """A dataset not in DATASET_DETECTORS emits a warning and does not crash."""
        with (
            patch.dict("os.environ", BASE_ENV),
            patch(
                "dmpworks.scheduler.handler.version_checker_handler.load_lambda_config",
                return_value=make_config(enabled_datasets=["unknown-dataset"]),
            ),
            patch("dmpworks.scheduler.handler.version_checker_handler.discover_latest_release") as mock_discover,
            patch("dmpworks.scheduler.handler.version_checker_handler.boto3.client") as mock_boto3,
        ):
            mock_sfn = MagicMock()
            mock_boto3.return_value = mock_sfn
            result = version_checker_handler({}, None)

        assert result["triggered"] == []
        assert result["discovered"] == []
        assert result["dry_run"] is False
        mock_discover.assert_not_called()
        mock_sfn.start_execution.assert_not_called()


class TestVersionCheckerHandlerStartDt:
    """Tests for start_dt propagation from latest known release."""

    def test_start_dt_from_latest_known_release(self):
        """When a prior release exists, its publication_date is parsed and passed as start_dt."""
        prior_record = make_release_record("ror", "2026-01-01")
        new_record = make_release_record("ror", "2026-03-12")

        with (
            patch.dict("os.environ", BASE_ENV),
            patch("dmpworks.scheduler.handler.version_checker_handler.load_lambda_config", return_value=make_config()),
            patch(
                "dmpworks.scheduler.handler.version_checker_handler.get_latest_known_release", return_value=prior_record
            ),
            patch(
                "dmpworks.scheduler.handler.version_checker_handler.discover_latest_release", return_value=new_record
            ) as mock_discover,
            patch("dmpworks.scheduler.handler.version_checker_handler.boto3.client") as mock_boto3,
        ):
            mock_boto3.return_value = MagicMock()
            version_checker_handler({}, None)

        call_kwargs = mock_discover.call_args[1]
        assert call_kwargs["detector_kwargs"]["start_dt"] == pendulum.parse("2026-01-01")


class TestVersionCheckerHandlerEmptyDatasets:
    """Tests for empty enabled_datasets."""

    def test_empty_enabled_datasets_does_nothing(self):
        """When enabled_datasets is empty, no detectors are called."""
        with (
            patch.dict("os.environ", BASE_ENV),
            patch(
                "dmpworks.scheduler.handler.version_checker_handler.load_lambda_config",
                return_value=make_config(enabled_datasets=[]),
            ),
            patch("dmpworks.scheduler.handler.version_checker_handler.discover_latest_release") as mock_discover,
            patch("dmpworks.scheduler.handler.version_checker_handler.boto3.client") as mock_boto3,
        ):
            mock_sfn = MagicMock()
            mock_boto3.return_value = mock_sfn
            result = version_checker_handler({}, None)

        assert result["triggered"] == []
        assert result["discovered"] == []
        assert result["dry_run"] is False
        mock_discover.assert_not_called()
        mock_sfn.start_execution.assert_not_called()
