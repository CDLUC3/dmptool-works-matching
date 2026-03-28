"""Unit tests for s3_cleanup_handler."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from dmpworks.scheduler.handler.s3_cleanup_handler import s3_cleanup_handler

PATCH_BASE = "dmpworks.scheduler.handler.s3_cleanup_handler"
BUCKET = "test-bucket"


class NoSuchLifecycleConfiguration(Exception):
    """Stand-in for the boto3 NoSuchLifecycleConfiguration error used in tests."""


def make_s3_mock(*, existing_rules: list | None = None) -> MagicMock:
    """Return a mock boto3 S3 client.

    Args:
        existing_rules: If provided, `get_bucket_lifecycle_configuration` returns these rules.
            If None, raises NoSuchLifecycleConfiguration.
    """
    mock_s3 = MagicMock()
    mock_s3.exceptions.from_code.return_value = NoSuchLifecycleConfiguration
    if existing_rules is None:
        mock_s3.get_bucket_lifecycle_configuration.side_effect = NoSuchLifecycleConfiguration("no config")
    else:
        mock_s3.get_bucket_lifecycle_configuration.return_value = {"Rules": existing_rules}
    return mock_s3


def make_stale(prefix_type: str, run_id: str) -> dict:
    return {"prefix_type": prefix_type, "run_id": run_id, "bucket_name": BUCKET}


def run_handler(stale_prefixes: list, mock_s3: MagicMock | None = None) -> dict:
    """Invoke the handler with patched dependencies."""
    mock_settings = MagicMock()
    mock_settings.bucket_name = BUCKET

    with (
        patch(f"{PATCH_BASE}.S3CleanupEnvSettings", return_value=mock_settings),
        patch(f"{PATCH_BASE}.build_cleanup_plan", return_value=stale_prefixes),
        patch(f"{PATCH_BASE}.boto3") as mock_boto3,
    ):
        if mock_s3 is not None:
            mock_boto3.client.return_value = mock_s3
        return s3_cleanup_handler({}, None)


class TestNothingToSchedule:
    def test_returns_zero_count(self):
        result = run_handler([])
        assert result == {"scheduled_count": 0}

    def test_does_not_call_s3(self):
        mock_s3 = make_s3_mock()
        run_handler([], mock_s3=mock_s3)
        mock_s3.get_bucket_lifecycle_configuration.assert_not_called()
        mock_s3.put_bucket_lifecycle_configuration.assert_not_called()


class TestLifecycleRulesWritten:
    def test_returns_correct_scheduled_count(self):
        stale = [
            make_stale("openalex-works-download", "run-1"),
            make_stale("datacite-transform", "run-2"),
        ]
        result = run_handler(stale, mock_s3=make_s3_mock())
        assert result == {"scheduled_count": 2}

    def test_put_lifecycle_called_once(self):
        stale = [make_stale("openalex-works-download", "run-1")]
        mock_s3 = make_s3_mock()
        run_handler(stale, mock_s3=mock_s3)
        mock_s3.put_bucket_lifecycle_configuration.assert_called_once()

    def test_lifecycle_called_with_correct_bucket(self):
        stale = [make_stale("openalex-works-download", "run-1")]
        mock_s3 = make_s3_mock()
        run_handler(stale, mock_s3=mock_s3)
        kwargs = mock_s3.put_bucket_lifecycle_configuration.call_args.kwargs
        assert kwargs["Bucket"] == BUCKET

    def test_rule_id_format(self):
        stale = [make_stale("openalex-works-download", "run-abc")]
        mock_s3 = make_s3_mock()
        run_handler(stale, mock_s3=mock_s3)
        rules = mock_s3.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"]["Rules"]
        assert rules[0]["ID"] == "cleanup-openalex-works-download-run-abc"

    def test_rule_prefix_format(self):
        stale = [make_stale("openalex-works-download", "run-abc")]
        mock_s3 = make_s3_mock()
        run_handler(stale, mock_s3=mock_s3)
        rules = mock_s3.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"]["Rules"]
        assert rules[0]["Filter"] == {"Prefix": "openalex-works-download/run-abc/"}

    def test_rule_expiration_days_is_one(self):
        stale = [make_stale("process-works-sqlmesh", "run-x")]
        mock_s3 = make_s3_mock()
        run_handler(stale, mock_s3=mock_s3)
        rules = mock_s3.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"]["Rules"]
        assert rules[0]["Expiration"] == {"Days": 1}
        assert rules[0]["Status"] == "Enabled"

    def test_one_rule_per_stale_prefix(self):
        stale = [
            make_stale("openalex-works-download", "run-1"),
            make_stale("datacite-subset", "run-2"),
            make_stale("process-dmps-dmp-works-search", "run-3"),
        ]
        mock_s3 = make_s3_mock()
        run_handler(stale, mock_s3=mock_s3)
        rules = mock_s3.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"]["Rules"]
        assert len(rules) == 3


class TestExistingLifecycleConfig:
    def test_non_cleanup_rules_are_preserved(self):
        existing = [{"ID": "glacier-archive", "Filter": {"Prefix": "logs/"}, "Status": "Enabled"}]
        stale = [make_stale("openalex-works-download", "run-1")]
        mock_s3 = make_s3_mock(existing_rules=existing)
        run_handler(stale, mock_s3=mock_s3)
        rules = mock_s3.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"]["Rules"]
        ids = [r["ID"] for r in rules]
        assert "glacier-archive" in ids

    def test_old_cleanup_rules_are_removed(self):
        existing = [
            {"ID": "cleanup-openalex-works-download-old-run", "Filter": {}, "Status": "Enabled"},
            {"ID": "keep-this-rule", "Filter": {}, "Status": "Enabled"},
        ]
        stale = [make_stale("openalex-works-download", "new-run")]
        mock_s3 = make_s3_mock(existing_rules=existing)
        run_handler(stale, mock_s3=mock_s3)
        rules = mock_s3.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"]["Rules"]
        ids = [r["ID"] for r in rules]
        assert "cleanup-openalex-works-download-old-run" not in ids
        assert "keep-this-rule" in ids
        assert "cleanup-openalex-works-download-new-run" in ids

    def test_no_existing_config_starts_from_empty(self):
        stale = [make_stale("process-works-sqlmesh", "run-1")]
        mock_s3 = make_s3_mock(existing_rules=None)  # raises NoSuchLifecycleConfiguration
        run_handler(stale, mock_s3=mock_s3)
        rules = mock_s3.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"]["Rules"]
        assert len(rules) == 1
        assert rules[0]["ID"] == "cleanup-process-works-sqlmesh-run-1"
