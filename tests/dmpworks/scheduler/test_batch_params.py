"""Tests for compute_batch_params, focusing on predecessor checkpoint resolution."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dmpworks.scheduler.batch_params import compute_batch_params


def make_config(**overrides):
    """Return a minimal mock LambdaConfig."""
    config = MagicMock()
    config.to_env_dict.return_value = overrides
    return config


def make_checkpoint(run_id: str):
    cp = MagicMock()
    cp.run_id = run_id
    return cp


BASE_EVENT = {
    "workflow_key": "openalex-works",
    "task_type": "transform",
    "publication_date": "2025-01-01",
    "bucket_name": "my-bucket",
    "aws_env": "dev",
    "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:test:exec-1",
    "openalex_works_transform_batch_size": "8",
    "openalex_works_transform_row_group_size": "100000",
    "openalex_works_transform_row_groups_per_file": "2",
    "openalex_works_transform_max_workers": "4",
    "openalex_works_transform_include_xpac": "false",
}


class TestComputeBatchParamsRunId:
    @patch("dmpworks.scheduler.batch_params.get_task_checkpoint")
    @patch("dmpworks.scheduler.batch_params.JOB_FACTORIES")
    def test_uses_preassigned_run_id_when_present(self, mock_factories, mock_get_checkpoint):
        """compute_batch_params uses the run_id from the event when provided by the parent SM."""
        captured_kwargs = {}

        def fake_factory(**kwargs):
            captured_kwargs.update(kwargs)
            return {
                "run_name": "openalex-works-transform",
                "JobName": "j",
                "JobQueue": "q",
                "JobDefinition": "d",
                "ContainerOverrides": {
                    "Command": ["/bin/bash", "-c", "echo"],
                    "Vcpus": 1,
                    "Memory": 1024,
                    "Environment": [],
                },
            }

        mock_factories.__getitem__ = MagicMock(return_value=fake_factory)
        event = {**BASE_EVENT, "run_id": "preassigned-run-id-123"}

        result = compute_batch_params(event, make_config())

        assert result["run_id"] == "preassigned-run-id-123"
        assert captured_kwargs["run_id"] == "preassigned-run-id-123"

    @patch("dmpworks.scheduler.batch_params.get_task_checkpoint")
    @patch("dmpworks.scheduler.batch_params.JOB_FACTORIES")
    def test_generates_run_id_when_absent(self, mock_factories, mock_get_checkpoint):
        """compute_batch_params generates a fresh run_id when none is provided."""

        def fake_factory(**kwargs):
            return {
                "run_name": "openalex-works-transform",
                "JobName": "j",
                "JobQueue": "q",
                "JobDefinition": "d",
                "ContainerOverrides": {
                    "Command": ["/bin/bash", "-c", "echo"],
                    "Vcpus": 1,
                    "Memory": 1024,
                    "Environment": [],
                },
            }

        mock_factories.__getitem__ = MagicMock(return_value=fake_factory)

        result = compute_batch_params(BASE_EVENT, make_config())

        assert result["run_id"] is not None
        assert len(result["run_id"]) > 0
        assert result["run_id"] != ""


class TestComputeBatchParamsPredecessor:
    @patch("dmpworks.scheduler.batch_params.get_task_checkpoint")
    @patch("dmpworks.scheduler.batch_params.JOB_FACTORIES")
    def test_single_predecessor_still_works(self, mock_factories, mock_get_checkpoint):
        mock_get_checkpoint.return_value = make_checkpoint("prev-run-123")
        captured_kwargs = {}

        def fake_factory(**kwargs):
            captured_kwargs.update(kwargs)
            return {
                "run_name": "openalex-works-transform",
                "JobName": "j",
                "JobQueue": "q",
                "JobDefinition": "d",
                "ContainerOverrides": {
                    "Command": ["/bin/bash", "-c", "echo"],
                    "Vcpus": 1,
                    "Memory": 1024,
                    "Environment": [],
                },
            }

        mock_factories.__getitem__ = MagicMock(return_value=fake_factory)

        event = {
            **BASE_EVENT,
            "predecessor_task_name": "download",
        }

        compute_batch_params(event, make_config())

        mock_get_checkpoint.assert_called_once_with(
            workflow_key="openalex-works", task_name="download", date="2025-01-01"
        )
        assert captured_kwargs.get("prev_job_run_id") == "prev-run-123"

    @patch("dmpworks.scheduler.batch_params.get_task_checkpoint")
    @patch("dmpworks.scheduler.batch_params.JOB_FACTORIES")
    def test_no_predecessor_fields_defaults_prev_job_run_id_to_none(self, mock_factories, mock_get_checkpoint):
        captured_kwargs = {}

        def fake_factory(**kwargs):
            captured_kwargs.update(kwargs)
            return {
                "run_name": "openalex-works-transform",
                "JobName": "j",
                "JobQueue": "q",
                "JobDefinition": "d",
                "ContainerOverrides": {
                    "Command": ["/bin/bash", "-c", "echo"],
                    "Vcpus": 1,
                    "Memory": 1024,
                    "Environment": [],
                },
            }

        mock_factories.__getitem__ = MagicMock(return_value=fake_factory)

        compute_batch_params(BASE_EVENT, make_config())

        mock_get_checkpoint.assert_not_called()
        assert captured_kwargs.get("prev_job_run_id") is None


# ---------------------------------------------------------------------------
# End-to-end event fixtures for TestComputeBatchParamsWithRealFactories.
# These mirror the exact field names sent by the Step Functions state machines
# after the checksum→file_hash rename.
# ---------------------------------------------------------------------------

SFN_BASE = {
    "publication_date": "2026-01-01",
    "aws_env": "dev",
    "bucket_name": "b",
    "download_url": "http://x",
    "file_name": None,
    "use_subset": False,
}

SFN_EVENTS: dict[tuple[str, str], dict] = {
    ("ror", "download"): {**SFN_BASE, "task_type": "download", "workflow_key": "ror", "file_hash": "md5:abc"},
    ("data-citation-corpus", "download"): {
        **SFN_BASE,
        "task_type": "download",
        "workflow_key": "data-citation-corpus",
        "file_hash": "md5:abc",
    },
    ("openalex-works", "download"): {**SFN_BASE, "task_type": "download", "workflow_key": "openalex-works"},
    ("crossref-metadata", "download"): {
        **SFN_BASE,
        "task_type": "download",
        "workflow_key": "crossref-metadata",
        "file_name": "crossref.tar.gz",
    },
    ("datacite", "download"): {**SFN_BASE, "task_type": "download", "workflow_key": "datacite"},
    ("openalex-works", "subset"): {
        **SFN_BASE,
        "task_type": "subset",
        "workflow_key": "openalex-works",
        "use_subset": True,
        "predecessor_task_name": "download",
    },
    ("crossref-metadata", "subset"): {
        **SFN_BASE,
        "task_type": "subset",
        "workflow_key": "crossref-metadata",
        "use_subset": True,
        "predecessor_task_name": "download",
    },
    ("datacite", "subset"): {
        **SFN_BASE,
        "task_type": "subset",
        "workflow_key": "datacite",
        "use_subset": True,
        "predecessor_task_name": "download",
    },
    ("openalex-works", "transform"): {
        **SFN_BASE,
        "task_type": "transform",
        "workflow_key": "openalex-works",
        "predecessor_task_name": "download",
    },
    ("crossref-metadata", "transform"): {
        **SFN_BASE,
        "task_type": "transform",
        "workflow_key": "crossref-metadata",
        "predecessor_task_name": "download",
    },
    ("datacite", "transform"): {
        **SFN_BASE,
        "task_type": "transform",
        "workflow_key": "datacite",
        "predecessor_task_name": "download",
    },
}

# Config env-var overrides per (dataset, task_type); UPPERCASE keys are lowercased by compute_batch_params.
# Only entries that need non-event config values are listed; all others get {}.
SFN_CONFIG_ENVS: dict[tuple[str, str], dict[str, str]] = {
    ("openalex-works", "download"): {"OPENALEX_BUCKET_NAME": "oa"},
    ("crossref-metadata", "download"): {"CROSSREF_METADATA_BUCKET_NAME": "cb"},
    ("datacite", "download"): {"DATACITE_BUCKET_NAME": "db"},
    ("openalex-works", "transform"): {
        "OPENALEX_WORKS_TRANSFORM_BATCH_SIZE": "8",
        "OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE": "100000",
        "OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE": "2",
        "OPENALEX_WORKS_TRANSFORM_MAX_WORKERS": "4",
        "OPENALEX_WORKS_TRANSFORM_INCLUDE_XPAC": "false",
    },
    ("crossref-metadata", "transform"): {
        "CROSSREF_METADATA_TRANSFORM_BATCH_SIZE": "8",
        "CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE": "100000",
        "CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE": "2",
        "CROSSREF_METADATA_TRANSFORM_MAX_WORKERS": "4",
    },
    ("datacite", "transform"): {
        "DATACITE_TRANSFORM_BATCH_SIZE": "8",
        "DATACITE_TRANSFORM_ROW_GROUP_SIZE": "100000",
        "DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE": "2",
        "DATACITE_TRANSFORM_MAX_WORKERS": "4",
    },
}


class TestComputeBatchParamsWithRealFactories:
    @pytest.mark.parametrize(
        "key",
        list(SFN_EVENTS.keys()),
        ids=[f"{d}-{t}" for d, t in SFN_EVENTS.keys()],
    )
    @patch("dmpworks.scheduler.batch_params.get_task_checkpoint")
    def test_sfn_event_succeeds_for_all_datasets(self, mock_get_checkpoint, key):
        """compute_batch_params must not raise for any valid SFN event + config combination.

        Exercises the full path: SFN event payload → pool aliasing → real factory call.
        A missing key alias or renamed factory parameter raises TypeError here, catching
        the class of bug that caused the original checksum/file_hash mismatch.
        """
        dataset, task_type = key
        mock_get_checkpoint.return_value = None
        result = compute_batch_params(SFN_EVENTS[key], make_config(**SFN_CONFIG_ENVS.get(key, {})))
        assert "run_id" in result
        assert "run_name" in result
        assert "batch_params" in result
        assert result["batch_params"]["JobName"].startswith(f"{result['run_name']}-2026-01-01-")
