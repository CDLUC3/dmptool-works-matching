from unittest.mock import patch

import pytest

from dmpworks.batch_submit.cli import (
    CROSSREF_METADATA_JOBS,
    DATACITE_JOBS,
    OPENALEX_WORKS_JOBS,
    PROCESS_DMPS_JOBS,
    PROCESS_WORKS_JOBS,
    ROR_JOBS,
    crossref_metadata_cmd,
    datacite_cmd,
    openalex_works_cmd,
    process_dmps_cmd,
    process_works_cmd,
    ror_cmd,
)
from dmpworks.batch_submit.jobs import submit_factory_job
from dmpworks.cli_utils import (
    DatasetSubsetAWS,
    RunIdentifiers,
    SQLMeshConfig,
)
from tests.dmpworks.batch_submit.conftest import expand_command, get_env_dict


def call_task(task_defs, task_name):
    """Call a task definition with submit_job mocked, return the factory params."""
    with patch("dmpworks.batch_submit.jobs.submit_job") as mock_submit:
        task_defs[task_name]()
    # Reconstruct the PascalCase params from what submit_job_from_params passed
    kw = mock_submit.call_args.kwargs
    return {
        "run_name": kw["job_name"],
        "JobQueue": kw["job_queue"],
        "JobDefinition": kw["job_definition"],
        "ContainerOverrides": {
            "Command": ["/bin/bash", "-c", kw["command"]],
            "Vcpus": kw["vcpus"],
            "Memory": kw["memory"],
            "Environment": [{"Name": e["name"], "Value": e["value"]} for e in kw["environment"]],
        },
    }


def invoke_cli(cli_func, cli_kwargs):
    """Call a CLI command with run_job_pipeline mocked, return the pipeline kwargs."""
    with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
        cli_func(**cli_kwargs)
    return mock_pipeline.call_args.kwargs


# Each entry: (cli_func, cli_kwargs, task_name) → (expanded_command, env_var_subset).
# env_var_subset verifies CLI kwargs flow through to factory output correctly;
# complete env var coverage is in test_job_factories.py.
CLI_EXPANSION_CASES: dict[str, dict] = {
    "ror-download": {
        "cli_func": ror_cmd,
        "cli_kwargs": {
            "env": "dev",
            "run_id": "test-run",
            "bucket_name": "my-bucket",
            "download_url": "https://zenodo.org/ror.zip",
            "hash": "abc123",
        },
        "task_name": "download",
        "expanded_command": "dmpworks aws-batch ror download my-bucket test-run https://zenodo.org/ror.zip --file-hash abc123",
        "expected_env_subset": {
            "BUCKET_NAME": "my-bucket",
            "RUN_ID": "test-run",
            "DOWNLOAD_URL": "https://zenodo.org/ror.zip",
            "FILE_HASH": "abc123",
        },
    },
    "crossref-metadata-transform": {
        "cli_func": crossref_metadata_cmd,
        "cli_kwargs": {
            "env": "dev",
            "run_id": "test-run",
            "bucket_name": "my-bucket",
            "file_name": "crossref.tar.gz",
            "crossref_metadata_bucket_name": "crossref-bucket",
        },
        "task_name": "transform",
        "expanded_command": "dmpworks aws-batch crossref-metadata transform my-bucket test-run --use-subset=false --log-level=INFO",
        "expected_env_subset": {
            "BUCKET_NAME": "my-bucket",
            "RUN_ID": "test-run",
            "USE_SUBSET": "false",
        },
    },
    "process-works-sync-works": {
        "cli_func": process_works_cmd,
        "cli_kwargs": {
            "env": "dev",
            "bucket_name": "my-bucket",
            "run_identifiers": RunIdentifiers(
                run_id_sqlmesh="sm-run",
                release_date_process_works="2026-03-29",
            ),
            "sqlmesh_config": SQLMeshConfig(),
            "works_index_name": "my-works-index",
        },
        "task_name": "sync-works",
        "expanded_command": "dmpworks aws-batch opensearch sync-works my-bucket my-works-index",
        "expected_env_subset": {
            "BUCKET_NAME": "my-bucket",
            "INDEX_NAME": "my-works-index",
            "RUN_ID_SQLMESH": "sm-run",
            "RELEASE_DATE_PROCESS_WORKS": "2026-03-29",
        },
    },
    "process-dmps-dmp-works-search": {
        "cli_func": process_dmps_cmd,
        "cli_kwargs": {
            "env": "dev",
            "bucket_name": "my-bucket",
            "run_id_dmps": "dmps-run-1",
            "dmps_index_name": "my-dmps-index",
            "works_index_name": "my-works-index",
        },
        "task_name": "dmp-works-search",
        "expanded_command": "dmpworks aws-batch opensearch dmp-works-search my-bucket dmps-run-1 my-dmps-index my-works-index",
        "expected_env_subset": {
            "BUCKET_NAME": "my-bucket",
            "RUN_ID": "dmps-run-1",
            "DMPS_INDEX_NAME": "my-dmps-index",
            "WORKS_INDEX_NAME": "my-works-index",
        },
    },
    "process-dmps-sync-dmps": {
        "cli_func": process_dmps_cmd,
        "cli_kwargs": {
            "env": "dev",
            "bucket_name": "my-bucket",
            "run_id_dmps": "dmps-run-1",
            "dmps_index_name": "custom-dmps",
        },
        "task_name": "sync-dmps",
        "expanded_command": "dmpworks aws-batch opensearch sync-dmps my-bucket custom-dmps",
        "expected_env_subset": {
            "INDEX_NAME": "custom-dmps",
        },
    },
    "process-dmps-merge-related-works": {
        "cli_func": process_dmps_cmd,
        "cli_kwargs": {
            "env": "dev",
            "bucket_name": "my-bucket",
            "run_id_dmps": "dmps-run-1",
        },
        "task_name": "merge-related-works",
        "expanded_command": "dmpworks aws-batch opensearch merge-related-works my-bucket dmps-run-1 dmps-run-1",
        "expected_env_subset": {
            "SEARCH_RUN_ID": "dmps-run-1",
        },
    },
}


class TestCliExpansion:
    """Verify CLI kwargs flow through partial -> factory -> expanded command + env vars."""

    @pytest.mark.parametrize("case_id", CLI_EXPANSION_CASES.keys())
    def test_expanded_command_and_env(self, case_id):
        case = CLI_EXPANSION_CASES[case_id]
        pipeline_kwargs = invoke_cli(case["cli_func"], case["cli_kwargs"])
        params = call_task(pipeline_kwargs["task_definitions"], case["task_name"])

        assert expand_command(params) == case["expanded_command"]
        env = get_env_dict(params)
        for var, expected in case["expected_env_subset"].items():
            assert env[var] == expected, f"{var}: {env.get(var)!r} != {expected!r}"


class TestRorCmd:
    def test_task_order_and_definitions(self):
        kwargs = invoke_cli(
            ror_cmd,
            {
                "env": "dev",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "download_url": "https://zenodo.org/ror.zip",
                "hash": "abc123",
            },
        )

        assert kwargs["task_order"] == list(ROR_JOBS)
        assert kwargs["start_task_name"] == "download"
        assert set(kwargs["task_definitions"].keys()) == {"download"}
        assert kwargs["task_definitions"]["download"].func is submit_factory_job

    def test_custom_start_job(self):
        kwargs = invoke_cli(
            ror_cmd,
            {
                "env": "dev",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "download_url": "https://zenodo.org/ror.zip",
                "hash": "abc123",
                "start_job": "download",
            },
        )
        assert kwargs["start_task_name"] == "download"


class TestCrossrefMetadataCmd:
    def test_no_subset_excludes_dataset_subset_task(self):
        kwargs = invoke_cli(
            crossref_metadata_cmd,
            {
                "env": "dev",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "file_name": "crossref.tar.gz",
                "crossref_metadata_bucket_name": "crossref-bucket",
            },
        )

        assert kwargs["task_order"] == ["download", "transform"]
        assert "subset" not in kwargs["task_order"]

    def test_with_subset_includes_dataset_subset_task(self):
        kwargs = invoke_cli(
            crossref_metadata_cmd,
            {
                "env": "dev",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "file_name": "crossref.tar.gz",
                "crossref_metadata_bucket_name": "crossref-bucket",
                "dataset_subset": DatasetSubsetAWS(enable=True, institutions_s3_path="path/institutions.csv"),
            },
        )

        assert kwargs["task_order"] == list(CROSSREF_METADATA_JOBS)
        assert set(kwargs["task_definitions"].keys()) == {"download", "subset", "transform"}

    def test_disabled_dataset_subset_treated_as_no_subset(self):
        kwargs = invoke_cli(
            crossref_metadata_cmd,
            {
                "env": "dev",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "file_name": "crossref.tar.gz",
                "crossref_metadata_bucket_name": "crossref-bucket",
                "dataset_subset": DatasetSubsetAWS(enable=False),
            },
        )

        assert "subset" not in kwargs["task_order"]

    def test_start_job_passed_through(self):
        kwargs = invoke_cli(
            crossref_metadata_cmd,
            {
                "env": "dev",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "file_name": "crossref.tar.gz",
                "crossref_metadata_bucket_name": "crossref-bucket",
                "start_job": "transform",
            },
        )
        assert kwargs["start_task_name"] == "transform"


class TestDataCiteCmd:
    def test_no_subset_excludes_dataset_subset_task(self):
        kwargs = invoke_cli(
            datacite_cmd,
            {
                "env": "prod",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "datacite_bucket_name": "datacite-bucket",
            },
        )

        assert kwargs["task_order"] == ["download", "transform"]
        assert set(kwargs["task_definitions"].keys()) == {"download", "transform"}

    def test_with_subset_includes_dataset_subset_task(self):
        kwargs = invoke_cli(
            datacite_cmd,
            {
                "env": "prod",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "datacite_bucket_name": "datacite-bucket",
                "dataset_subset": DatasetSubsetAWS(enable=True),
            },
        )

        assert kwargs["task_order"] == list(DATACITE_JOBS)
        assert "subset" in kwargs["task_definitions"]

    def test_start_job_passed_through(self):
        kwargs = invoke_cli(
            datacite_cmd,
            {
                "env": "prod",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "datacite_bucket_name": "datacite-bucket",
                "start_job": "transform",
            },
        )
        assert kwargs["start_task_name"] == "transform"


class TestOpenAlexWorksCmd:
    def test_no_subset_excludes_dataset_subset_task(self):
        kwargs = invoke_cli(
            openalex_works_cmd,
            {
                "env": "stage",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "openalex_bucket_name": "openalex-bucket",
            },
        )

        assert kwargs["task_order"] == ["download", "transform"]
        assert set(kwargs["task_definitions"].keys()) == {"download", "transform"}

    def test_with_subset_includes_dataset_subset_task(self):
        kwargs = invoke_cli(
            openalex_works_cmd,
            {
                "env": "stage",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "openalex_bucket_name": "openalex-bucket",
                "dataset_subset": DatasetSubsetAWS(enable=True, dois_s3_path="path/dois.csv"),
            },
        )

        assert kwargs["task_order"] == list(OPENALEX_WORKS_JOBS)
        assert "subset" in kwargs["task_definitions"]

    def test_start_job_passed_through(self):
        kwargs = invoke_cli(
            openalex_works_cmd,
            {
                "env": "stage",
                "run_id": "test-run",
                "bucket_name": "my-bucket",
                "openalex_bucket_name": "openalex-bucket",
                "start_job": "transform",
            },
        )
        assert kwargs["start_task_name"] == "transform"


class TestProcessWorksCmd:
    def test_task_order_and_definitions(self):
        kwargs = invoke_cli(
            process_works_cmd,
            {
                "env": "dev",
                "bucket_name": "my-bucket",
                "run_identifiers": RunIdentifiers(run_id_sqlmesh="works-run-1"),
                "sqlmesh_config": SQLMeshConfig(),
            },
        )

        assert kwargs["task_order"] == list(PROCESS_WORKS_JOBS)
        assert kwargs["start_task_name"] == PROCESS_WORKS_JOBS[0]
        assert set(kwargs["task_definitions"].keys()) == {"sqlmesh-transform", "sync-works"}

    def test_start_job_passed_through(self):
        kwargs = invoke_cli(
            process_works_cmd,
            {
                "env": "dev",
                "bucket_name": "my-bucket",
                "run_identifiers": RunIdentifiers(run_id_sqlmesh="works-run-1"),
                "sqlmesh_config": SQLMeshConfig(),
                "start_job": "sync-works",
            },
        )
        assert kwargs["start_task_name"] == "sync-works"


class TestProcessDmpsCmd:
    def test_task_order_and_definitions(self):
        kwargs = invoke_cli(
            process_dmps_cmd,
            {
                "env": "dev",
                "bucket_name": "my-bucket",
                "run_id_dmps": "dmps-run-1",
            },
        )

        assert kwargs["task_order"] == list(PROCESS_DMPS_JOBS)
        assert kwargs["start_task_name"] == PROCESS_DMPS_JOBS[0]
        assert set(kwargs["task_definitions"].keys()) == {
            "sync-dmps",
            "enrich-dmps",
            "dmp-works-search",
            "merge-related-works",
        }

    def test_start_job_passed_through(self):
        kwargs = invoke_cli(
            process_dmps_cmd,
            {
                "env": "dev",
                "bucket_name": "my-bucket",
                "run_id_dmps": "dmps-run-1",
                "start_job": "enrich-dmps",
            },
        )
        assert kwargs["start_task_name"] == "enrich-dmps"
