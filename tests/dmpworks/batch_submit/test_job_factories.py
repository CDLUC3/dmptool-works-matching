"""Tests for job factory functions, build_env_list, and build_batch_params."""

from __future__ import annotations

import pytest

from tests.dmpworks.batch_submit.conftest import expand_command, get_env_dict

from dmpworks.batch_submit.job_factories import (
    DMPS_INDEX_NAME,
    DOWNLOAD_QUEUE_MEMORY,
    DOWNLOAD_QUEUE_VCPUS,
    JOB_FACTORIES,
    OPENSEARCH_QUEUE_MEMORY,
    OPENSEARCH_QUEUE_VCPUS,
    SMALL_QUEUE_MEMORY,
    SMALL_QUEUE_VCPUS,
    SQLMESH_QUEUE_MEMORY,
    SQLMESH_QUEUE_VCPUS,
    TRANSFORM_QUEUE_MEMORY,
    TRANSFORM_QUEUE_VCPUS,
    WORKS_INDEX_NAME,
    build_batch_params,
    build_env_list,
)

# Minimal kwargs for each factory — mirrors what compute_batch_params (Lambda path) would pass.
MINIMAL_FACTORY_ARGS: dict[tuple[str, str], dict] = {
    ("ror", "download"): {
        "run_id": "r",
        "bucket_name": "b",
        "download_url": "http://x",
        "file_hash": "h",
        "env": "dev",
    },
    ("data-citation-corpus", "download"): {
        "run_id": "r",
        "bucket_name": "b",
        "download_url": "http://x",
        "file_hash": "h",
        "env": "dev",
    },
    ("openalex-works", "download"): {"run_id": "r", "bucket_name": "b", "openalex_bucket_name": "oa", "env": "dev",},
    ("crossref-metadata", "download"): {
        "run_id": "r",
        "bucket_name": "b",
        "file_name": "f.tar.gz",
        "crossref_metadata_bucket_name": "cb",
        "env": "dev",
    },
    ("datacite", "download"): {"run_id": "r", "bucket_name": "b", "datacite_bucket_name": "db", "env": "dev",},
    ("openalex-works", "subset"): {"run_id": "r", "bucket_name": "b", "env": "dev", "dataset": "openalex-works",},
    ("crossref-metadata", "subset"): {"run_id": "r", "bucket_name": "b", "env": "dev", "dataset": "crossref-metadata",},
    ("datacite", "subset"): {"run_id": "r", "bucket_name": "b", "env": "dev", "dataset": "datacite",},
    ("openalex-works", "transform"): {
        "run_id": "r",
        "bucket_name": "b",
        "env": "dev",
        "openalex_works_transform_batch_size": 8,
        "openalex_works_transform_row_group_size": 100_000,
        "openalex_works_transform_row_groups_per_file": 2,
        "openalex_works_transform_max_workers": 4,
        "openalex_works_transform_include_xpac": False,
    },
    ("crossref-metadata", "transform"): {
        "run_id": "r",
        "bucket_name": "b",
        "env": "dev",
        "crossref_metadata_transform_batch_size": 8,
        "crossref_metadata_transform_row_group_size": 100_000,
        "crossref_metadata_transform_row_groups_per_file": 2,
        "crossref_metadata_transform_max_workers": 4,
    },
    ("datacite", "transform"): {
        "run_id": "r",
        "bucket_name": "b",
        "env": "dev",
        "datacite_transform_batch_size": 8,
        "datacite_transform_row_group_size": 100_000,
        "datacite_transform_row_groups_per_file": 2,
        "datacite_transform_max_workers": 4,
    },
    ("process-works", "sqlmesh"): {
        "run_id": "r",
        "env": "dev",
        "bucket_name": "b",
        "release_date": "2026-03-29",
        "run_id_sqlmesh_prev": "prev",
        "run_id_openalex_works": "oa",
        "run_id_datacite": "dc",
        "run_id_crossref_metadata": "cr",
        "run_id_ror": "ror",
        "run_id_data_citation_corpus": "dcc",
    },
    ("process-works", "sync-works"): {
        "run_id": "r",
        "env": "dev",
        "bucket_name": "b",
        "release_date": "2026-03-29",
        "sqlmesh_run_id": "sm",
    },
    ("process-dmps", "sync-dmps"): {"run_id": "r", "env": "dev", "bucket_name": "b",},
    ("process-dmps", "enrich-dmps"): {"run_id": "r", "env": "dev", "bucket_name": "b",},
    ("process-dmps", "dmp-works-search"): {"run_id": "r", "env": "dev", "bucket_name": "b",},
    ("process-dmps", "merge-related-works"): {
        "run_id": "r",
        "env": "dev",
        "bucket_name": "b",
        "search_run_id": "sr",
    },
}

# Complete expected output for each factory given MINIMAL_FACTORY_ARGS.
# expanded_command is verified via real bash expansion in an isolated env.
# env_vars is an exact 1-1 match — any added, removed, or changed var fails.
FACTORY_EXPECTATIONS: dict[tuple[str, str], dict] = {
    ("ror", "download"): {
        "run_name": "ror-download",
        "job_queue": "dmpworks-dev-batch-small-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": SMALL_QUEUE_VCPUS,
        "memory": SMALL_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch ror download b r http://x --file-hash h",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "DOWNLOAD_URL": "http://x",
            "FILE_HASH": "h",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("data-citation-corpus", "download"): {
        "run_name": "data-citation-corpus-download",
        "job_queue": "dmpworks-dev-batch-small-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": SMALL_QUEUE_VCPUS,
        "memory": SMALL_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch data-citation-corpus download b r http://x --file-hash h",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "DOWNLOAD_URL": "http://x",
            "FILE_HASH": "h",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("openalex-works", "download"): {
        "run_name": "openalex-works-download",
        "job_queue": "dmpworks-dev-batch-download-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": DOWNLOAD_QUEUE_VCPUS,
        "memory": DOWNLOAD_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch openalex-works download b r oa",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "OPENALEX_BUCKET_NAME": "oa",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("crossref-metadata", "download"): {
        "run_name": "crossref-metadata-download",
        "job_queue": "dmpworks-dev-batch-download-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": DOWNLOAD_QUEUE_VCPUS,
        "memory": DOWNLOAD_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch crossref-metadata download b r f.tar.gz cb",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "FILE_NAME": "f.tar.gz",
            "CROSSREF_METADATA_BUCKET_NAME": "cb",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("datacite", "download"): {
        "run_name": "datacite-download",
        "job_queue": "dmpworks-dev-batch-download-job-queue",
        "job_definition": "dmpworks-dev-datacite-download-job",
        "vcpus": DOWNLOAD_QUEUE_VCPUS,
        "memory": DOWNLOAD_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch datacite download b r db",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "DATACITE_BUCKET_NAME": "db",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("openalex-works", "subset"): {
        "run_name": "openalex-works-subset",
        "job_queue": "dmpworks-dev-batch-transform-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": TRANSFORM_QUEUE_VCPUS,
        "memory": TRANSFORM_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch openalex-works subset b r",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "DATASET": "openalex-works",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("crossref-metadata", "subset"): {
        "run_name": "crossref-metadata-subset",
        "job_queue": "dmpworks-dev-batch-transform-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": TRANSFORM_QUEUE_VCPUS,
        "memory": TRANSFORM_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch crossref-metadata subset b r",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "DATASET": "crossref-metadata",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("datacite", "subset"): {
        "run_name": "datacite-subset",
        "job_queue": "dmpworks-dev-batch-transform-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": TRANSFORM_QUEUE_VCPUS,
        "memory": TRANSFORM_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch datacite subset b r",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "DATASET": "datacite",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("openalex-works", "transform"): {
        "run_name": "openalex-works-transform",
        "job_queue": "dmpworks-dev-batch-transform-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": TRANSFORM_QUEUE_VCPUS,
        "memory": TRANSFORM_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch openalex-works transform b r --use-subset=false --log-level=INFO",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "USE_SUBSET": "false",
            "LOG_LEVEL": "INFO",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
            "OPENALEX_WORKS_TRANSFORM_BATCH_SIZE": "8",
            "OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE": "100000",
            "OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE": "2",
            "OPENALEX_WORKS_TRANSFORM_MAX_WORKERS": "4",
            "OPENALEX_WORKS_TRANSFORM_INCLUDE_XPAC": "false",
        },
    },
    ("crossref-metadata", "transform"): {
        "run_name": "crossref-metadata-transform",
        "job_queue": "dmpworks-dev-batch-transform-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": TRANSFORM_QUEUE_VCPUS,
        "memory": TRANSFORM_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch crossref-metadata transform b r --use-subset=false --log-level=INFO",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "USE_SUBSET": "false",
            "LOG_LEVEL": "INFO",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
            "CROSSREF_METADATA_TRANSFORM_BATCH_SIZE": "8",
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE": "100000",
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE": "2",
            "CROSSREF_METADATA_TRANSFORM_MAX_WORKERS": "4",
        },
    },
    ("datacite", "transform"): {
        "run_name": "datacite-transform",
        "job_queue": "dmpworks-dev-batch-transform-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": TRANSFORM_QUEUE_VCPUS,
        "memory": TRANSFORM_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch datacite transform b r --use-subset=false --log-level=INFO",
        "env_vars": {
            "RUN_ID": "r",
            "BUCKET_NAME": "b",
            "USE_SUBSET": "false",
            "LOG_LEVEL": "INFO",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
            "DATACITE_TRANSFORM_BATCH_SIZE": "8",
            "DATACITE_TRANSFORM_ROW_GROUP_SIZE": "100000",
            "DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE": "2",
            "DATACITE_TRANSFORM_MAX_WORKERS": "4",
        },
    },
    ("process-works", "sqlmesh"): {
        "run_name": "process-works-sqlmesh",
        "job_queue": "dmpworks-dev-batch-sqlmesh-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": SQLMESH_QUEUE_VCPUS,
        "memory": SQLMESH_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch sqlmesh plan b",
        "env_vars": {
            "RUN_ID_SQLMESH": "r",
            "RELEASE_DATE_PROCESS_WORKS": "2026-03-29",
            "RUN_ID_SQLMESH_PREV": "prev",
            "RUN_ID_OPENALEX_WORKS": "oa",
            "RUN_ID_DATACITE": "dc",
            "RUN_ID_CROSSREF_METADATA": "cr",
            "RUN_ID_ROR": "ror",
            "RUN_ID_DATA_CITATION_CORPUS": "dcc",
            "BUCKET_NAME": "b",
        },
    },
    ("process-works", "sync-works"): {
        "run_name": "process-works-sync-works",
        "job_queue": "dmpworks-dev-batch-opensearch-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": OPENSEARCH_QUEUE_VCPUS,
        "memory": OPENSEARCH_QUEUE_MEMORY,
        "expanded_command": f"dmpworks aws-batch opensearch sync-works b {WORKS_INDEX_NAME}",
        "env_vars": {
            "RELEASE_DATE_PROCESS_WORKS": "2026-03-29",
            "RUN_ID_SQLMESH": "sm",
            "BUCKET_NAME": "b",
            "INDEX_NAME": WORKS_INDEX_NAME,
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("process-dmps", "sync-dmps"): {
        "run_name": "process-dmps-sync-dmps",
        "job_queue": "dmpworks-dev-batch-opensearch-job-queue",
        "job_definition": "dmpworks-dev-database-job",
        "vcpus": OPENSEARCH_QUEUE_VCPUS,
        "memory": OPENSEARCH_QUEUE_MEMORY,
        "expanded_command": f"dmpworks aws-batch opensearch sync-dmps b {DMPS_INDEX_NAME}",
        "env_vars": {
            "BUCKET_NAME": "b",
            "INDEX_NAME": DMPS_INDEX_NAME,
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("process-dmps", "enrich-dmps"): {
        "run_name": "process-dmps-enrich-dmps",
        "job_queue": "dmpworks-dev-batch-opensearch-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": OPENSEARCH_QUEUE_VCPUS,
        "memory": OPENSEARCH_QUEUE_MEMORY,
        "expanded_command": f"dmpworks aws-batch opensearch enrich-dmps {DMPS_INDEX_NAME} --bucket-name b",
        "env_vars": {
            "BUCKET_NAME": "b",
            "INDEX_NAME": DMPS_INDEX_NAME,
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("process-dmps", "dmp-works-search"): {
        "run_name": "process-dmps-dmp-works-search",
        "job_queue": "dmpworks-dev-batch-opensearch-job-queue",
        "job_definition": "dmpworks-dev-job",
        "vcpus": OPENSEARCH_QUEUE_VCPUS,
        "memory": OPENSEARCH_QUEUE_MEMORY,
        "expanded_command": (
            f"dmpworks aws-batch opensearch dmp-works-search b r {DMPS_INDEX_NAME} {WORKS_INDEX_NAME}"
        ),
        "env_vars": {
            "BUCKET_NAME": "b",
            "RUN_ID": "r",
            "DMPS_INDEX_NAME": DMPS_INDEX_NAME,
            "WORKS_INDEX_NAME": WORKS_INDEX_NAME,
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
    ("process-dmps", "merge-related-works"): {
        "run_name": "process-dmps-merge-related-works",
        "job_queue": "dmpworks-dev-batch-opensearch-job-queue",
        "job_definition": "dmpworks-dev-database-job",
        "vcpus": OPENSEARCH_QUEUE_VCPUS,
        "memory": OPENSEARCH_QUEUE_MEMORY,
        "expanded_command": "dmpworks aws-batch opensearch merge-related-works b r sr",
        "env_vars": {
            "BUCKET_NAME": "b",
            "RUN_ID": "r",
            "SEARCH_RUN_ID": "sr",
            "TQDM_POSITION": "-1",
            "TQDM_MININTERVAL": "120",
        },
    },
}


class TestBuildEnvList:
    def test_filters_none(self):
        result = build_env_list({"PRESENT": "yes", "ABSENT": None})
        names = [e["Name"] for e in result]
        assert "PRESENT" in names
        assert "ABSENT" not in names

    def test_converts_bool_true_to_lowercase(self):
        result = build_env_list({"FLAG": True})
        assert result[0]["Value"] == "true"

    def test_converts_bool_false_to_lowercase(self):
        result = build_env_list({"FLAG": False})
        assert result[0]["Value"] == "false"

    def test_uses_pascal_case_keys(self):
        result = build_env_list({"MY_VAR": "x"})
        assert result[0]["Name"] == "MY_VAR"
        assert result[0]["Value"] == "x"

    def test_non_string_values_converted_to_string(self):
        result = build_env_list({"NUM": 42})
        assert result[0]["Value"] == "42"


class TestBuildBatchParams:
    def test_returns_expected_structure(self):
        result = build_batch_params(
            run_name="test-job",
            env="dev",
            queue=lambda env: f"queue-{env}",
            job_definition=lambda env: f"jobdef-{env}",
            vcpus=4,
            memory=8192,
            command="echo hello",
            env_vars={"FOO": "bar"},
        )
        assert result["run_name"] == "test-job"
        assert result["JobQueue"] == "queue-dev"
        assert result["JobDefinition"] == "jobdef-dev"
        overrides = result["ContainerOverrides"]
        assert overrides["Command"] == ["/bin/bash", "-c", "echo hello"]
        assert overrides["Vcpus"] == 4
        assert overrides["Memory"] == 8192
        assert overrides["Environment"] == [{"Name": "FOO", "Value": "bar"}]

    def test_wraps_command_in_bash(self):
        result = build_batch_params(
            run_name="x",
            env="dev",
            queue=lambda e: "q",
            job_definition=lambda e: "j",
            vcpus=1,
            memory=1,
            command="dmpworks aws-batch ror download",
            env_vars={},
        )
        assert result["ContainerOverrides"]["Command"] == ["/bin/bash", "-c", "dmpworks aws-batch ror download"]

    def test_env_vars_passed_through_build_env_list(self):
        result = build_batch_params(
            run_name="x",
            env="dev",
            queue=lambda e: "q",
            job_definition=lambda e: "j",
            vcpus=1,
            memory=1,
            command="cmd",
            env_vars={"PRESENT": "yes", "ABSENT": None, "FLAG": True},
        )
        env = result["ContainerOverrides"]["Environment"]
        names = [e["Name"] for e in env]
        assert "PRESENT" in names
        assert "ABSENT" not in names
        flag_val = next(e["Value"] for e in env if e["Name"] == "FLAG")
        assert flag_val == "true"


class TestFactoryOutput:
    """Test factory output: batch params, env vars, and expanded commands (Lambda path)."""

    def test_all_factory_keys_have_expectations(self):
        assert set(MINIMAL_FACTORY_ARGS.keys()) == set(FACTORY_EXPECTATIONS.keys())

    def test_all_factory_keys_exist_in_registry(self):
        for key in MINIMAL_FACTORY_ARGS:
            assert key in JOB_FACTORIES, f"{key} not found in JOB_FACTORIES"

    @pytest.mark.parametrize(
        "key",
        FACTORY_EXPECTATIONS.keys(),
        ids=[f"{wf}-{task}" for wf, task in FACTORY_EXPECTATIONS],
    )
    def test_batch_params_and_env_vars(self, key):
        """Factory produces correct run_name, queue, definition, resources, and env vars."""
        expected = FACTORY_EXPECTATIONS[key]
        params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])

        assert params["run_name"] == expected["run_name"]
        assert params["JobQueue"] == expected["job_queue"]
        assert params["JobDefinition"] == expected["job_definition"]
        assert params["ContainerOverrides"]["Vcpus"] == expected["vcpus"]
        assert params["ContainerOverrides"]["Memory"] == expected["memory"]
        assert get_env_dict(params) == expected["env_vars"]

    @pytest.mark.parametrize(
        "key",
        FACTORY_EXPECTATIONS.keys(),
        ids=[f"{wf}-{task}" for wf, task in FACTORY_EXPECTATIONS],
    )
    def test_expanded_command(self, key):
        """Command after bash $VAR expansion in isolated env matches expected string."""
        params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
        expanded = expand_command(params)
        assert expanded == FACTORY_EXPECTATIONS[key]["expanded_command"]

    def test_none_prev_job_run_id_filtered_from_env(self):
        params = JOB_FACTORIES[("openalex-works", "transform")](
            **MINIMAL_FACTORY_ARGS[("openalex-works", "transform")], prev_job_run_id=None
        )
        env_names = [e["Name"] for e in params["ContainerOverrides"]["Environment"]]
        assert "PREV_JOB_RUN_ID" not in env_names
