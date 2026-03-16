"""Tests for build_env_list and JOB_FACTORIES factory output."""

from __future__ import annotations

import pytest

from dmpworks.batch_submit.job_registry import (
    JOB_FACTORIES,
    DOWNLOAD_QUEUE_MEMORY,
    DOWNLOAD_QUEUE_VCPUS,
    OPENSEARCH_QUEUE_MEMORY,
    OPENSEARCH_QUEUE_VCPUS,
    SMALL_QUEUE_MEMORY,
    SMALL_QUEUE_VCPUS,
    SQLMESH_QUEUE_MEMORY,
    SQLMESH_QUEUE_VCPUS,
    TRANSFORM_QUEUE_MEMORY,
    TRANSFORM_QUEUE_VCPUS,
    build_batch_params,
    build_env_list,
    database_job_definition,
    datacite_download_job_definition,
    standard_job_definition,
)

MINIMAL_FACTORY_ARGS = {
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
    ("openalex-works", "download"): {"run_id": "r", "bucket_name": "b", "openalex_bucket_name": "oa", "env": "dev"},
    ("crossref-metadata", "download"): {
        "run_id": "r",
        "bucket_name": "b",
        "file_name": "f.tar.gz",
        "crossref_metadata_bucket_name": "cb",
        "env": "dev",
    },
    ("datacite", "download"): {"run_id": "r", "bucket_name": "b", "datacite_bucket_name": "db", "env": "dev"},
    ("openalex-works", "subset"): {"run_id": "r", "bucket_name": "b", "env": "dev", "dataset": "openalex-works"},
    ("crossref-metadata", "subset"): {"run_id": "r", "bucket_name": "b", "env": "dev", "dataset": "crossref-metadata"},
    ("datacite", "subset"): {"run_id": "r", "bucket_name": "b", "env": "dev", "dataset": "datacite"},
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
        "sqlmesh_run_id": "sm",
    },
    ("process-dmps", "sync-dmps"): {"run_id": "r", "env": "dev", "bucket_name": "b"},
    ("process-dmps", "enrich-dmps"): {"run_id": "r", "env": "dev", "bucket_name": "b"},
    ("process-dmps", "dmp-works-search"): {"run_id": "r", "env": "dev", "bucket_name": "b"},
    ("process-dmps", "merge-related-works"): {
        "run_id": "r",
        "env": "dev",
        "bucket_name": "b",
        "search_run_id": "sr",
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


class TestFactoryOutput:
    def test_all_factory_keys_exist(self):
        for key in MINIMAL_FACTORY_ARGS:
            assert key in JOB_FACTORIES, f"{key} not found in JOB_FACTORIES"

    def test_output_contains_run_name_key(self):
        for key, args in MINIMAL_FACTORY_ARGS.items():
            params = JOB_FACTORIES[key](**args)
            assert "run_name" in params, f"{key} factory did not return run_name"

    def test_output_schema_structure(self):
        for key, args in MINIMAL_FACTORY_ARGS.items():
            params = JOB_FACTORIES[key](**args)
            assert "JobQueue" in params
            assert "JobDefinition" in params
            overrides = params["ContainerOverrides"]
            assert "Command" in overrides
            assert "Vcpus" in overrides
            assert "Memory" in overrides
            assert "Environment" in overrides

    def test_env_items_are_pascal_case(self):
        params = JOB_FACTORIES[("ror", "download")](**MINIMAL_FACTORY_ARGS[("ror", "download")])
        for item in params["ContainerOverrides"]["Environment"]:
            assert "Name" in item
            assert "Value" in item
            assert "name" not in item

    def test_ror_download_maps_file_hash_to_file_hash_env_var(self):
        params = JOB_FACTORIES[("ror", "download")](
            run_id="r", bucket_name="b", download_url="http://x", file_hash="abc123", env="dev"
        )
        env_dict = {e["Name"]: e["Value"] for e in params["ContainerOverrides"]["Environment"]}
        assert env_dict["FILE_HASH"] == "abc123"
        assert "HASH" not in env_dict

    def test_datacite_download_uses_special_job_definition(self):
        params = JOB_FACTORIES[("datacite", "download")](**MINIMAL_FACTORY_ARGS[("datacite", "download")])
        assert params["JobDefinition"] == datacite_download_job_definition("dev")
        assert params["JobDefinition"] != standard_job_definition("dev")

    def test_all_others_use_standard_job_definition(self):
        for key in [
            ("ror", "download"),
            ("data-citation-corpus", "download"),
            ("openalex-works", "download"),
            ("crossref-metadata", "download"),
            ("openalex-works", "subset"),
            ("crossref-metadata", "subset"),
            ("datacite", "subset"),
            ("openalex-works", "transform"),
            ("crossref-metadata", "transform"),
            ("datacite", "transform"),
        ]:
            params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
            assert params["JobDefinition"] == standard_job_definition(
                "dev"
            ), f"{key} should use standard job definition"

    def test_ror_dcc_use_small_queue_resources(self):
        for key in [("ror", "download"), ("data-citation-corpus", "download")]:
            params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
            assert params["ContainerOverrides"]["Vcpus"] == SMALL_QUEUE_VCPUS
            assert params["ContainerOverrides"]["Memory"] == SMALL_QUEUE_MEMORY

    def test_download_stages_use_download_queue_resources(self):
        for key in [
            ("openalex-works", "download"),
            ("crossref-metadata", "download"),
            ("datacite", "download"),
        ]:
            params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
            assert params["ContainerOverrides"]["Vcpus"] == DOWNLOAD_QUEUE_VCPUS
            assert params["ContainerOverrides"]["Memory"] == DOWNLOAD_QUEUE_MEMORY

    def test_transform_stages_use_transform_queue_resources(self):
        for key in [
            ("openalex-works", "subset"),
            ("openalex-works", "transform"),
            ("crossref-metadata", "subset"),
            ("crossref-metadata", "transform"),
            ("datacite", "subset"),
            ("datacite", "transform"),
        ]:
            params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
            assert params["ContainerOverrides"]["Vcpus"] == TRANSFORM_QUEUE_VCPUS
            assert params["ContainerOverrides"]["Memory"] == TRANSFORM_QUEUE_MEMORY

    @pytest.mark.parametrize(
        "key,expected_run_name",
        [
            (("ror", "download"), "ror-download"),
            (("data-citation-corpus", "download"), "data-citation-corpus-download"),
            (("openalex-works", "download"), "openalex-works-download"),
            (("crossref-metadata", "download"), "crossref-metadata-download"),
            (("datacite", "download"), "datacite-download"),
            (("openalex-works", "subset"), "openalex-works-dataset-subset"),
            (("crossref-metadata", "subset"), "crossref-metadata-dataset-subset"),
            (("datacite", "subset"), "datacite-dataset-subset"),
            (("openalex-works", "transform"), "openalex-works-transform"),
            (("crossref-metadata", "transform"), "crossref-metadata-transform"),
            (("datacite", "transform"), "datacite-transform"),
            (("process-works", "sqlmesh"), "process-works-sqlmesh"),
            (("process-works", "sync-works"), "process-works-sync-works"),
            (("process-dmps", "sync-dmps"), "process-dmps-sync-dmps"),
            (("process-dmps", "enrich-dmps"), "process-dmps-enrich-dmps"),
            (("process-dmps", "dmp-works-search"), "process-dmps-dmp-works-search"),
            (("process-dmps", "merge-related-works"), "process-dmps-merge-related-works"),
        ],
    )
    def test_run_names(self, key, expected_run_name):
        params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
        assert params["run_name"] == expected_run_name

    def test_subset_factories_inject_dataset_env_var(self):
        for key in [("openalex-works", "subset"), ("crossref-metadata", "subset"), ("datacite", "subset")]:
            params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
            env_dict = {e["Name"]: e["Value"] for e in params["ContainerOverrides"]["Environment"]}
            assert "DATASET" in env_dict

    def test_none_prev_job_run_id_filtered_from_env(self):
        params = JOB_FACTORIES[("openalex-works", "transform")](
            **MINIMAL_FACTORY_ARGS[("openalex-works", "transform")], prev_job_run_id=None
        )
        env_names = [e["Name"] for e in params["ContainerOverrides"]["Environment"]]
        assert "PREV_JOB_RUN_ID" not in env_names

    def test_sqlmesh_uses_sqlmesh_queue_resources(self):
        params = JOB_FACTORIES[("process-works", "sqlmesh")](**MINIMAL_FACTORY_ARGS[("process-works", "sqlmesh")])
        assert params["ContainerOverrides"]["Vcpus"] == SQLMESH_QUEUE_VCPUS
        assert params["ContainerOverrides"]["Memory"] == SQLMESH_QUEUE_MEMORY

    def test_opensearch_stages_use_opensearch_queue_resources(self):
        for key in [
            ("process-works", "sync-works"),
            ("process-dmps", "sync-dmps"),
            ("process-dmps", "enrich-dmps"),
            ("process-dmps", "dmp-works-search"),
            ("process-dmps", "merge-related-works"),
        ]:
            params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
            assert params["ContainerOverrides"]["Vcpus"] == OPENSEARCH_QUEUE_VCPUS
            assert params["ContainerOverrides"]["Memory"] == OPENSEARCH_QUEUE_MEMORY

    def test_database_job_definition_stages(self):
        for key in [("process-dmps", "sync-dmps"), ("process-dmps", "merge-related-works")]:
            params = JOB_FACTORIES[key](**MINIMAL_FACTORY_ARGS[key])
            assert params["JobDefinition"] == database_job_definition("dev")


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
