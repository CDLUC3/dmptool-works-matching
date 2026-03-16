from unittest.mock import MagicMock

import pytest

from dmpworks.batch_submit.job_registry import (
    JOB_FACTORIES,
    DOWNLOAD_QUEUE_MEMORY,
    DOWNLOAD_QUEUE_VCPUS,
    SMALL_QUEUE_MEMORY,
    SMALL_QUEUE_VCPUS,
    TQDM_MININTERVAL,
    TQDM_POSITION,
    TRANSFORM_QUEUE_MEMORY,
    TRANSFORM_QUEUE_VCPUS,
    datacite_download_job_definition,
    standard_job_definition,
)
from dmpworks.batch_submit.jobs import (
    get_task_types_to_run,
    make_env,
    run_job_pipeline,
    standard_job_queue,
    submit_factory_job,
    submit_job_from_params,
)


@pytest.fixture
def mock_submit(monkeypatch):
    m = MagicMock(return_value="test-job-id")
    monkeypatch.setattr("dmpworks.batch_submit.jobs.submit_job", m)
    return m


def env_as_dict(call_args) -> dict[str, str]:
    """Convert the environment list from a submit_job call_args to a plain dict."""
    return {e["name"]: e["value"] for e in call_args.kwargs["environment"]}


class TestNamingHelpers:
    def test_standard_job_definition(self):
        assert standard_job_definition("dev") == "dmpworks-dev-job"
        assert standard_job_definition("stg") == "dmpworks-stg-job"

    def test_datacite_download_job_definition(self):
        assert datacite_download_job_definition("dev") == "dmpworks-dev-datacite-download-job"
        assert datacite_download_job_definition("stg") == "dmpworks-stg-datacite-download-job"

    def test_standard_job_queue(self):
        assert standard_job_queue("dev") == "dmpworks-dev-batch-small-job-queue"
        assert standard_job_queue("stg") == "dmpworks-stg-batch-small-job-queue"


class TestMakeEnv:
    def test_converts_dict_to_list_of_name_value_dicts(self):
        result = make_env({"FOO": "bar", "BAZ": "qux"})
        assert {"name": "FOO", "value": "bar"} in result
        assert {"name": "BAZ", "value": "qux"} in result

    def test_filters_out_none_values(self):
        result = make_env({"PRESENT": "yes", "ABSENT": None})
        names = [e["name"] for e in result]
        assert "PRESENT" in names
        assert "ABSENT" not in names


class TestRunJobPipeline:
    def test_runs_all_tasks_in_order(self):
        call_order = []

        def task1():
            call_order.append("task1")
            return "job-1"

        def task2():
            call_order.append("task2")
            return "job-2"

        run_job_pipeline(
            task_definitions={"task1": task1, "task2": task2},
            task_order=["task1", "task2"],
            start_task_name="task1",
        )
        assert call_order == ["task1", "task2"]

    def test_skips_tasks_before_start(self):
        call_order = []

        def task1():
            call_order.append("task1")
            return "job-1"

        def task2():
            call_order.append("task2")
            return "job-2"

        run_job_pipeline(
            task_definitions={"task1": task1, "task2": task2},
            task_order=["task1", "task2"],
            start_task_name="task2",
        )
        assert call_order == ["task2"]

    def test_passes_previous_job_id_as_depends_on(self):
        received = []

        def task1():
            return "job-abc"

        def task2(*, depends_on=None):
            received.append(depends_on)
            return "job-def"

        run_job_pipeline(
            task_definitions={"task1": task1, "task2": task2},
            task_order=["task1", "task2"],
            start_task_name="task1",
        )
        assert received == [[{"jobId": "job-abc"}]]

    def test_does_not_pass_depends_on_to_task_without_parameter(self):
        """A task without a depends_on parameter must not receive it (would raise TypeError)."""
        called = []

        def task1():
            return "job-abc"

        def task2():
            called.append(True)
            return "job-def"

        # Would raise TypeError if depends_on were passed
        run_job_pipeline(
            task_definitions={"task1": task1, "task2": task2},
            task_order=["task1", "task2"],
            start_task_name="task1",
        )
        assert called == [True]

    def test_no_depends_on_when_first_task_returns_none(self):
        received = []

        def task1():
            return None  # No job ID produced

        def task2(*, depends_on=None):
            received.append(depends_on)
            return "job-def"

        run_job_pipeline(
            task_definitions={"task1": task1, "task2": task2},
            task_order=["task1", "task2"],
            start_task_name="task1",
        )
        # job_ids is empty after task1, so depends_on is never injected
        assert received == [None]

    def test_raises_for_unknown_start_task(self):
        with pytest.raises(ValueError, match="Unknown start_task"):
            run_job_pipeline(
                task_definitions={"task1": lambda: None},
                task_order=["task1"],
                start_task_name="nonexistent",
            )

    def test_raises_for_missing_task_definition(self):
        with pytest.raises(ValueError, match="No function defined"):
            run_job_pipeline(
                task_definitions={"task1": lambda: "job-1"},
                task_order=["task1", "task2"],
                start_task_name="task1",
            )


class TestSubmitFactoryJob:
    def test_calls_factory_and_submits(self, mock_submit):
        submit_factory_job(
            factory_key=("ror", "download"),
            run_id="test-run",
            env="dev",
            bucket_name="b",
            download_url="http://x",
            file_hash="h",
        )
        mock_submit.assert_called_once()
        assert mock_submit.call_args.kwargs["job_name"] == "ror-download"

    def test_depends_on_forwarded(self, mock_submit):
        depends = [{"jobId": "prior"}]
        submit_factory_job(
            factory_key=("ror", "download"),
            run_id="r",
            env="dev",
            bucket_name="b",
            download_url="http://x",
            file_hash="h",
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends

    def test_raises_for_unknown_factory_key(self):
        with pytest.raises(KeyError):
            submit_factory_job(factory_key=("nonexistent", "task"), run_id="r")


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
}


class TestStageRegistry:
    def test_all_pipeline_stage_keys_exist(self):
        expected_keys = [
            ("ror", "download"),
            ("data-citation-corpus", "download"),
            ("openalex-works", "download"),
            ("openalex-works", "subset"),
            ("openalex-works", "transform"),
            ("crossref-metadata", "download"),
            ("crossref-metadata", "subset"),
            ("crossref-metadata", "transform"),
            ("datacite", "download"),
            ("datacite", "subset"),
            ("datacite", "transform"),
        ]
        for key in expected_keys:
            assert key in JOB_FACTORIES, f"{key} not found in JOB_FACTORIES"

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

    def test_run_names(self):
        assert (
            JOB_FACTORIES[("ror", "download")](**MINIMAL_FACTORY_ARGS[("ror", "download")])["run_name"]
            == "ror-download"
        )
        assert (
            JOB_FACTORIES[("data-citation-corpus", "download")](
                **MINIMAL_FACTORY_ARGS[("data-citation-corpus", "download")]
            )["run_name"]
            == "data-citation-corpus-download"
        )
        assert (
            JOB_FACTORIES[("openalex-works", "subset")](**MINIMAL_FACTORY_ARGS[("openalex-works", "subset")])[
                "run_name"
            ]
            == "openalex-works-dataset-subset"
        )
        assert (
            JOB_FACTORIES[("crossref-metadata", "subset")](**MINIMAL_FACTORY_ARGS[("crossref-metadata", "subset")])[
                "run_name"
            ]
            == "crossref-metadata-dataset-subset"
        )
        assert (
            JOB_FACTORIES[("datacite", "subset")](**MINIMAL_FACTORY_ARGS[("datacite", "subset")])["run_name"]
            == "datacite-dataset-subset"
        )
        assert (
            JOB_FACTORIES[("openalex-works", "transform")](**MINIMAL_FACTORY_ARGS[("openalex-works", "transform")])[
                "run_name"
            ]
            == "openalex-works-transform"
        )
        assert (
            JOB_FACTORIES[("crossref-metadata", "transform")](
                **MINIMAL_FACTORY_ARGS[("crossref-metadata", "transform")]
            )["run_name"]
            == "crossref-metadata-transform"
        )
        assert (
            JOB_FACTORIES[("datacite", "transform")](**MINIMAL_FACTORY_ARGS[("datacite", "transform")])["run_name"]
            == "datacite-transform"
        )


class TestGetTaskTypesToRun:
    def test_ror_always_download_only(self):
        assert get_task_types_to_run("ror", use_subset=True) == ["download"]
        assert get_task_types_to_run("ror", use_subset=False) == ["download"]

    def test_dcc_always_download_only(self):
        assert get_task_types_to_run("data-citation-corpus", use_subset=True) == ["download"]
        assert get_task_types_to_run("data-citation-corpus", use_subset=False) == ["download"]

    def test_openalex_with_subset(self):
        assert get_task_types_to_run("openalex-works", use_subset=True) == ["download", "subset", "transform"]

    def test_openalex_without_subset(self):
        assert get_task_types_to_run("openalex-works", use_subset=False) == ["download", "transform"]

    def test_crossref_with_subset(self):
        assert get_task_types_to_run("crossref-metadata", use_subset=True) == ["download", "subset", "transform"]

    def test_crossref_without_subset(self):
        assert get_task_types_to_run("crossref-metadata", use_subset=False) == ["download", "transform"]

    def test_datacite_with_subset(self):
        assert get_task_types_to_run("datacite", use_subset=True) == ["download", "subset", "transform"]

    def test_datacite_without_subset(self):
        assert get_task_types_to_run("datacite", use_subset=False) == ["download", "transform"]


class TestSubmitJobFromParams:
    def test_reads_run_name_from_params(self, mock_submit):
        params = JOB_FACTORIES[("ror", "download")](
            run_id="test-run", bucket_name="b", download_url="http://x", file_hash="h", env="dev"
        )
        submit_job_from_params(params=params, run_id="test-run")
        assert mock_submit.call_args.kwargs["job_name"] == "ror-download"

    def test_translates_pascal_to_snake_case_env(self, mock_submit):
        params = JOB_FACTORIES[("ror", "download")](
            run_id="test-run", bucket_name="b", download_url="http://x", file_hash="h", env="dev"
        )
        submit_job_from_params(params=params, run_id="test-run")
        env = mock_submit.call_args.kwargs["environment"]
        assert all("name" in e and "value" in e for e in env)
        assert all("Name" not in e for e in env)

    def test_extracts_command_string_at_index_2(self, mock_submit):
        params = JOB_FACTORIES[("ror", "download")](
            run_id="test-run", bucket_name="b", download_url="http://x", file_hash="h", env="dev"
        )
        submit_job_from_params(params=params, run_id="test-run")
        command = mock_submit.call_args.kwargs["command"]
        assert isinstance(command, str)
        assert "ror download" in command

    def test_passes_depends_on(self, mock_submit):
        params = JOB_FACTORIES[("ror", "download")](
            run_id="test-run", bucket_name="b", download_url="http://x", file_hash="h", env="dev"
        )
        depends = [{"jobId": "prior"}]
        submit_job_from_params(params=params, run_id="test-run", depends_on=depends)
        assert mock_submit.call_args.kwargs["depends_on"] == depends
