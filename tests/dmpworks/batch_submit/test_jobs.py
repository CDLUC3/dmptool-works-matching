from unittest.mock import MagicMock

from dmpworks.batch_submit.jobs import (
    LARGE_MEMORY,
    LARGE_VCPUS,
    NANO_MEMORY,
    NANO_VCPUS,
    TQDM_MININTERVAL,
    TQDM_POSITION,
    VERY_LARGE_MEMORY,
    VERY_LARGE_VCPUS,
    crossref_metadata_download_job,
    crossref_metadata_transform_job,
    database_job_definition,
    datacite_download_job,
    datacite_download_job_definition,
    datacite_transform_job,
    dataset_subset_job,
    make_env,
    openalex_works_download_job,
    openalex_works_transform_job,
    ror_download_job,
    run_job_pipeline,
    standard_job_definition,
    standard_job_queue,
    submit_dmp_works_search_job,
    submit_enrich_dmps_job,
    submit_merge_related_works_job,
    submit_sqlmesh_job,
    submit_sync_dmps_job,
    submit_sync_works_job,
)
from dmpworks.cli_utils import (
    CrossrefMetadataTransformConfig,
    DataCiteTransformConfig,
    DatasetSubsetAWS,
    DMPSubsetAWS,
    OpenAlexWorksTransformConfig,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
    RunIdentifiers,
    SQLMeshConfig,
)
import pytest


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
        assert standard_job_definition("dev") == "dmp-tool-dev-batch-dmpworks-job"
        assert standard_job_definition("prod") == "dmp-tool-prod-batch-dmpworks-job"

    def test_datacite_download_job_definition(self):
        assert datacite_download_job_definition("dev") == "dmp-tool-dev-batch-dmpworks-datacite-download-job"
        assert datacite_download_job_definition("prod") == "dmp-tool-prod-batch-dmpworks-datacite-download-job"

    def test_database_job_definition(self):
        assert database_job_definition("dev") == "dmp-tool-dev-batch-dmpworks-database-job"
        assert database_job_definition("prod") == "dmp-tool-prod-batch-dmpworks-database-job"

    def test_standard_job_queue(self):
        assert standard_job_queue("dev") == "dmp-tool-dev-batch-job-queue"
        assert standard_job_queue("prod") == "dmp-tool-prod-batch-job-queue"


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


class TestRorDownloadJob:
    def test_submit_job_args(self, mock_submit):
        ror_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            download_url="https://zenodo.org/ror.zip",
            hash="sha256:abc",
        )
        mock_submit.assert_called_once()
        kwargs = mock_submit.call_args.kwargs
        assert kwargs["job_name"] == "ror-download"
        assert kwargs["run_id"] == "test-run"
        assert kwargs["job_queue"] == standard_job_queue("dev")
        assert kwargs["job_definition"] == standard_job_definition("dev")
        assert kwargs["vcpus"] == NANO_VCPUS
        assert kwargs["memory"] == NANO_MEMORY
        assert "ror download" in kwargs["command"]

    def test_environment(self, mock_submit):
        ror_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            download_url="https://zenodo.org/ror.zip",
            hash="sha256:abc",
        )
        assert env_as_dict(mock_submit.call_args) == {
            "RUN_ID": "test-run",
            "BUCKET_NAME": "my-bucket",
            "DOWNLOAD_URL": "https://zenodo.org/ror.zip",
            "HASH": "sha256:abc",
        }


class TestDatasetSubsetJob:
    def test_submit_job_args(self, mock_submit):
        ds = DatasetSubsetAWS(enable=True, institutions_s3_path="path/institutions.csv")
        dataset_subset_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            dataset="crossref-metadata",
            dataset_subset=ds,
        )
        mock_submit.assert_called_once()
        kwargs = mock_submit.call_args.kwargs
        assert kwargs["job_name"] == "crossref-metadata-dataset-subset"
        assert kwargs["job_definition"] == standard_job_definition("dev")
        assert kwargs["command"] == "dmpworks aws-batch $DATASET dataset-subset $BUCKET_NAME $RUN_ID"
        assert kwargs["vcpus"] == LARGE_VCPUS
        assert kwargs["memory"] == LARGE_MEMORY

    def test_environment_with_all_subset_fields(self, mock_submit):
        ds = DatasetSubsetAWS(
            enable=True,
            institutions_s3_path="path/institutions.csv",
            dois_s3_path="path/dois.csv",
        )
        dataset_subset_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            dataset="crossref-metadata",
            dataset_subset=ds,
        )
        assert env_as_dict(mock_submit.call_args) == {
            "RUN_ID": "test-run",
            "BUCKET_NAME": "my-bucket",
            "DATASET": "crossref-metadata",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "DATASET_SUBSET_ENABLE": "true",
            "DATASET_SUBSET_INSTITUTIONS_S3_PATH": "path/institutions.csv",
            "DATASET_SUBSET_DOIS_S3_PATH": "path/dois.csv",
        }

    def test_environment_none_subset_fields_filtered(self, mock_submit):
        ds = DatasetSubsetAWS(enable=False)
        dataset_subset_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            dataset="openalex-works",
            dataset_subset=ds,
        )
        env = env_as_dict(mock_submit.call_args)
        assert env["DATASET_SUBSET_ENABLE"] == "false"
        assert "DATASET_SUBSET_INSTITUTIONS_S3_PATH" not in env
        assert "DATASET_SUBSET_DOIS_S3_PATH" not in env

    def test_depends_on_passed(self, mock_submit):
        ds = DatasetSubsetAWS(enable=True)
        depends = [{"jobId": "prior-job"}]
        dataset_subset_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            dataset="openalex-works",
            dataset_subset=ds,
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends


class TestOpenAlexWorksDownloadJob:
    def test_submit_job_args(self, mock_submit):
        openalex_works_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            openalex_bucket_name="openalex-bucket",
        )
        kwargs = mock_submit.call_args.kwargs
        assert kwargs["job_name"] == "openalex-works-download"
        assert kwargs["job_definition"] == standard_job_definition("dev")
        assert kwargs["vcpus"] == LARGE_VCPUS
        assert kwargs["memory"] == LARGE_MEMORY
        assert "openalex-works download" in kwargs["command"]

    def test_environment(self, mock_submit):
        openalex_works_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            openalex_bucket_name="openalex-bucket",
        )
        assert env_as_dict(mock_submit.call_args) == {
            "RUN_ID": "test-run",
            "BUCKET_NAME": "my-bucket",
            "OPENALEX_BUCKET_NAME": "openalex-bucket",
        }


class TestOpenAlexWorksTransformJob:
    def test_environment(self, mock_submit):
        config = OpenAlexWorksTransformConfig(
            batch_size=8,
            row_group_size=100_000,
            row_groups_per_file=2,
            max_workers=16,
        )
        openalex_works_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=True,
            config=config,
            log_level="DEBUG",
        )
        assert env_as_dict(mock_submit.call_args) == {
            "RUN_ID": "test-run",
            "BUCKET_NAME": "my-bucket",
            "USE_SUBSET": "true",
            "LOG_LEVEL": "DEBUG",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "OPENALEX_WORKS_TRANSFORM_BATCH_SIZE": "8",
            "OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE": "100000",
            "OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE": "2",
            "OPENALEX_WORKS_TRANSFORM_MAX_WORKERS": "16",
        }

    def test_use_subset_false_serialized_to_lowercase(self, mock_submit):
        openalex_works_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=False,
            config=OpenAlexWorksTransformConfig(),
        )
        assert env_as_dict(mock_submit.call_args)["USE_SUBSET"] == "false"

    def test_use_subset_true_serialized_to_lowercase(self, mock_submit):
        openalex_works_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=True,
            config=OpenAlexWorksTransformConfig(),
        )
        assert env_as_dict(mock_submit.call_args)["USE_SUBSET"] == "true"

    def test_command(self, mock_submit):
        openalex_works_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            config=OpenAlexWorksTransformConfig(),
        )
        command = mock_submit.call_args.kwargs["command"]
        assert "openalex-works transform" in command
        assert "--use-subset=$USE_SUBSET" in command

    def test_uses_standard_job_definition(self, mock_submit):
        openalex_works_transform_job(
            env="stage",
            bucket_name="my-bucket",
            run_id="test-run",
            config=OpenAlexWorksTransformConfig(),
        )
        assert mock_submit.call_args.kwargs["job_definition"] == standard_job_definition("stage")

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        openalex_works_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            config=OpenAlexWorksTransformConfig(),
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends


class TestCrossrefMetadataDownloadJob:
    def test_submit_job_args(self, mock_submit):
        crossref_metadata_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            file_name="crossref.tar.gz",
            crossref_bucket_name="crossref-bucket",
        )
        kwargs = mock_submit.call_args.kwargs
        assert kwargs["job_name"] == "crossref-metadata-download"
        assert kwargs["job_definition"] == standard_job_definition("dev")
        assert "crossref-metadata download" in kwargs["command"]

    def test_environment(self, mock_submit):
        crossref_metadata_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            file_name="crossref.tar.gz",
            crossref_bucket_name="crossref-bucket",
        )
        assert env_as_dict(mock_submit.call_args) == {
            "RUN_ID": "test-run",
            "BUCKET_NAME": "my-bucket",
            "FILE_NAME": "crossref.tar.gz",
            "CROSSREF_BUCKET": "crossref-bucket",
        }


class TestCrossrefMetadataTransformJob:
    def test_environment(self, mock_submit):
        config = CrossrefMetadataTransformConfig(
            batch_size=100,
            row_group_size=250_000,
            row_groups_per_file=2,
            max_workers=8,
        )
        crossref_metadata_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=False,
            config=config,
            log_level="WARNING",
        )
        assert env_as_dict(mock_submit.call_args) == {
            "RUN_ID": "test-run",
            "BUCKET_NAME": "my-bucket",
            "USE_SUBSET": "false",
            "LOG_LEVEL": "WARNING",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "CROSSREF_METADATA_TRANSFORM_BATCH_SIZE": "100",
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE": "250000",
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE": "2",
            "CROSSREF_METADATA_TRANSFORM_MAX_WORKERS": "8",
        }

    def test_use_subset_false(self, mock_submit):
        crossref_metadata_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=False,
            config=CrossrefMetadataTransformConfig(),
        )
        assert env_as_dict(mock_submit.call_args)["USE_SUBSET"] == "false"

    def test_use_subset_true(self, mock_submit):
        crossref_metadata_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=True,
            config=CrossrefMetadataTransformConfig(),
        )
        assert env_as_dict(mock_submit.call_args)["USE_SUBSET"] == "true"

    def test_uses_standard_job_definition(self, mock_submit):
        crossref_metadata_transform_job(
            env="prod",
            bucket_name="my-bucket",
            run_id="test-run",
            config=CrossrefMetadataTransformConfig(),
        )
        assert mock_submit.call_args.kwargs["job_definition"] == standard_job_definition("prod")

    def test_command(self, mock_submit):
        crossref_metadata_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            config=CrossrefMetadataTransformConfig(),
        )
        command = mock_submit.call_args.kwargs["command"]
        assert "crossref-metadata transform" in command
        assert "--use-subset=$USE_SUBSET" in command

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        crossref_metadata_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            config=CrossrefMetadataTransformConfig(),
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends


class TestDataciteDownloadJob:
    def test_uses_datacite_download_job_definition(self, mock_submit):
        datacite_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            datacite_bucket_name="datacite-bucket",
        )
        job_def = mock_submit.call_args.kwargs["job_definition"]
        assert job_def == datacite_download_job_definition("dev")
        assert job_def != standard_job_definition("dev")

    def test_command(self, mock_submit):
        datacite_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            datacite_bucket_name="datacite-bucket",
        )
        assert "datacite download" in mock_submit.call_args.kwargs["command"]

    def test_environment(self, mock_submit):
        datacite_download_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            datacite_bucket_name="datacite-bucket",
        )
        assert env_as_dict(mock_submit.call_args) == {
            "RUN_ID": "test-run",
            "BUCKET_NAME": "my-bucket",
            "DATACITE_BUCKET_NAME": "datacite-bucket",
        }


class TestDataciteTransformJob:
    def test_environment(self, mock_submit):
        config = DataCiteTransformConfig(
            batch_size=50,
            row_group_size=125_000,
            row_groups_per_file=4,
            max_workers=4,
        )
        datacite_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=True,
            config=config,
            log_level="ERROR",
        )
        assert env_as_dict(mock_submit.call_args) == {
            "RUN_ID": "test-run",
            "BUCKET_NAME": "my-bucket",
            "USE_SUBSET": "true",
            "LOG_LEVEL": "ERROR",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "DATACITE_TRANSFORM_BATCH_SIZE": "50",
            "DATACITE_TRANSFORM_ROW_GROUP_SIZE": "125000",
            "DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE": "4",
            "DATACITE_TRANSFORM_MAX_WORKERS": "4",
        }

    def test_uses_standard_job_definition(self, mock_submit):
        datacite_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            config=DataCiteTransformConfig(),
        )
        assert mock_submit.call_args.kwargs["job_definition"] == standard_job_definition("dev")

    def test_use_subset_false(self, mock_submit):
        datacite_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=False,
            config=DataCiteTransformConfig(),
        )
        assert env_as_dict(mock_submit.call_args)["USE_SUBSET"] == "false"

    def test_use_subset_true(self, mock_submit):
        datacite_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            use_subset=True,
            config=DataCiteTransformConfig(),
        )
        assert env_as_dict(mock_submit.call_args)["USE_SUBSET"] == "true"

    def test_command(self, mock_submit):
        datacite_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            config=DataCiteTransformConfig(),
        )
        command = mock_submit.call_args.kwargs["command"]
        assert "datacite transform" in command
        assert "--use-subset=$USE_SUBSET" in command

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        datacite_transform_job(
            env="dev",
            bucket_name="my-bucket",
            run_id="test-run",
            config=DataCiteTransformConfig(),
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends


# Expected env for a default SQLMeshConfig with no paths set.
# All path fields (duckdb_database, *_path fields) are None and filtered by make_env.
SQLMESH_CFG_ENV = {
    "DUCKDB_THREADS": "32",
    "DUCKDB_MEMORY_LIMIT": "225GB",
    "AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD": "167008747",
    "AUDIT_DATACITE_WORKS_THRESHOLD": "72019576",
    "AUDIT_NESTED_OBJECT_LIMIT": "20000",
    "AUDIT_OPENALEX_WORKS_THRESHOLD": "264675126",
    "MAX_DOI_STATES": "3",
    "MAX_RELATION_DEGREES": "100",
    "CROSSREF_CROSSREF_METADATA_THREADS": "32",
    "CROSSREF_INDEX_WORKS_METADATA_THREADS": "32",
    "DATACITE_DATACITE_THREADS": "32",
    "DATACITE_INDEX_AWARDS_THREADS": "32",
    "DATACITE_INDEX_DATACITE_INDEX_THREADS": "32",
    "DATACITE_INDEX_FUNDERS_THREADS": "32",
    "DATACITE_INDEX_INSTITUTIONS_THREADS": "32",
    "DATACITE_INDEX_UPDATED_DATES_THREADS": "32",
    "DATACITE_INDEX_WORK_TYPES_THREADS": "32",
    "DATACITE_INDEX_WORKS_THREADS": "32",
    "DATACITE_INDEX_DATACITE_INDEX_HASHES_THREADS": "32",
    "OPENALEX_OPENALEX_WORKS_THREADS": "32",
    "OPENALEX_INDEX_ABSTRACT_STATS_THREADS": "32",
    "OPENALEX_INDEX_ABSTRACTS_THREADS": "32",
    "OPENALEX_INDEX_AUTHOR_NAMES_THREADS": "32",
    "OPENALEX_INDEX_AWARDS_THREADS": "32",
    "OPENALEX_INDEX_FUNDERS_THREADS": "32",
    "OPENALEX_INDEX_OPENALEX_INDEX_THREADS": "32",
    "OPENALEX_INDEX_PUBLICATION_DATES_THREADS": "32",
    "OPENALEX_INDEX_TITLE_STATS_THREADS": "32",
    "OPENALEX_INDEX_TITLES_THREADS": "32",
    "OPENALEX_INDEX_UPDATED_DATES_THREADS": "32",
    "OPENALEX_INDEX_WORKS_METADATA_THREADS": "32",
    "OPENALEX_INDEX_OPENALEX_INDEX_HASHES_THREADS": "32",
    "OPENSEARCH_CURRENT_DOI_STATE_THREADS": "32",
    "OPENSEARCH_EXPORT_THREADS": "32",
    "OPENSEARCH_NEXT_DOI_STATE_THREADS": "32",
    "DATA_CITATION_CORPUS_THREADS": "32",
    "RELATIONS_CROSSREF_METADATA_DEGREES_THREADS": "32",
    "RELATIONS_CROSSREF_METADATA_THREADS": "32",
    "RELATIONS_DATA_CITATION_CORPUS_THREADS": "32",
    "RELATIONS_DATACITE_DEGREES_THREADS": "16",
    "RELATIONS_DATACITE_THREADS": "16",
    "RELATIONS_RELATIONS_INDEX_THREADS": "32",
    "ROR_INDEX_THREADS": "32",
    "ROR_ROR_THREADS": "32",
    "WORKS_INDEX_EXPORT_THREADS": "32",
}

# Realistic AWS OpenSearch client config (mirrors .env.aws.example).
AWS_OS_CLIENT_CONFIG = OpenSearchClientConfig(
    host="opensearch-domain.region.es.amazonaws.com",
    port=9200,
    use_ssl=True,
    verify_certs=True,
    auth_type="aws",
    aws_region="aws-region",
    aws_service="es",
    pool_maxsize=20,
    timeout=300,
)

# Expected env for _AWS_OS_CLIENT_CONFIG.
# None fields (username, password) are filtered by make_env.
OS_CLIENT_CFG_ENV = {
    "OPENSEARCH_HOST": "opensearch-domain.region.es.amazonaws.com",
    "OPENSEARCH_PORT": "9200",
    "OPENSEARCH_USE_SSL": "true",
    "OPENSEARCH_VERIFY_CERTS": "true",
    "OPENSEARCH_AUTH_TYPE": "aws",
    "OPENSEARCH_REGION": "aws-region",
    "OPENSEARCH_SERVICE": "es",
    "OPENSEARCH_POOL_MAXSIZE": "20",
    "OPENSEARCH_TIMEOUT": "300",
}

# Expected env for a default OpenSearchSyncConfig.
OS_SYNC_CFG_ENV = {
    "OPENSEARCH_SYNC_MAX_PROCESSES": "2",
    "OPENSEARCH_SYNC_CHUNK_SIZE": "1000",
    "OPENSEARCH_SYNC_MAX_CHUNK_BYTES": "104857600",
    "OPENSEARCH_SYNC_MAX_RETRIES": "10",
    "OPENSEARCH_SYNC_INITIAL_BACKOFF": "2",
    "OPENSEARCH_SYNC_MAX_BACKOFF": "600",
    "OPENSEARCH_SYNC_DRY_RUN": "false",
    "OPENSEARCH_SYNC_MEASURE_CHUNK_SIZE": "false",
    "OPENSEARCH_SYNC_MAX_ERROR_SAMPLES": "50",
    "OPENSEARCH_SYNC_STAGGERED_START": "false",
}


class TestSubmitSqlmeshJob:
    def test_submit_job_args(self, mock_submit):
        run_ids = RunIdentifiers(run_id_process_works="works-run-1")
        submit_sqlmesh_job(
            env="dev",
            bucket_name="my-bucket",
            run_identifiers=run_ids,
            sqlmesh_config=SQLMeshConfig(),
        )
        kwargs = mock_submit.call_args.kwargs
        assert kwargs["job_name"] == "sqlmesh"
        assert kwargs["run_id"] == "works-run-1"
        assert kwargs["job_definition"] == standard_job_definition("dev")
        assert kwargs["vcpus"] == VERY_LARGE_VCPUS
        assert kwargs["memory"] == VERY_LARGE_MEMORY
        assert "sqlmesh plan" in kwargs["command"]

    def test_environment(self, mock_submit):
        # Set a few RunIdentifiers values; others remain None and are filtered.
        run_ids = RunIdentifiers(
            run_id_process_works="works-run-1",
            openalex_works="2024-01-01",
            datacite="2024-03-15",
        )
        submit_sqlmesh_job(
            env="dev",
            bucket_name="my-bucket",
            run_identifiers=run_ids,
            sqlmesh_config=SQLMeshConfig(),
        )
        expected = {
            "BUCKET_NAME": "my-bucket",
            # RunIdentifiers non-None fields
            "RUN_ID_OPENALEX_WORKS": "2024-01-01",
            "RUN_ID_DATACITE": "2024-03-15",
            "RUN_ID_PROCESS_WORKS": "works-run-1",
        }
        expected.update(SQLMESH_CFG_ENV)
        assert env_as_dict(mock_submit.call_args) == expected

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        submit_sqlmesh_job(
            env="dev",
            bucket_name="my-bucket",
            run_identifiers=RunIdentifiers(run_id_process_works="works-run-1"),
            sqlmesh_config=SQLMeshConfig(),
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends


class TestSubmitSyncWorksJob:
    def test_submit_job_args(self, mock_submit):
        run_ids = RunIdentifiers(run_id_process_works="works-run-1")
        submit_sync_works_job(
            env="dev",
            bucket_name="my-bucket",
            run_identifiers=run_ids,
            index_name="my-works-index",
        )
        kwargs = mock_submit.call_args.kwargs
        assert kwargs["job_definition"] == standard_job_definition("dev")
        assert "sync-works" in kwargs["command"]

    def test_environment(self, mock_submit):
        run_ids = RunIdentifiers(run_id_process_works="works-run-1", ror="2024-01-01")
        submit_sync_works_job(
            env="dev",
            bucket_name="my-bucket",
            run_identifiers=run_ids,
            index_name="my-works-index",
            os_client_config=AWS_OS_CLIENT_CONFIG,
            os_sync_config=OpenSearchSyncConfig(),
        )
        expected = {
            "BUCKET_NAME": "my-bucket",
            "INDEX_NAME": "my-works-index",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            # RunIdentifiers non-None fields
            "RUN_ID_ROR": "2024-01-01",
            "RUN_ID_PROCESS_WORKS": "works-run-1",
        }
        expected.update(OS_CLIENT_CFG_ENV)
        expected.update(OS_SYNC_CFG_ENV)
        assert env_as_dict(mock_submit.call_args) == expected

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        submit_sync_works_job(
            env="dev",
            bucket_name="my-bucket",
            run_identifiers=RunIdentifiers(run_id_process_works="works-run-1"),
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends


class TestSubmitSyncDmpsJob:
    def test_uses_database_job_definition(self, mock_submit):
        submit_sync_dmps_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1")
        job_def = mock_submit.call_args.kwargs["job_definition"]
        assert job_def == database_job_definition("dev")
        assert job_def != standard_job_definition("dev")

    def test_command(self, mock_submit):
        submit_sync_dmps_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1", index_name="my-dmps-index")
        assert "sync-dmps $BUCKET_NAME $INDEX_NAME" in mock_submit.call_args.kwargs["command"]

    def test_environment(self, mock_submit):
        submit_sync_dmps_job(
            env="dev",
            bucket_name="my-bucket",
            run_id_dmps="dmps-run-1",
            index_name="my-dmps-index",
            os_client_config=AWS_OS_CLIENT_CONFIG,
            os_sync_config=OpenSearchSyncConfig(),
        )
        expected = {
            "BUCKET_NAME": "my-bucket",
            "INDEX_NAME": "my-dmps-index",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        }
        expected.update(OS_CLIENT_CFG_ENV)
        expected.update(OS_SYNC_CFG_ENV)
        assert env_as_dict(mock_submit.call_args) == expected

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        submit_sync_dmps_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1", depends_on=depends)
        assert mock_submit.call_args.kwargs["depends_on"] == depends


class TestSubmitEnrichDmpsJob:
    def test_uses_standard_job_definition(self, mock_submit):
        submit_enrich_dmps_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1")
        assert mock_submit.call_args.kwargs["job_definition"] == standard_job_definition("dev")

    def test_command(self, mock_submit):
        submit_enrich_dmps_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1", index_name="my-dmps-index")
        assert "enrich-dmps $INDEX_NAME --bucket-name $BUCKET_NAME" in mock_submit.call_args.kwargs["command"]

    def test_environment(self, mock_submit):
        submit_enrich_dmps_job(
            env="dev",
            bucket_name="my-bucket",
            run_id_dmps="dmps-run-1",
            index_name="my-dmps-index",
            os_client_config=AWS_OS_CLIENT_CONFIG,
        )
        expected = {
            "BUCKET_NAME": "my-bucket",
            "INDEX_NAME": "my-dmps-index",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        }
        expected.update(OS_CLIENT_CFG_ENV)
        assert env_as_dict(mock_submit.call_args) == expected

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        submit_enrich_dmps_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1", depends_on=depends)
        assert mock_submit.call_args.kwargs["depends_on"] == depends


class TestSubmitDmpWorksSearchJob:
    def test_uses_standard_job_definition(self, mock_submit):
        submit_dmp_works_search_job(
            env="dev",
            bucket_name="my-bucket",
            run_id_dmps="dmps-run-1",
            dmp_subset=None,
        )
        assert mock_submit.call_args.kwargs["job_definition"] == standard_job_definition("dev")

    def test_command(self, mock_submit):
        submit_dmp_works_search_job(
            env="dev",
            bucket_name="my-bucket",
            run_id_dmps="dmps-run-1",
            dmp_subset=None,
        )
        assert "dmp-works-search" in mock_submit.call_args.kwargs["command"]

    def test_environment_without_dmp_subset(self, mock_submit):
        submit_dmp_works_search_job(
            env="dev",
            bucket_name="my-bucket",
            run_id_dmps="dmps-run-1",
            os_client_config=AWS_OS_CLIENT_CONFIG,
            dmps_index_name="my-dmps-index",
            works_index_name="my-works-index",
            dmp_subset=None,
        )
        expected = {
            "BUCKET_NAME": "my-bucket",
            "RUN_ID": "dmps-run-1",
            "DMPS_INDEX_NAME": "my-dmps-index",
            "WORKS_INDEX_NAME": "my-works-index",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "DMP_WORKS_SEARCH_QUERY_BUILDER": "build_dmp_works_search_baseline_query",
            "DMP_WORKS_SEARCH_SCROLL_TIME": "360m",
            "DMP_WORKS_SEARCH_BATCH_SIZE": "250",
            "DMP_WORKS_SEARCH_MAX_RESULTS": "100",
            "DMP_WORKS_SEARCH_PROJECT_END_BUFFER_YEARS": "3",
            "DMP_WORKS_SEARCH_PARALLEL_SEARCH": "false",
            "DMP_WORKS_SEARCH_INCLUDE_NAMED_QUERIES_SCORE": "true",
            "DMP_WORKS_SEARCH_MAX_CONCURRENT_SEARCHES": "125",
            "DMP_WORKS_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS": "12",
            "DMP_WORKS_SEARCH_INNER_HITS_SIZE": "50",
            "DMP_WORKS_SEARCH_ROW_GROUP_SIZE": "50000",
            "DMP_WORKS_SEARCH_ROW_GROUPS_PER_FILE": "4",
        }
        expected.update(OS_CLIENT_CFG_ENV)
        assert env_as_dict(mock_submit.call_args) == expected

    def test_environment_with_dmp_subset(self, mock_submit):
        ds = DMPSubsetAWS(enable=True, dois_s3_path="path/dois.csv", institutions_s3_path="path/inst.csv")
        submit_dmp_works_search_job(
            env="dev",
            bucket_name="my-bucket",
            run_id_dmps="dmps-run-1",
            os_client_config=AWS_OS_CLIENT_CONFIG,
            dmps_index_name="my-dmps-index",
            works_index_name="my-works-index",
            dmp_subset=ds,
        )
        expected = {
            "BUCKET_NAME": "my-bucket",
            "RUN_ID": "dmps-run-1",
            "DMPS_INDEX_NAME": "my-dmps-index",
            "WORKS_INDEX_NAME": "my-works-index",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
            "DMP_WORKS_SEARCH_QUERY_BUILDER": "build_dmp_works_search_baseline_query",
            "DMP_WORKS_SEARCH_SCROLL_TIME": "360m",
            "DMP_WORKS_SEARCH_BATCH_SIZE": "250",
            "DMP_WORKS_SEARCH_MAX_RESULTS": "100",
            "DMP_WORKS_SEARCH_PROJECT_END_BUFFER_YEARS": "3",
            "DMP_WORKS_SEARCH_PARALLEL_SEARCH": "false",
            "DMP_WORKS_SEARCH_INCLUDE_NAMED_QUERIES_SCORE": "true",
            "DMP_WORKS_SEARCH_MAX_CONCURRENT_SEARCHES": "125",
            "DMP_WORKS_SEARCH_MAX_CONCURRENT_SHARD_REQUESTS": "12",
            "DMP_WORKS_SEARCH_INNER_HITS_SIZE": "50",
            "DMP_WORKS_SEARCH_ROW_GROUP_SIZE": "50000",
            "DMP_WORKS_SEARCH_ROW_GROUPS_PER_FILE": "4",
            "DMP_SUBSET_ENABLE": "true",
            "DMP_SUBSET_DOIS_S3_PATH": "path/dois.csv",
            "DMP_SUBSET_INSTITUTIONS_S3_PATH": "path/inst.csv",
        }
        expected.update(OS_CLIENT_CFG_ENV)
        assert env_as_dict(mock_submit.call_args) == expected

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        submit_dmp_works_search_job(
            env="dev",
            bucket_name="my-bucket",
            run_id_dmps="dmps-run-1",
            dmp_subset=None,
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends


class TestSubmitMergeRelatedWorksJob:
    def test_uses_database_job_definition(self, mock_submit):
        submit_merge_related_works_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1")
        job_def = mock_submit.call_args.kwargs["job_definition"]
        assert job_def == database_job_definition("dev")
        assert job_def != standard_job_definition("dev")

    def test_command(self, mock_submit):
        submit_merge_related_works_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1")
        assert "merge-related-works" in mock_submit.call_args.kwargs["command"]

    def test_environment(self, mock_submit):
        submit_merge_related_works_job(env="dev", bucket_name="my-bucket", run_id_dmps="dmps-run-1")
        assert env_as_dict(mock_submit.call_args) == {
            "BUCKET_NAME": "my-bucket",
            "RUN_ID": "dmps-run-1",
            "TQDM_POSITION": TQDM_POSITION,
            "TQDM_MININTERVAL": TQDM_MININTERVAL,
        }

    def test_depends_on_passed(self, mock_submit):
        depends = [{"jobId": "prior"}]
        submit_merge_related_works_job(
            env="dev",
            bucket_name="my-bucket",
            run_id_dmps="dmps-run-1",
            depends_on=depends,
        )
        assert mock_submit.call_args.kwargs["depends_on"] == depends
