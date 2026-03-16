from unittest.mock import patch

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


class TestRorCmd:
    def test_builds_correct_task_definitions(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            ror_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                download_url="https://zenodo.org/ror.zip",
                hash="abc123",
            )

        mock_pipeline.assert_called_once()
        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == list(ROR_JOBS)
        assert kwargs["start_task_name"] == "download"
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"download"}
        assert task_defs["download"].func is submit_factory_job
        assert task_defs["download"].keywords["factory_key"] == ("ror", "download")
        assert task_defs["download"].keywords["env"] == "dev"
        assert task_defs["download"].keywords["run_id"] == "test-run"
        assert task_defs["download"].keywords["bucket_name"] == "my-bucket"
        assert task_defs["download"].keywords["download_url"] == "https://zenodo.org/ror.zip"
        assert task_defs["download"].keywords["file_hash"] == "abc123"

    def test_custom_start_job(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            ror_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                download_url="https://zenodo.org/ror.zip",
                hash="abc123",
                start_job="download",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "download"


class TestCrossrefMetadataCmd:
    def test_no_subset_excludes_dataset_subset_task(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            crossref_metadata_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                file_name="crossref.tar.gz",
                crossref_metadata_bucket_name="crossref-bucket",
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == ["download", "transform"]
        assert "dataset-subset" not in kwargs["task_order"]
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"download", "transform"}
        assert task_defs["download"].func is submit_factory_job
        assert task_defs["transform"].func is submit_factory_job
        assert task_defs["transform"].keywords["factory_key"] == ("crossref-metadata", "transform")

    def test_with_subset_includes_dataset_subset_task(self):
        ds = DatasetSubsetAWS(enable=True, institutions_s3_path="path/institutions.csv")
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            crossref_metadata_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                file_name="crossref.tar.gz",
                crossref_metadata_bucket_name="crossref-bucket",
                dataset_subset=ds,
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == list(CROSSREF_METADATA_JOBS)
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"download", "dataset-subset", "transform"}
        assert task_defs["dataset-subset"].func is submit_factory_job
        assert task_defs["dataset-subset"].keywords["factory_key"] == ("crossref-metadata", "subset")
        assert task_defs["dataset-subset"].keywords["dataset"] == "crossref-metadata"

    def test_disabled_dataset_subset_treated_as_no_subset(self):
        ds = DatasetSubsetAWS(enable=False)
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            crossref_metadata_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                file_name="crossref.tar.gz",
                crossref_metadata_bucket_name="crossref-bucket",
                dataset_subset=ds,
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert "dataset-subset" not in kwargs["task_order"]

    def test_start_job_passed_through(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            crossref_metadata_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                file_name="crossref.tar.gz",
                crossref_metadata_bucket_name="crossref-bucket",
                start_job="transform",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "transform"


class TestDataCiteCmd:
    def test_no_subset_excludes_dataset_subset_task(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            datacite_cmd(
                env="prod",
                run_id="test-run",
                bucket_name="my-bucket",
                datacite_bucket_name="datacite-bucket",
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == ["download", "transform"]
        assert "dataset-subset" not in kwargs["task_order"]
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"download", "transform"}
        assert task_defs["download"].func is submit_factory_job
        assert task_defs["download"].keywords["factory_key"] == ("datacite", "download")

    def test_with_subset_includes_dataset_subset_task(self):
        ds = DatasetSubsetAWS(enable=True)
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            datacite_cmd(
                env="prod",
                run_id="test-run",
                bucket_name="my-bucket",
                datacite_bucket_name="datacite-bucket",
                dataset_subset=ds,
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == list(DATACITE_JOBS)
        task_defs = kwargs["task_definitions"]
        assert "dataset-subset" in task_defs
        assert task_defs["dataset-subset"].keywords["factory_key"] == ("datacite", "subset")
        assert task_defs["dataset-subset"].keywords["dataset"] == "datacite"

    def test_start_job_passed_through(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            datacite_cmd(
                env="prod",
                run_id="test-run",
                bucket_name="my-bucket",
                datacite_bucket_name="datacite-bucket",
                start_job="transform",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "transform"


class TestOpenAlexWorksCmd:
    def test_no_subset_excludes_dataset_subset_task(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            openalex_works_cmd(
                env="stage",
                run_id="test-run",
                bucket_name="my-bucket",
                openalex_bucket_name="openalex-bucket",
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == ["download", "transform"]
        assert "dataset-subset" not in kwargs["task_order"]
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"download", "transform"}
        assert task_defs["download"].func is submit_factory_job
        assert task_defs["download"].keywords["factory_key"] == ("openalex-works", "download")

    def test_with_subset_includes_dataset_subset_task(self):
        ds = DatasetSubsetAWS(enable=True, dois_s3_path="path/dois.csv")
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            openalex_works_cmd(
                env="stage",
                run_id="test-run",
                bucket_name="my-bucket",
                openalex_bucket_name="openalex-bucket",
                dataset_subset=ds,
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == list(OPENALEX_WORKS_JOBS)
        task_defs = kwargs["task_definitions"]
        assert "dataset-subset" in task_defs
        assert task_defs["dataset-subset"].keywords["factory_key"] == ("openalex-works", "subset")
        assert task_defs["dataset-subset"].keywords["dataset"] == "openalex-works"

    def test_start_job_passed_through(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            openalex_works_cmd(
                env="stage",
                run_id="test-run",
                bucket_name="my-bucket",
                openalex_bucket_name="openalex-bucket",
                start_job="transform",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "transform"


class TestProcessWorksCmd:
    def test_builds_correct_task_definitions(self):
        run_ids = RunIdentifiers(run_id_sqlmesh="works-run-1")
        sqlmesh_cfg = SQLMeshConfig()
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            process_works_cmd(
                env="dev",
                bucket_name="my-bucket",
                run_identifiers=run_ids,
                sqlmesh_config=sqlmesh_cfg,
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == list(PROCESS_WORKS_JOBS)
        assert kwargs["start_task_name"] == PROCESS_WORKS_JOBS[0]
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"sqlmesh-transform", "sync-works"}
        assert task_defs["sqlmesh-transform"].func is submit_factory_job
        assert task_defs["sync-works"].func is submit_factory_job
        assert task_defs["sqlmesh-transform"].keywords["factory_key"] == ("process-works", "sqlmesh")
        assert task_defs["sync-works"].keywords["factory_key"] == ("process-works", "sync-works")

    def test_start_job_passed_through(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            process_works_cmd(
                env="dev",
                bucket_name="my-bucket",
                run_identifiers=RunIdentifiers(run_id_sqlmesh="works-run-1"),
                sqlmesh_config=SQLMeshConfig(),
                start_job="sync-works",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "sync-works"


class TestProcessDmpsCmd:
    def test_builds_correct_task_definitions(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            process_dmps_cmd(
                env="dev",
                bucket_name="my-bucket",
                run_id_dmps="dmps-run-1",
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == list(PROCESS_DMPS_JOBS)
        assert kwargs["start_task_name"] == PROCESS_DMPS_JOBS[0]
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"sync-dmps", "enrich-dmps", "dmp-works-search", "merge-related-works"}
        assert task_defs["sync-dmps"].func is submit_factory_job
        assert task_defs["enrich-dmps"].func is submit_factory_job
        assert task_defs["dmp-works-search"].func is submit_factory_job
        assert task_defs["merge-related-works"].func is submit_factory_job

    def test_partial_keywords(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            process_dmps_cmd(
                env="dev",
                bucket_name="my-bucket",
                run_id_dmps="dmps-run-1",
                dmps_index_name="my-dmps-index",
                works_index_name="my-works-index",
            )

        task_defs = mock_pipeline.call_args.kwargs["task_definitions"]
        assert task_defs["sync-dmps"].keywords["run_id"] == "dmps-run-1"
        assert task_defs["sync-dmps"].keywords["dmps_index_name"] == "my-dmps-index"
        assert task_defs["enrich-dmps"].keywords["dmps_index_name"] == "my-dmps-index"
        assert task_defs["dmp-works-search"].keywords["dmps_index_name"] == "my-dmps-index"
        assert task_defs["dmp-works-search"].keywords["works_index_name"] == "my-works-index"
        assert task_defs["merge-related-works"].keywords["bucket_name"] == "my-bucket"

    def test_start_job_passed_through(self):
        with patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline:
            process_dmps_cmd(
                env="dev",
                bucket_name="my-bucket",
                run_id_dmps="dmps-run-1",
                start_job="enrich-dmps",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "enrich-dmps"
