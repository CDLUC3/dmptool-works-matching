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
from dmpworks.cli_utils import (
    DatasetSubsetAWS,
    RunIdentifiers,
    SQLMeshConfig,
)


class TestRorCmd:
    def test_builds_correct_task_definitions(self):
        with (
            patch("dmpworks.batch_submit.jobs.ror_download_job") as mock_dl,
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        assert task_defs["download"].func is mock_dl
        assert task_defs["download"].keywords["env"] == "dev"
        assert task_defs["download"].keywords["run_id"] == "test-run"
        assert task_defs["download"].keywords["bucket_name"] == "my-bucket"
        assert task_defs["download"].keywords["download_url"] == "https://zenodo.org/ror.zip"
        assert task_defs["download"].keywords["hash"] == "abc123"

    def test_custom_start_job(self):
        with (
            patch("dmpworks.batch_submit.jobs.ror_download_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        with (
            patch("dmpworks.batch_submit.jobs.crossref_metadata_download_job") as mock_dl,
            patch("dmpworks.batch_submit.jobs.crossref_metadata_transform_job") as mock_tr,
            patch("dmpworks.batch_submit.jobs.dataset_subset_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
            crossref_metadata_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                file_name="crossref.tar.gz",
                crossref_bucket_name="crossref-bucket",
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == ["download", "transform"]
        assert "dataset-subset" not in kwargs["task_order"]
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"download", "transform"}
        assert task_defs["download"].func is mock_dl
        assert task_defs["transform"].func is mock_tr
        assert task_defs["transform"].keywords["use_subset"] is False

    def test_with_subset_includes_dataset_subset_task(self):
        ds = DatasetSubsetAWS(enable=True, institutions_s3_path="path/institutions.csv")
        with (
            patch("dmpworks.batch_submit.jobs.crossref_metadata_download_job") as mock_dl,
            patch("dmpworks.batch_submit.jobs.crossref_metadata_transform_job") as mock_tr,
            patch("dmpworks.batch_submit.jobs.dataset_subset_job") as mock_ds,
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
            crossref_metadata_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                file_name="crossref.tar.gz",
                crossref_bucket_name="crossref-bucket",
                dataset_subset=ds,
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert kwargs["task_order"] == list(CROSSREF_METADATA_JOBS)
        task_defs = kwargs["task_definitions"]
        assert set(task_defs.keys()) == {"download", "dataset-subset", "transform"}
        assert task_defs["dataset-subset"].func is mock_ds
        assert task_defs["dataset-subset"].keywords["dataset"] == "crossref-metadata"
        assert task_defs["dataset-subset"].keywords["dataset_subset"] is ds
        assert task_defs["transform"].keywords["use_subset"] is True

    def test_disabled_dataset_subset_treated_as_no_subset(self):
        ds = DatasetSubsetAWS(enable=False)
        with (
            patch("dmpworks.batch_submit.jobs.crossref_metadata_download_job"),
            patch("dmpworks.batch_submit.jobs.crossref_metadata_transform_job") as mock_tr,
            patch("dmpworks.batch_submit.jobs.dataset_subset_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
            crossref_metadata_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                file_name="crossref.tar.gz",
                crossref_bucket_name="crossref-bucket",
                dataset_subset=ds,
            )

        kwargs = mock_pipeline.call_args.kwargs
        assert "dataset-subset" not in kwargs["task_order"]
        assert mock_tr.call_args is None  # not called directly; via partial
        assert kwargs["task_definitions"]["transform"].keywords["use_subset"] is False

    def test_start_job_passed_through(self):
        with (
            patch("dmpworks.batch_submit.jobs.crossref_metadata_download_job"),
            patch("dmpworks.batch_submit.jobs.crossref_metadata_transform_job"),
            patch("dmpworks.batch_submit.jobs.dataset_subset_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
            crossref_metadata_cmd(
                env="dev",
                run_id="test-run",
                bucket_name="my-bucket",
                file_name="crossref.tar.gz",
                crossref_bucket_name="crossref-bucket",
                start_job="transform",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "transform"


class TestDataciteCmd:
    def test_no_subset_excludes_dataset_subset_task(self):
        with (
            patch("dmpworks.batch_submit.jobs.datacite_download_job") as mock_dl,
            patch("dmpworks.batch_submit.jobs.datacite_transform_job") as mock_tr,
            patch("dmpworks.batch_submit.jobs.dataset_subset_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        assert task_defs["download"].func is mock_dl
        assert task_defs["transform"].keywords["use_subset"] is False

    def test_with_subset_includes_dataset_subset_task(self):
        ds = DatasetSubsetAWS(enable=True)
        with (
            patch("dmpworks.batch_submit.jobs.datacite_download_job"),
            patch("dmpworks.batch_submit.jobs.datacite_transform_job") as mock_tr,
            patch("dmpworks.batch_submit.jobs.dataset_subset_job") as mock_ds,
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        assert task_defs["dataset-subset"].keywords["dataset"] == "datacite"
        assert task_defs["dataset-subset"].keywords["dataset_subset"] is ds
        assert task_defs["transform"].keywords["use_subset"] is True

    def test_start_job_passed_through(self):
        with (
            patch("dmpworks.batch_submit.jobs.datacite_download_job"),
            patch("dmpworks.batch_submit.jobs.datacite_transform_job"),
            patch("dmpworks.batch_submit.jobs.dataset_subset_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        with (
            patch("dmpworks.batch_submit.jobs.openalex_works_download_job") as mock_dl,
            patch("dmpworks.batch_submit.jobs.openalex_works_transform_job") as mock_tr,
            patch("dmpworks.batch_submit.jobs.dataset_subset_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        assert task_defs["download"].func is mock_dl
        assert task_defs["transform"].keywords["use_subset"] is False

    def test_with_subset_includes_dataset_subset_task(self):
        ds = DatasetSubsetAWS(enable=True, dois_s3_path="path/dois.csv")
        with (
            patch("dmpworks.batch_submit.jobs.openalex_works_download_job"),
            patch("dmpworks.batch_submit.jobs.openalex_works_transform_job") as mock_tr,
            patch("dmpworks.batch_submit.jobs.dataset_subset_job") as mock_ds,
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        assert task_defs["dataset-subset"].keywords["dataset"] == "openalex-works"
        assert task_defs["dataset-subset"].keywords["dataset_subset"] is ds
        assert task_defs["transform"].keywords["use_subset"] is True

    def test_start_job_passed_through(self):
        with (
            patch("dmpworks.batch_submit.jobs.openalex_works_download_job"),
            patch("dmpworks.batch_submit.jobs.openalex_works_transform_job"),
            patch("dmpworks.batch_submit.jobs.dataset_subset_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        run_ids = RunIdentifiers(run_id_process_works="works-run-1")
        sqlmesh_cfg = SQLMeshConfig()
        with (
            patch("dmpworks.batch_submit.jobs.submit_sqlmesh_job") as mock_sqlmesh,
            patch("dmpworks.batch_submit.jobs.submit_sync_works_job") as mock_sync,
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        assert task_defs["sqlmesh-transform"].func is mock_sqlmesh
        assert task_defs["sync-works"].func is mock_sync

    def test_start_job_passed_through(self):
        with (
            patch("dmpworks.batch_submit.jobs.submit_sqlmesh_job"),
            patch("dmpworks.batch_submit.jobs.submit_sync_works_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
            process_works_cmd(
                env="dev",
                bucket_name="my-bucket",
                run_identifiers=RunIdentifiers(run_id_process_works="works-run-1"),
                sqlmesh_config=SQLMeshConfig(),
                start_job="sync-works",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "sync-works"


class TestProcessDmpsCmd:
    def test_builds_correct_task_definitions(self):
        with (
            patch("dmpworks.batch_submit.jobs.submit_sync_dmps_job") as mock_sync,
            patch("dmpworks.batch_submit.jobs.submit_enrich_dmps_job") as mock_enrich,
            patch("dmpworks.batch_submit.jobs.submit_dmp_works_search_job") as mock_search,
            patch("dmpworks.batch_submit.jobs.submit_merge_related_works_job") as mock_merge,
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
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
        assert task_defs["sync-dmps"].func is mock_sync
        assert task_defs["enrich-dmps"].func is mock_enrich
        assert task_defs["dmp-works-search"].func is mock_search
        assert task_defs["merge-related-works"].func is mock_merge

    def test_partial_keywords(self):
        with (
            patch("dmpworks.batch_submit.jobs.submit_sync_dmps_job"),
            patch("dmpworks.batch_submit.jobs.submit_enrich_dmps_job"),
            patch("dmpworks.batch_submit.jobs.submit_dmp_works_search_job"),
            patch("dmpworks.batch_submit.jobs.submit_merge_related_works_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
            process_dmps_cmd(
                env="dev",
                bucket_name="my-bucket",
                run_id_dmps="dmps-run-1",
                dmps_index_name="my-dmps-index",
                works_index_name="my-works-index",
            )

        task_defs = mock_pipeline.call_args.kwargs["task_definitions"]
        assert task_defs["sync-dmps"].keywords["run_id_dmps"] == "dmps-run-1"
        assert task_defs["sync-dmps"].keywords["index_name"] == "my-dmps-index"
        assert task_defs["enrich-dmps"].keywords["index_name"] == "my-dmps-index"
        assert task_defs["dmp-works-search"].keywords["dmps_index_name"] == "my-dmps-index"
        assert task_defs["dmp-works-search"].keywords["works_index_name"] == "my-works-index"
        assert task_defs["merge-related-works"].keywords["bucket_name"] == "my-bucket"

    def test_start_job_passed_through(self):
        with (
            patch("dmpworks.batch_submit.jobs.submit_sync_dmps_job"),
            patch("dmpworks.batch_submit.jobs.submit_enrich_dmps_job"),
            patch("dmpworks.batch_submit.jobs.submit_dmp_works_search_job"),
            patch("dmpworks.batch_submit.jobs.submit_merge_related_works_job"),
            patch("dmpworks.batch_submit.jobs.run_job_pipeline") as mock_pipeline,
        ):
            process_dmps_cmd(
                env="dev",
                bucket_name="my-bucket",
                run_id_dmps="dmps-run-1",
                start_job="enrich-dmps",
            )
        assert mock_pipeline.call_args.kwargs["start_task_name"] == "enrich-dmps"
