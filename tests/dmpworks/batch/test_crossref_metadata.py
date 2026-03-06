from contextlib import contextmanager
import logging
import pathlib
import tempfile
from unittest.mock import MagicMock, call, patch

from dmpworks.batch import crossref_metadata as crossref_metadata_module
from dmpworks.batch.tasks import DownloadTaskContext
import pytest

MODULE = "dmpworks.batch.crossref_metadata"


class TestCrossrefMetadata:
    @pytest.fixture(autouse=True)
    def isolate_env(self, monkeypatch):
        """Remove Cyclopts env vars so session-level values don't override test defaults."""
        for var in [
            "CROSSREF_METADATA_TRANSFORM_BATCH_SIZE",
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE",
            "CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE",
            "CROSSREF_METADATA_TRANSFORM_MAX_WORKERS",
        ]:
            monkeypatch.delenv(var, raising=False)

    @pytest.fixture
    def mock_run_process(self, mocker):
        return mocker.patch(f"{MODULE}.run_process")

    @pytest.fixture
    def mock_download_source_task(self):
        data = {}

        @contextmanager
        def _mocked(bucket_name: str, dataset: str, run_id: str):
            with tempfile.TemporaryDirectory() as tmp_dir:
                data["temp_path"] = pathlib.Path(tmp_dir)
                ctx = DownloadTaskContext(
                    download_dir=pathlib.Path(tmp_dir) / dataset / run_id / "download",
                    target_uri=f"s3://{bucket_name}/{dataset}/{run_id}/download/",
                )
                yield ctx

        mock_wrapper = MagicMock(side_effect=_mocked)

        with patch(f"{MODULE}.download_source_task", mock_wrapper):
            yield {"data": data, "mock": mock_wrapper}

    def test_download(self, mock_download_source_task, mock_run_process):
        bucket = "my-bucket"
        dataset = "crossref_metadata"
        run_id = "2025-01-01"
        archive_name = "April_2025_Public_Data_File_from_Crossref.tar"
        crossref_bucket = "crossref-source-bucket"

        crossref_metadata_module.download(
            bucket_name=bucket,
            run_id=run_id,
            file_name=archive_name,
            crossref_bucket_name=crossref_bucket,
        )

        mock = mock_download_source_task["mock"]
        mock.assert_called_once_with(bucket, dataset, run_id)

        temp_path = mock_download_source_task["data"]["temp_path"]
        download_dir = temp_path / dataset / run_id / "download"
        archive_path: pathlib.Path = download_dir / archive_name

        mock_run_process.assert_has_calls(
            [
                call(
                    [
                        "s5cmd",
                        "--request-payer",
                        "requester",
                        "cp",
                        f"s3://{crossref_bucket}/{archive_name}",
                        f"{download_dir}/",
                    ],
                ),
                call(
                    [
                        "tar",
                        "-xvf",
                        str(archive_path),
                        "-C",
                        str(download_dir),
                        "--strip-components",
                        "1",
                    ],
                ),
            ]
        )

    @pytest.fixture
    def mock_dataset_subset_task(self):
        data = {}

        @contextmanager
        def _mocked(*, bucket_name: str, dataset: str, run_id: str, dataset_subset=None):
            with tempfile.TemporaryDirectory() as tmp_dir:
                data["temp_path"] = pathlib.Path(tmp_dir)
                ctx = MagicMock()
                ctx.download_dir = pathlib.Path(tmp_dir) / dataset / run_id / "download"
                ctx.subset_dir = pathlib.Path(tmp_dir) / dataset / run_id / "subset"
                ctx.institutions = []
                ctx.dois = []
                yield ctx

        mock_wrapper = MagicMock(side_effect=_mocked)

        with patch(f"{MODULE}.dataset_subset_task", mock_wrapper):
            yield {"data": data, "mock": mock_wrapper}

    @pytest.fixture
    def mock_transform_parquets_task(self):
        data = {}

        @contextmanager
        def _mocked(bucket_name: str, dataset: str, run_id: str, use_subset: bool = False):
            with tempfile.TemporaryDirectory() as tmp_dir:
                data["temp_path"] = pathlib.Path(tmp_dir)
                ctx = MagicMock()
                ctx.download_dir = pathlib.Path(tmp_dir) / dataset / run_id / "download"
                ctx.transform_dir = pathlib.Path(tmp_dir) / dataset / run_id / "transform"
                yield ctx

        mock_wrapper = MagicMock(side_effect=_mocked)

        with patch(f"{MODULE}.transform_parquets_task", mock_wrapper):
            yield {"data": data, "mock": mock_wrapper}

    @pytest.fixture
    def mock_transform_crossref_metadata(self, mocker):
        return mocker.patch(f"{MODULE}.transform_crossref_metadata")

    def test_transform_defaults(self, mock_transform_parquets_task, mock_transform_crossref_metadata):
        from dmpworks.cli_utils import CrossrefMetadataTransformConfig

        bucket = "my-bucket"
        dataset = "crossref_metadata"
        run_id = "2025-01-01"

        crossref_metadata_module.transform(
            bucket_name=bucket,
            run_id=run_id,
            config=CrossrefMetadataTransformConfig(),
        )

        mock_task = mock_transform_parquets_task["mock"]
        mock_task.assert_called_once_with(bucket, dataset, run_id, use_subset=False)

        temp_path = mock_transform_parquets_task["data"]["temp_path"]
        download_dir = temp_path / dataset / run_id / "download"
        transform_dir = temp_path / dataset / run_id / "transform"

        mock_transform_crossref_metadata.assert_called_once_with(
            in_dir=download_dir,
            out_dir=transform_dir,
            batch_size=500,
            row_group_size=500_000,
            row_groups_per_file=4,
            max_workers=32,
            log_level=logging.INFO,
        )

    def test_transform_use_subset(self, mock_transform_parquets_task, mock_transform_crossref_metadata):
        from dmpworks.cli_utils import CrossrefMetadataTransformConfig

        bucket = "my-bucket"
        dataset = "crossref_metadata"
        run_id = "2025-01-01"

        crossref_metadata_module.transform(
            bucket_name=bucket,
            run_id=run_id,
            config=CrossrefMetadataTransformConfig(),
            use_subset=True,
        )

        mock_task = mock_transform_parquets_task["mock"]
        mock_task.assert_called_once_with(bucket, dataset, run_id, use_subset=True)
