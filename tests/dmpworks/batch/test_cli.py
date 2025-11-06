import pathlib
import tempfile
from contextlib import contextmanager
from unittest.mock import call, MagicMock, patch

import pytest

from dmpworks.batch.tasks import DownloadTaskContext
from dmpworks.cli import cli


##############
# Crossref Metadata
##############


@pytest.fixture
def mock_download_source_task():
    data = {}

    @contextmanager
    def _mocked(bucket_name: str, dataset: str, task_id: str):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data["temp_path"] = pathlib.Path(tmp_dir)
            ctx = DownloadTaskContext(
                download_dir=pathlib.Path(tmp_dir) / dataset / task_id / "download",
                target_uri=f"s3://{bucket_name}/{dataset}/{task_id}/download/",
            )
            yield ctx

    mock_wrapper = MagicMock(side_effect=_mocked)

    with patch("dmpworks.batch.crossref_metadata.download_source_task", mock_wrapper):
        yield {"data": data, "mock": mock_wrapper}


@pytest.fixture
def mock_run_process(mocker):
    return mocker.patch("dmpworks.batch.crossref_metadata.run_process")


def test_crossref_metadata_download(mock_download_source_task, mock_run_process):
    bucket = "my-bucket"
    dataset = "crossref_metadata"
    task_id = "2025-01-01"
    archive_name = "April_2025_Public_Data_File_from_Crossref.tar"
    cli(
        [
            "aws-batch",
            "crossref-metadata",
            "download",
            bucket,
            task_id,
            archive_name,
        ]
    )

    mock = mock_download_source_task["mock"]
    mock.assert_called_once_with(
        "my-bucket",
        "crossref_metadata",
        "2025-01-01",
    )

    temp_path = mock_download_source_task.get("data", {}).get("temp_path")
    download_dir = temp_path / dataset / task_id / "download"
    archive_path: pathlib.Path = download_dir / archive_name

    mock_run_process.assert_has_calls(
        [
            call(
                [
                    "s5cmd",
                    "--request-payer",
                    "requester",
                    "cp",
                    f"s3://api-snapshots-reqpays-crossref/{archive_name}",
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
