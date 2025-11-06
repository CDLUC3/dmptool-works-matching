import logging
import os
import pathlib

import pytest
from opensearchpy import OpenSearch

from dmpworks.cli import cli
from dmpworks.opensearch.utils import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.utils import InstanceOf


##############
# OpenSearch
##############


@pytest.fixture
def mock_create_index(mocker):
    return mocker.patch("dmpworks.opensearch.cli.create_index")


def test_opensearch_create_index(mock_create_index, tmp_path: pathlib.Path):
    in_dir = tmp_path / "input"
    in_dir.mkdir()

    cli(["opensearch", "create-index", "works-index", "works-mapping.json"])

    mock_create_index.assert_called_once_with(
        InstanceOf(OpenSearch),
        "works-index",
        "works-mapping.json",
    )


@pytest.fixture
def mock_sync_works(mocker):
    return mocker.patch("dmpworks.opensearch.cli.sync_works")


def test_opensearch_sync_works(mock_sync_works, tmp_path: pathlib.Path):
    in_dir = tmp_path / "input"
    in_dir.mkdir()

    cli(["opensearch", "sync-works", "works-index", str(in_dir)])

    mock_sync_works.assert_called_once_with(
        "works-index",
        in_dir,
        OpenSearchClientConfig(),
        OpenSearchSyncConfig(),
        log_level=logging.INFO,
    )


###########
# SQLMesh
###########


@pytest.fixture
def mock_run_plan(mocker):
    # Patch in original location as this is imported in the command function
    return mocker.patch("dmpworks.sql.commands.run_plan")


def test_sqlmesh_plan(mock_run_plan):
    cli(["sqlmesh", "plan"])

    mock_run_plan.assert_called_once()


@pytest.fixture
def mock_run_test(mocker):
    # Patch in original location as this is imported in the command function
    return mocker.patch("dmpworks.sql.commands.run_test")


def test_sqlmesh_test(mock_run_test):
    cli(["sqlmesh", "test"])

    mock_run_test.assert_called_once()


############
# Transform
############


@pytest.fixture
def mock_transform_crossref_metadata(mocker):
    return mocker.patch("dmpworks.transform.cli.transform_crossref_metadata")


def test_transform_crossref_metadata(mock_transform_crossref_metadata, tmp_path: pathlib.Path):
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "output"
    in_dir.mkdir()
    out_dir.mkdir()

    cli(["transform", "crossref-metadata", str(in_dir), str(out_dir)])

    mock_transform_crossref_metadata.assert_called_once_with(
        in_dir,
        out_dir,
        batch_size=os.cpu_count(),
        extract_workers=1,
        transform_workers=2,
        cleanup_workers=1,
        extract_queue_size=0,
        transform_queue_size=3,
        cleanup_queue_size=0,
        max_file_processes=os.cpu_count(),
        n_batches=None,
        low_memory=False,
    )


@pytest.fixture
def mock_transform_datacite(mocker):
    return mocker.patch("dmpworks.transform.cli.transform_datacite")


def test_transform_datacite(mock_transform_datacite, tmp_path: pathlib.Path):
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "output"
    in_dir.mkdir()
    out_dir.mkdir()

    cli(["transform", "datacite", str(in_dir), str(out_dir)])

    mock_transform_datacite.assert_called_once_with(
        in_dir,
        out_dir,
        batch_size=os.cpu_count(),
        extract_workers=1,
        transform_workers=2,
        cleanup_workers=1,
        extract_queue_size=0,
        transform_queue_size=10,
        cleanup_queue_size=0,
        max_file_processes=os.cpu_count(),
        n_batches=None,
        low_memory=False,
    )


@pytest.fixture
def mock_transform_openalex_funders(mocker):
    return mocker.patch("dmpworks.transform.cli.transform_openalex_funders")


def test_transform_openalex_funders(mock_transform_openalex_funders, tmp_path: pathlib.Path):
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "output"
    in_dir.mkdir()
    out_dir.mkdir()

    cli(["transform", "openalex-funders", str(in_dir), str(out_dir)])

    mock_transform_openalex_funders.assert_called_once_with(
        in_dir,
        out_dir,
        batch_size=os.cpu_count(),
        extract_workers=1,
        transform_workers=1,
        cleanup_workers=1,
        extract_queue_size=0,
        transform_queue_size=2,
        cleanup_queue_size=0,
        max_file_processes=os.cpu_count(),
        n_batches=None,
        low_memory=False,
    )


@pytest.fixture
def mock_transform_openalex_works(mocker):
    return mocker.patch("dmpworks.transform.cli.transform_openalex_works")


def test_transform_openalex_works(mock_transform_openalex_works, tmp_path: pathlib.Path):
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "output"
    in_dir.mkdir()
    out_dir.mkdir()

    cli(["transform", "openalex-works", str(in_dir), str(out_dir)])

    mock_transform_openalex_works.assert_called_once_with(
        in_dir,
        out_dir,
        batch_size=os.cpu_count(),
        extract_workers=1,
        transform_workers=1,
        cleanup_workers=1,
        extract_queue_size=0,
        transform_queue_size=2,
        cleanup_queue_size=0,
        max_file_processes=os.cpu_count(),
        n_batches=None,
        low_memory=False,
    )


@pytest.fixture
def mock_transform_ror(mocker):
    return mocker.patch("dmpworks.transform.cli.transform_ror")


def test_transform_ror(mock_transform_ror, tmp_path: pathlib.Path):
    ror_v2_json_file = tmp_path / "ror_v2_json_file.json"
    out_dir = tmp_path / "output"
    ror_v2_json_file.touch()
    out_dir.mkdir()

    cli(["transform", "ror", str(ror_v2_json_file), str(out_dir)])

    mock_transform_ror.assert_called_once_with(
        ror_v2_json_file,
        out_dir,
    )


@pytest.fixture
def mock_create_demo_dataset(mocker):
    return mocker.patch("dmpworks.transform.cli.create_demo_dataset")


def test_demo_dataset(mock_create_demo_dataset, tmp_path: pathlib.Path):
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "output"
    in_dir.mkdir()
    out_dir.mkdir()

    cli(
        [
            "transform",
            "demo-dataset",
            "openalex-works",
            "01an7q238",
            str(in_dir),
            str(out_dir),
            "--institution-name",
            "University of California, Berkeley",
        ]
    )
    mock_create_demo_dataset.assert_called_once_with(
        "openalex-works",
        "01an7q238",
        "University of California, Berkeley",
        in_dir,
        out_dir,
        logging.INFO,
    )
