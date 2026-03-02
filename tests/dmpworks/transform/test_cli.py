import logging
import os
import pathlib

import pytest

from dmpworks.cli import cli

CLI_MODULE = "dmpworks.transform.cli"


class TestTransformCLI:
    @pytest.fixture
    def mock_transform_crossref_metadata(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.transform_crossref_metadata")

    def test_transform_crossref_metadata(self, mock_transform_crossref_metadata, tmp_path: pathlib.Path):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        in_dir.mkdir()
        out_dir.mkdir()

        cli(["transform", "crossref-metadata", str(in_dir), str(out_dir)])

        mock_transform_crossref_metadata.assert_called_once_with(
            in_dir=in_dir,
            out_dir=out_dir,
            log_level=logging.INFO,
            batch_size=500,
            row_group_size=500_000,
            row_groups_per_file=4,
            max_workers=os.cpu_count(),
        )

    @pytest.fixture
    def mock_transform_datacite(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.transform_datacite")

    def test_transform_datacite(self, mock_transform_datacite, tmp_path: pathlib.Path):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        in_dir.mkdir()
        out_dir.mkdir()

        cli(["transform", "datacite", str(in_dir), str(out_dir)])

        mock_transform_datacite.assert_called_once_with(
            in_dir=in_dir,
            out_dir=out_dir,
            log_level=logging.INFO,
            batch_size=150,
            row_group_size=250_000,
            row_groups_per_file=8,
            max_workers=8,
        )

    @pytest.fixture
    def mock_transform_openalex_works(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.transform_openalex_works")

    def test_transform_openalex_works(self, mock_transform_openalex_works, tmp_path: pathlib.Path):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        in_dir.mkdir()
        out_dir.mkdir()

        cli(["transform", "openalex-works", str(in_dir), str(out_dir)])

        mock_transform_openalex_works.assert_called_once_with(
            in_dir=in_dir,
            out_dir=out_dir,
            log_level=logging.INFO,
            batch_size=16,
            row_group_size=250_000,
            row_groups_per_file=4,
            max_workers=os.cpu_count(),
        )

    @pytest.fixture
    def mock_create_dataset_subset(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.create_dataset_subset")

    @pytest.fixture
    def mock_load_institutions(self, mocker):
        return mocker.patch(
            f"{CLI_MODULE}.load_institutions",
            return_value=[{"name": "University of California, Berkeley", "ror": "01an7q238"}],
        )

    @pytest.fixture
    def mock_load_dois(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.load_dois", return_value=["10.0000/abc"])

    def test_dataset_subset_with_dois(
        self,
        mock_create_dataset_subset,
        mock_load_institutions,
        mock_load_dois,
        tmp_path: pathlib.Path,
    ):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        inst_path = tmp_path / "institutions.json"
        dois_path = tmp_path / "dois.json"

        in_dir.mkdir()
        out_dir.mkdir()

        # Cyclopts validators require exists=True for file paths
        inst_path.touch()
        dois_path.touch()

        cli(
            [
                "transform",
                "dataset-subset",
                "openalex-works",
                str(in_dir),
                str(out_dir),
                str(inst_path),
                "--dois-path",
                str(dois_path),
            ]
        )

        mock_load_institutions.assert_called_once_with(inst_path)
        mock_load_dois.assert_called_once_with(dois_path)
        mock_create_dataset_subset.assert_called_once_with(
            dataset="openalex-works",
            in_dir=in_dir,
            out_dir=out_dir,
            institutions=[{"name": "University of California, Berkeley", "ror": "01an7q238"}],
            dois=["10.0000/abc"],
            log_level=logging.INFO,
        )

    def test_dataset_subset_without_dois(
        self,
        mock_create_dataset_subset,
        mock_load_institutions,
        tmp_path: pathlib.Path,
    ):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        inst_path = tmp_path / "institutions.json"

        in_dir.mkdir()
        out_dir.mkdir()
        inst_path.touch()

        cli(
            [
                "transform",
                "dataset-subset",
                "datacite",
                str(in_dir),
                str(out_dir),
                str(inst_path),
            ]
        )

        mock_load_institutions.assert_called_once_with(inst_path)
        mock_create_dataset_subset.assert_called_once_with(
            dataset="datacite",
            in_dir=in_dir,
            out_dir=out_dir,
            institutions=[{"name": "University of California, Berkeley", "ror": "01an7q238"}],
            dois=[],
            log_level=logging.INFO,
        )
