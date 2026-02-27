import pathlib
import pytest

from dmpworks.cli import cli

CLI_MODULE = "dmpworks.sql.commands"


class TestSQLMeshCLI:

    @pytest.fixture
    def mock_run_plan(self, mocker):
        # Patch in original location as this is imported in the command function
        return mocker.patch(f"{CLI_MODULE}.run_plan")

    def test_sqlmesh_plan(self, mock_run_plan):
        cli(["sqlmesh", "plan"])

        mock_run_plan.assert_called_once()

    @pytest.fixture
    def mock_run_test(self, mocker):
        # Patch in original location as this is imported in the command function
        return mocker.patch(f"{CLI_MODULE}.run_test")

    def test_sqlmesh_test(self, mock_run_test):
        cli(["sqlmesh", "test"])

        mock_run_test.assert_called_once()

    @pytest.fixture
    def mock_init_doi_state(self, mocker):
        # Patch in original location as this is imported in the command function
        return mocker.patch(f"{CLI_MODULE}.init_doi_state")

    def test_sqlmesh_init_doi_state_with_path(self, mock_init_doi_state, tmp_path: pathlib.Path):
        # Create a path but DO NOT touch/create the file,
        # to satisfy Cyclopts' validator (exists=False)
        parquet_file = tmp_path / "doi_state.parquet"

        cli(["sqlmesh", "init-doi-state", str(parquet_file)])

        # Cyclopts passes the resolved pathlib.Path object to the function
        mock_init_doi_state.assert_called_once_with(parquet_file)
