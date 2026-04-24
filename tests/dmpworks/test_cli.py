from dmpworks.cli import cli
import pytest


@pytest.fixture
def subcommand_tokens(mocker, tmp_path):
    """Mock the transform subcommand and return tokens that invoke it."""
    mocker.patch("dmpworks.transform.crossref_metadata.transform_crossref_metadata")
    in_dir = tmp_path / "in"
    out_dir = tmp_path / "out"
    in_dir.mkdir()
    out_dir.mkdir()
    return ["transform", "crossref-metadata", str(in_dir), str(out_dir)]


class TestCLIEnvFile:
    def test_env_file_flag_loads_dotenv(self, mocker, subcommand_tokens, tmp_path):
        mock_load_dotenv = mocker.patch("dmpworks.cli.load_dotenv")
        env_file = tmp_path / ".env.test"
        env_file.touch()

        cli.meta(["--env-file", str(env_file)] + subcommand_tokens)

        mock_load_dotenv.assert_called_once_with(dotenv_path=env_file, override=False)

    def test_dmpworks_env_var_loads_dotenv(self, mocker, monkeypatch, subcommand_tokens, tmp_path):
        mock_load_dotenv = mocker.patch("dmpworks.cli.load_dotenv")
        env_file = tmp_path / ".env.test"
        env_file.touch()

        monkeypatch.setenv("DMPWORKS_ENV_FILE", str(env_file))

        cli.meta(subcommand_tokens)

        mock_load_dotenv.assert_called_once_with(dotenv_path=env_file, override=False)

    def test_missing_env_file_skips_load(self, mocker, subcommand_tokens, tmp_path):
        mock_load_dotenv = mocker.patch("dmpworks.cli.load_dotenv")
        nonexistent = tmp_path / ".env.missing"

        cli.meta(["--env-file", str(nonexistent)] + subcommand_tokens)

        mock_load_dotenv.assert_not_called()
