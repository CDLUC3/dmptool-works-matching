"""CLI dispatch tests for pipeline commands.

Tests the non-interactive pipeline commands. Interactive commands that use
questionary (cleanup, delete-checkpoints) are not tested here.
"""

import io
import json

from dmpworks.cli import cli
import pytest


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch):
    for var in ["AWS_ENV", "AWS_REGION", "BUCKET_NAME"]:
        monkeypatch.delenv(var, raising=False)


class TestRunIngest:
    def test_dispatches_to_wizard(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_ingest_wizard")

        cli(["pipeline", "run", "ingest", "--env", "dev", "--bucket-name", "my-bucket"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="my-bucket")


class TestRunProcessWorks:
    def test_dispatches_to_wizard(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_process_works_wizard")

        cli(["pipeline", "run", "process-works", "--env", "dev", "--bucket-name", "my-bucket"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="my-bucket")


class TestRunProcessDmps:
    def test_dispatches_to_wizard(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_process_dmps_wizard")

        cli(["pipeline", "run", "process-dmps", "--env", "dev", "--bucket-name", "my-bucket"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="my-bucket")


class TestCheckVersions:
    def test_invokes_lambda_with_dry_run(self, mocker):
        mock_boto = mocker.patch("boto3.client")
        mock_lambda = mock_boto.return_value
        mock_lambda.invoke.return_value = {
            "Payload": io.BytesIO(json.dumps({"versions": []}).encode()),
        }
        mocker.patch("dmpworks.pipeline.display.display_discovered_versions")
        mocker.patch(
            "dmpworks.pipeline.aws.get_lambda_function_name",
            return_value="dmpworks-dev-version-checker",
        )

        cli(["pipeline", "check-versions", "--env", "dev"])

        mock_lambda.invoke.assert_called_once_with(
            FunctionName="dmpworks-dev-version-checker",
            Payload=json.dumps({"dry_run": True}),
        )
