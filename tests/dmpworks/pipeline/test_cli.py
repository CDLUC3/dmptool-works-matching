"""CLI dispatch tests for pipeline commands.

Tests the non-interactive pipeline commands. Interactive commands that use
questionary (cleanup-s3, delete-checkpoints) are not tested here.
"""

import io
import json

import pytest

from dmpworks.cli import cli


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch):
    for var in ["AWS_ENV", "BUCKET_NAME"]:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_REGION", "us-west-2")


class TestStartIngest:
    def test_dispatches_to_wizard(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_ingest_wizard")

        cli(["pipeline", "runs", "start", "ingest", "--env", "dev", "--bucket-name", "my-bucket"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="my-bucket")

    def test_defaults_bucket_name(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_ingest_wizard")

        cli(["pipeline", "runs", "start", "ingest", "--env", "dev"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="dmpworks-dev-s3")


class TestStartProcessWorks:
    def test_dispatches_to_wizard(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_process_works_wizard")

        cli(["pipeline", "runs", "start", "process-works", "--env", "dev", "--bucket-name", "my-bucket"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="my-bucket")

    def test_defaults_bucket_name(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_process_works_wizard")

        cli(["pipeline", "runs", "start", "process-works", "--env", "dev"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="dmpworks-dev-s3")


class TestStartProcessDmps:
    def test_dispatches_to_wizard(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_process_dmps_wizard")

        cli(["pipeline", "runs", "start", "process-dmps", "--env", "dev", "--bucket-name", "my-bucket"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="my-bucket")

    def test_defaults_bucket_name(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_process_dmps_wizard")

        cli(["pipeline", "runs", "start", "process-dmps", "--env", "dev"])

        mock_wizard.assert_called_once_with(env="dev", bucket_name="dmpworks-dev-s3")


class TestShowNewVersions:
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

        cli(["pipeline", "show", "new-versions", "--env", "dev"])

        mock_lambda.invoke.assert_called_once_with(
            FunctionName="dmpworks-dev-version-checker",
            Payload=json.dumps({"dry_run": True}),
        )


class TestSchedulesSubcommands:
    def test_list_calls_display(self, mocker):
        mock_fetch = mocker.patch("dmpworks.pipeline.aws.fetch_schedule_rules", return_value=[])
        mock_display = mocker.patch("dmpworks.pipeline.display.display_schedules")

        cli(["pipeline", "schedules", "list", "--env", "dev"])

        mock_fetch.assert_called_once_with(env="dev")
        mock_display.assert_called_once_with(rules=[])

    def test_pause_disables_rules(self, mocker):
        mock_toggle = mocker.patch("dmpworks.pipeline.aws.toggle_schedule_rules")
        mocker.patch("dmpworks.pipeline.aws.fetch_schedule_rules", return_value=[])
        mocker.patch("dmpworks.pipeline.display.display_schedules")

        cli(["pipeline", "schedules", "pause", "--env", "dev", "--rule", "version-checker-schedule"])

        mock_toggle.assert_called_once_with(env="dev", rule="version-checker-schedule", enable=False)

    def test_resume_enables_rules(self, mocker):
        mock_toggle = mocker.patch("dmpworks.pipeline.aws.toggle_schedule_rules")
        mocker.patch("dmpworks.pipeline.aws.fetch_schedule_rules", return_value=[])
        mocker.patch("dmpworks.pipeline.display.display_schedules")

        cli(["pipeline", "schedules", "resume", "--env", "dev", "--rule", "version-checker-schedule"])

        mock_toggle.assert_called_once_with(env="dev", rule="version-checker-schedule", enable=True)


class TestApproveRetry:
    def test_dispatches_to_wizard(self, mocker):
        mock_wizard = mocker.patch("dmpworks.pipeline.interactive.run_approve_retry_wizard")

        cli(["pipeline", "runs", "approve-retry", "--env", "dev"])

        mock_wizard.assert_called_once_with()
