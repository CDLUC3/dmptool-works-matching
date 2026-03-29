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


class TestShowStatus:
    def test_queries_latest_per_group(self, mocker):
        mock_query = mocker.patch("dmpworks.scheduler.dynamodb_store.DatasetReleaseRecord.query", return_value=[])
        mock_get_cp = mocker.patch("dmpworks.scheduler.dynamodb_store.get_task_checkpoint", return_value=None)
        mocker.patch("dmpworks.scheduler.dynamodb_store.scan_all_process_works_runs", return_value=[])
        mocker.patch("dmpworks.scheduler.dynamodb_store.scan_all_process_dmps_runs", return_value=[])
        mocker.patch("dmpworks.pipeline.display.display_dataset_releases")
        mocker.patch("dmpworks.pipeline.display.display_task_checkpoints")
        mocker.patch("dmpworks.pipeline.display.display_process_works_runs")
        mocker.patch("dmpworks.pipeline.display.display_process_dmps_runs")

        cli(["pipeline", "show", "status", "--env", "dev"])

        # One query per dataset, last 2, newest-first — no date condition.
        assert mock_query.call_count == 5
        for call in mock_query.call_args_list:
            assert call.kwargs.get("limit") == 2
            assert call.kwargs.get("scan_index_forward") is False

        # One get_task_checkpoint per (workflow, task) combo: 5*3 + 2 + 4 = 21.
        assert mock_get_cp.call_count == 21

    def test_slices_process_runs(self, mocker):
        mocker.patch("dmpworks.scheduler.dynamodb_store.DatasetReleaseRecord.query", return_value=[])
        mocker.patch("dmpworks.scheduler.dynamodb_store.get_task_checkpoint", return_value=None)
        mocker.patch("dmpworks.pipeline.display.display_dataset_releases")
        mocker.patch("dmpworks.pipeline.display.display_task_checkpoints")

        def make_run(release_date, run_id):
            r = mocker.MagicMock()
            r.release_date = release_date
            r.run_id = run_id
            return r

        works_records = [make_run(f"2025-0{i}-01", f"run-{i}") for i in range(1, 11)]
        dmps_records = [make_run(f"2025-01-{i:02d}", f"run-{i}") for i in range(1, 21)]
        mocker.patch("dmpworks.scheduler.dynamodb_store.scan_all_process_works_runs", return_value=works_records)
        mocker.patch("dmpworks.scheduler.dynamodb_store.scan_all_process_dmps_runs", return_value=dmps_records)

        mock_display_works = mocker.patch("dmpworks.pipeline.display.display_process_works_runs")
        mock_display_dmps = mocker.patch("dmpworks.pipeline.display.display_process_dmps_runs")

        cli(["pipeline", "show", "status", "--env", "dev"])

        works_passed = mock_display_works.call_args.kwargs["records"]
        dmps_passed = mock_display_dmps.call_args.kwargs["records"]
        assert len(works_passed) == 3
        assert len(dmps_passed) == 7
        # Verify newest-first ordering.
        assert works_passed == sorted(works_passed, key=lambda r: (r.release_date, r.run_id), reverse=True)
        assert dmps_passed == sorted(dmps_passed, key=lambda r: (r.release_date, r.run_id), reverse=True)
