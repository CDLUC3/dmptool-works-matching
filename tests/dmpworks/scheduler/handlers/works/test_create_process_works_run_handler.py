"""Unit tests for create_process_works_run_handler."""

from __future__ import annotations

from unittest.mock import patch

from dmpworks.scheduler.handler.works.create_process_works_run_handler import create_process_works_run_handler

PATCH_BASE = "dmpworks.scheduler.handler.works.create_process_works_run_handler"

BASE_EVENT = {
    "release_date": "2025-01-13",
    "aws_env": "dev",
    "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-process-works:abc123",
    "run_id_sqlmesh_prev": "INITIAL",
    "run_id_openalex_works": "20250115T060000-a1b2c3d4",
    "run_id_datacite": "20250110T060000-b2c3d4e5",
    "run_id_crossref_metadata": "20250108T060000-c3d4e5f6",
    "run_id_ror": "20250105T060000-d4e5f6a7",
    "run_id_data_citation_corpus": "20250103T060000-e5f6a7b8",
    "release_date_openalex_works": "2025-01-10",
    "release_date_datacite": "2025-01-08",
    "release_date_crossref_metadata": "2025-01-05",
    "release_date_ror": "2025-01-03",
    "release_date_data_citation_corpus": "2025-01-01",
}


class TestCreateRun:
    """create_process_works_run_handler creates the DynamoDB record and returns run_id."""

    def test_calls_create_with_correct_args(self):
        with (
            patch(f"{PATCH_BASE}.create_process_works_run") as mock_create,
            patch(f"{PATCH_BASE}.generate_run_id", return_value="20250113T060000-aabbccdd"),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = create_process_works_run_handler(BASE_EVENT, None)

        mock_create.assert_called_once_with(
            release_date="2025-01-13",
            run_id="20250113T060000-aabbccdd",
            execution_arn="arn:aws:states:us-east-1:123456789012:execution:dmpworks-dev-process-works:abc123",
            run_id_sqlmesh_prev="INITIAL",
            run_id_openalex_works="20250115T060000-a1b2c3d4",
            run_id_datacite="20250110T060000-b2c3d4e5",
            run_id_crossref_metadata="20250108T060000-c3d4e5f6",
            run_id_ror="20250105T060000-d4e5f6a7",
            run_id_data_citation_corpus="20250103T060000-e5f6a7b8",
            release_date_openalex_works="2025-01-10",
            release_date_datacite="2025-01-08",
            release_date_crossref_metadata="2025-01-05",
            release_date_ror="2025-01-03",
            release_date_data_citation_corpus="2025-01-01",
        )

    def test_returns_event_merged_with_run_id(self):
        with (
            patch(f"{PATCH_BASE}.create_process_works_run"),
            patch(f"{PATCH_BASE}.generate_run_id", return_value="20250113T060000-aabbccdd"),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            result = create_process_works_run_handler(BASE_EVENT, None)

        assert result["run_id"] == "20250113T060000-aabbccdd"
        assert result["release_date"] == "2025-01-13"
        assert result["run_id_openalex_works"] == "20250115T060000-a1b2c3d4"

    def test_execution_arn_passed_from_event(self):
        """execution_arn comes from the event, not hardcoded."""
        custom_arn = "arn:aws:states:us-west-2:999:execution:sm:custom"
        event = {**BASE_EVENT, "execution_arn": custom_arn}

        with (
            patch(f"{PATCH_BASE}.create_process_works_run") as mock_create,
            patch(f"{PATCH_BASE}.generate_run_id", return_value="r"),
            patch(f"{PATCH_BASE}.LambdaEnvSettings"),
        ):
            create_process_works_run_handler(event, None)

        assert mock_create.call_args.kwargs["execution_arn"] == custom_arn
