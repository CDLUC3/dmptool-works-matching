"""Tests for store_approval_token_handler."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dmpworks.scheduler.handler.store_approval_token_handler import store_approval_token_handler


@pytest.fixture(autouse=True)
def _mock_env():
    with patch("dmpworks.scheduler.handler.store_approval_token_handler.LambdaEnvSettings"):
        yield


class TestStoreApprovalTokenHandler:
    @patch("dmpworks.scheduler.handler.store_approval_token_handler.set_process_dmps_run_status")
    def test_routes_to_process_dmps(self, mock_set_status):
        event = {
            "workflow_key": "process-dmps",
            "task_name": "sync-dmps",
            "approval_token": "token-abc",
            "run_date": "2026-03-28",
            "run_id": "20260328T120000-a1b2c3d4",
        }
        store_approval_token_handler(event, None)

        mock_set_status.assert_called_once_with(
            run_date="2026-03-28",
            run_id="20260328T120000-a1b2c3d4",
            status="WAITING_FOR_APPROVAL",
            approval_token="token-abc",
            approval_task_name="sync-dmps",
        )

    @patch("dmpworks.scheduler.handler.store_approval_token_handler.set_process_works_run_status")
    def test_routes_to_process_works(self, mock_set_status):
        event = {
            "workflow_key": "process-works",
            "task_name": "sqlmesh",
            "approval_token": "token-xyz",
            "run_date": "2026-03-28",
            "run_id": "20260328T120000-deadbeef",
        }
        store_approval_token_handler(event, None)

        mock_set_status.assert_called_once_with(
            run_date="2026-03-28",
            run_id="20260328T120000-deadbeef",
            status="WAITING_FOR_APPROVAL",
            approval_token="token-xyz",
            approval_task_name="sqlmesh",
        )

    @patch("dmpworks.scheduler.handler.store_approval_token_handler.update_release_status")
    def test_routes_to_dataset_release(self, mock_update):
        event = {
            "workflow_key": "openalex-works",
            "task_name": "download",
            "approval_token": "token-123",
            "publication_date": "2026-01-15",
        }
        store_approval_token_handler(event, None)

        mock_update.assert_called_once_with(
            dataset="openalex-works",
            publication_date="2026-01-15",
            status="WAITING_FOR_APPROVAL",
            approval_token="token-123",
            approval_task_name="download",
        )
