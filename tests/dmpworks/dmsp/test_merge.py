import logging
from unittest.mock import MagicMock

import pytest

from dmpworks.dmsp.merge import merge_related_works
from tests.dmpworks.dmsp.test_related_works import make_match_data_row, write_match_data


@pytest.fixture
def mock_loader(mocker):
    """Mock RelatedWorksLoader, capturing rows passed to insert methods."""
    mock_cls = mocker.patch("dmpworks.dmsp.merge.RelatedWorksLoader")
    instance = mock_cls.return_value.__enter__.return_value
    instance.captured_work_versions = []
    instance.captured_related_works = []

    def capture_work_versions(rows, **_kw):
        instance.captured_work_versions.append(list(rows))

    def capture_related_works(rows, **_kw):
        instance.captured_related_works.append(list(rows))

    instance.insert_work_versions.side_effect = capture_work_versions
    instance.insert_related_works.side_effect = capture_related_works
    return instance


class TestMergeRelatedWorks:
    def test_processes_each_dmp_separately(self, tmp_path, mock_loader):
        """Each DMP gets its own staging + update cycle."""
        matches_dir = write_match_data(
            tmp_path,
            [
                make_match_data_row(dmp_doi="10.0000/dmp1", work_doi="10.0000/work1"),
                make_match_data_row(dmp_doi="10.0000/dmp2", work_doi="10.0000/work2"),
            ],
        )

        merge_related_works(matches_dir=matches_dir, conn=MagicMock())

        assert mock_loader.prepare_staging_tables.call_count == 2
        assert mock_loader.run_update_procedure.call_count == 2

    def test_normalizes_dmp_doi_in_staging(self, tmp_path, mock_loader):
        """Bare lowercase dmpDoi is normalized to https://doi.org/UPPERCASE."""
        matches_dir = write_match_data(
            tmp_path,
            [
                make_match_data_row(dmp_doi="10.1234/abc", work_doi="10.0000/work1"),
            ],
        )

        merge_related_works(matches_dir=matches_dir, conn=MagicMock())

        # Related work rows are lists; dmpDoi is at index 1
        related_work_rows = mock_loader.captured_related_works[0]
        assert len(related_work_rows) == 1
        assert related_work_rows[0][1] == "https://doi.org/10.1234/ABC"

    def test_calls_cleanup_once_after_all_dmps(self, tmp_path, mock_loader):
        """Orphan cleanup runs exactly once, after all DMP batches."""
        matches_dir = write_match_data(
            tmp_path,
            [
                make_match_data_row(dmp_doi="10.0000/dmp1", work_doi="10.0000/work1"),
                make_match_data_row(dmp_doi="10.0000/dmp2", work_doi="10.0000/work2"),
            ],
        )

        merge_related_works(matches_dir=matches_dir, conn=MagicMock())

        assert mock_loader.run_cleanup_procedure.call_count == 1

    def test_only_loads_work_versions_referenced_by_dmp(self, tmp_path, mock_loader):
        """Each DMP batch only stages the work versions it references."""
        matches_dir = write_match_data(
            tmp_path,
            [
                make_match_data_row(dmp_doi="10.0000/dmp1", work_doi="10.0000/shared"),
                make_match_data_row(dmp_doi="10.0000/dmp1", work_doi="10.0000/only-dmp1"),
                make_match_data_row(dmp_doi="10.0000/dmp2", work_doi="10.0000/shared"),
                make_match_data_row(dmp_doi="10.0000/dmp2", work_doi="10.0000/only-dmp2"),
            ],
        )

        merge_related_works(matches_dir=matches_dir, conn=MagicMock())

        # Each batch should have exactly 2 work versions (shared + unique)
        assert len(mock_loader.captured_work_versions) == 2
        for batch in mock_loader.captured_work_versions:
            assert len(batch) == 2, f"Expected 2 work versions per DMP batch, got {len(batch)}"

    def test_commits_after_each_dmp(self, tmp_path, mock_loader):
        """Each DMP cycle ends with a commit()."""
        matches_dir = write_match_data(
            tmp_path,
            [
                make_match_data_row(dmp_doi="10.0000/dmp1", work_doi="10.0000/work1"),
                make_match_data_row(dmp_doi="10.0000/dmp2", work_doi="10.0000/work2"),
            ],
        )

        merge_related_works(matches_dir=matches_dir, conn=MagicMock())

        assert mock_loader.commit.call_count == 2


class TestMergeMetrics:
    def test_logs_timing_stats_after_completion(self, tmp_path, mock_loader, caplog):
        """Final INFO log contains mean/stdev for each step."""
        matches_dir = write_match_data(
            tmp_path,
            [
                make_match_data_row(dmp_doi="10.0000/dmp1", work_doi="10.0000/work1"),
                make_match_data_row(dmp_doi="10.0000/dmp2", work_doi="10.0000/work2"),
            ],
        )

        with caplog.at_level(logging.INFO, logger="dmpworks.dmsp.merge"):
            merge_related_works(matches_dir=matches_dir, conn=MagicMock())

        for step_name in ("stage-tables", "work-versions", "related-works", "update-proc", "cleanup"):
            assert any(
                step_name in record.message and "mean=" in record.message for record in caplog.records
            ), f"Expected timing log for step '{step_name}'"
