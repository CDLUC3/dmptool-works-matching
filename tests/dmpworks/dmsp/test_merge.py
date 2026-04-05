import logging
import pathlib
from unittest.mock import MagicMock

import pymysql.err
import pytest

from dmpworks.dmsp.merge import merge_related_works
from dmpworks.utils import JsonlGzBatchWriter

PLAN_ID_MAP = {
    "10.0000/dmp": 1,
    "10.0000/dmp1": 10,
    "10.0000/dmp2": 20,
    "10.1234/abc": 99,
}


def make_jsonl_record(
    dmp_doi: str = "10.0000/dmp",
    work_dois: list[str] | None = None,
) -> dict:
    """Build a JSONL match record with one or more works."""
    if work_dois is None:
        work_dois = ["10.0000/work"]
    works = []
    for work_doi in work_dois:
        works.append(
            {
                "work": {
                    "doi": work_doi,
                    "hash": "a" * 64,
                    "workType": "article",
                    "publicationDate": "2023-01-01",
                    "title": "Test Work",
                    "abstractText": "An abstract.",
                    "authors": [],
                    "institutions": [],
                    "funders": [],
                    "awards": [],
                    "publicationVenue": None,
                    "source": {"name": "OpenAlex", "url": "https://openalex.org"},
                },
                "score": 0.8,
                "scoreMax": 1.0,
                "doiMatch": {"found": False, "score": 0.0, "sources": []},
                "contentMatch": {"score": 0.0, "titleHighlight": None, "abstractHighlights": []},
                "authorMatches": [],
                "institutionMatches": [],
                "funderMatches": [],
                "awardMatches": [],
            }
        )
    return {"dmpDoi": dmp_doi, "works": works}


def write_jsonl_match_data(tmp_path: pathlib.Path, records: list[dict]) -> pathlib.Path:
    """Write JSONL match records and return the directory path."""
    matches_dir = tmp_path / "matches"
    matches_dir.mkdir()
    with JsonlGzBatchWriter(output_dir=matches_dir, records_per_file=10_000) as writer:
        for record in records:
            writer.write_record(record)
    return matches_dir


@pytest.fixture
def mock_loader(mocker):
    """Mock RelatedWorksLoader, capturing rows passed to insert methods."""
    mock_cls = mocker.patch("dmpworks.dmsp.merge.RelatedWorksLoader")
    instance = mock_cls.return_value
    instance.captured_work_versions = []
    instance.captured_related_works = []

    def capture_work_versions(rows, **_kw):
        instance.captured_work_versions.append(list(rows))

    def capture_related_works(rows, **_kw):
        instance.captured_related_works.append(list(rows))

    instance.insert_work_versions.side_effect = capture_work_versions
    instance.insert_related_works.side_effect = capture_related_works

    mocker.patch("dmpworks.dmsp.merge.fetch_plan_id_mapping", return_value=PLAN_ID_MAP)
    mocker.patch("dmpworks.dmsp.merge.make_connection", return_value=MagicMock())

    return instance


class TestMergeRelatedWorks:
    def test_processes_each_dmp_separately(self, tmp_path, mock_loader):
        """Each DMP gets its own staging + update cycle."""
        matches_dir = write_jsonl_match_data(
            tmp_path,
            [
                make_jsonl_record(dmp_doi="10.0000/dmp1", work_dois=["10.0000/work1"]),
                make_jsonl_record(dmp_doi="10.0000/dmp2", work_dois=["10.0000/work2"]),
            ],
        )

        merge_related_works(matches_dir, mysql_config=MagicMock())

        assert mock_loader.prepare_staging_tables.call_count == 2
        assert mock_loader.run_update_procedure.call_count == 2

    def test_resolves_plan_id_from_mapping(self, tmp_path, mock_loader):
        """Plan ID is resolved from the prefetched mapping and set in the staging row."""
        matches_dir = write_jsonl_match_data(
            tmp_path,
            [make_jsonl_record(dmp_doi="10.1234/abc", work_dois=["10.0000/work1"])],
        )

        merge_related_works(matches_dir, mysql_config=MagicMock())

        # Related work rows are lists; planId is at index 0
        related_work_rows = mock_loader.captured_related_works[0]
        assert len(related_work_rows) == 1
        assert related_work_rows[0][0] == 99

    def test_calls_cleanup_once_after_all_dmps(self, tmp_path, mock_loader):
        """Orphan cleanup runs exactly once, after all DMP batches."""
        matches_dir = write_jsonl_match_data(
            tmp_path,
            [
                make_jsonl_record(dmp_doi="10.0000/dmp1", work_dois=["10.0000/work1"]),
                make_jsonl_record(dmp_doi="10.0000/dmp2", work_dois=["10.0000/work2"]),
            ],
        )

        merge_related_works(matches_dir, mysql_config=MagicMock())

        assert mock_loader.run_cleanup_procedure.call_count == 1

    def test_only_loads_work_versions_referenced_by_dmp(self, tmp_path, mock_loader):
        """Each DMP batch only stages the work versions it references."""
        matches_dir = write_jsonl_match_data(
            tmp_path,
            [
                make_jsonl_record(dmp_doi="10.0000/dmp1", work_dois=["10.0000/shared", "10.0000/only-dmp1"]),
                make_jsonl_record(dmp_doi="10.0000/dmp2", work_dois=["10.0000/shared", "10.0000/only-dmp2"]),
            ],
        )

        merge_related_works(matches_dir, mysql_config=MagicMock())

        # Each batch should have exactly 2 work versions (shared + unique)
        assert len(mock_loader.captured_work_versions) == 2
        for batch in mock_loader.captured_work_versions:
            assert len(batch) == 2, f"Expected 2 work versions per DMP batch, got {len(batch)}"

    def test_commits_after_each_dmp(self, tmp_path, mock_loader):
        """Each DMP cycle ends with a commit()."""
        matches_dir = write_jsonl_match_data(
            tmp_path,
            [
                make_jsonl_record(dmp_doi="10.0000/dmp1", work_dois=["10.0000/work1"]),
                make_jsonl_record(dmp_doi="10.0000/dmp2", work_dois=["10.0000/work2"]),
            ],
        )

        merge_related_works(matches_dir, mysql_config=MagicMock())

        assert mock_loader.commit.call_count == 2


class TestDeadlockRetry:
    def test_retries_on_deadlock_and_succeeds(self, tmp_path, mock_loader, mocker):
        """A deadlock on the first attempt is retried and succeeds."""
        mocker.patch("dmpworks.dmsp.merge.time.sleep")
        matches_dir = write_jsonl_match_data(
            tmp_path,
            [make_jsonl_record(dmp_doi="10.0000/dmp1", work_dois=["10.0000/work1"])],
        )
        mock_loader.run_update_procedure.side_effect = [
            pymysql.err.OperationalError(1213, "Deadlock found"),
            None,  # succeeds on retry
        ]

        merge_related_works(matches_dir, mysql_config=MagicMock())

        assert mock_loader.run_update_procedure.call_count == 2
        assert mock_loader.commit.call_count == 1

    def test_counts_error_after_retry_exhaustion(self, tmp_path, mock_loader, mocker, caplog):
        """After max retries, the DMP is counted as an error and processing continues."""
        mocker.patch("dmpworks.dmsp.merge.time.sleep")
        matches_dir = write_jsonl_match_data(
            tmp_path,
            [
                make_jsonl_record(dmp_doi="10.0000/dmp1", work_dois=["10.0000/work1"]),
                make_jsonl_record(dmp_doi="10.0000/dmp2", work_dois=["10.0000/work2"]),
            ],
        )
        # First DMP: always deadlocks (4 attempts = 1 initial + 3 retries)
        # Second DMP: succeeds
        mock_loader.run_update_procedure.side_effect = [
            pymysql.err.OperationalError(1213, "Deadlock found"),
            pymysql.err.OperationalError(1213, "Deadlock found"),
            pymysql.err.OperationalError(1213, "Deadlock found"),
            pymysql.err.OperationalError(1213, "Deadlock found"),
            None,  # second DMP succeeds
        ]

        with caplog.at_level(logging.INFO, logger="dmpworks.dmsp.merge"):
            merge_related_works(matches_dir, mysql_config=MagicMock())

        assert mock_loader.commit.call_count == 1  # only second DMP committed
        assert any("Failed 1 DMPs due to errors" in r.message for r in caplog.records)


class TestMergeMetrics:
    def test_logs_timing_stats_after_completion(self, tmp_path, mock_loader, caplog):
        """Final INFO log contains mean/stdev for each step."""
        matches_dir = write_jsonl_match_data(
            tmp_path,
            [
                make_jsonl_record(dmp_doi="10.0000/dmp1", work_dois=["10.0000/work1"]),
                make_jsonl_record(dmp_doi="10.0000/dmp2", work_dois=["10.0000/work2"]),
            ],
        )

        with caplog.at_level(logging.INFO, logger="dmpworks.dmsp.merge"):
            merge_related_works(matches_dir, mysql_config=MagicMock())

        for step_name in (
            "read",
            "stage-tables",
            "work-versions",
            "related-works",
            "update-proc",
            "cleanup",
        ):
            assert any(
                step_name in record.message and "mean=" in record.message for record in caplog.records
            ), f"Expected timing log for step '{step_name}'"
