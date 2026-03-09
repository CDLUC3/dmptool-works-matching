import json
import pathlib

from dmpworks.dmsp.related_works import (
    create_related_works,
    create_work_versions,
)
from dmpworks.opensearch.dmp_works_search import MATCH_DATA_SCHEMA
from dmpworks.utils import ParquetBatchWriter, read_parquet_files


def write_match_data(tmp_path: pathlib.Path, rows: list[dict]) -> pathlib.Path:
    """Write match data rows to a directory and return the directory path."""
    matches_dir = tmp_path / "matches"
    matches_dir.mkdir()
    with ParquetBatchWriter(
        output_dir=matches_dir,
        schema=MATCH_DATA_SCHEMA,
        row_group_size=10_000,
        row_groups_per_file=4,
    ) as writer:
        writer.write_rows(rows)
    return matches_dir


def make_match_data_row(
    dmp_doi: str = "10.0000/dmp",
    work_doi: str = "10.0000/work",
    work_hash: str = "a" * 64,
    score: float = 0.8,
    score_max: float = 1.0,
) -> dict:
    """Build a minimal match data Parquet row."""
    work = {
        "doi": work_doi,
        "hash": work_hash,
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
    }
    return {
        "dmpDoi": dmp_doi,
        "work": json.dumps(work),
        "score": score,
        "scoreMax": score_max,
        "doiMatch": json.dumps({"found": False, "score": 0.0, "sources": []}),
        "contentMatch": json.dumps({"score": 0.0, "titleHighlight": None, "abstractHighlights": []}),
        "authorMatches": json.dumps([]),
        "institutionMatches": json.dumps([]),
        "funderMatches": json.dumps([]),
        "awardMatches": json.dumps([]),
        "intraWorkDoiMatches": json.dumps([]),
        "possibleSharedProjectDoiMatches": json.dumps([]),
        "datasetCitationDoiMatches": json.dumps([]),
    }


class TestCreateWorkVersions:
    def test_basic(self, tmp_path: pathlib.Path):
        matches_dir = write_match_data(tmp_path, [make_match_data_row()])
        out_dir = tmp_path / "work_versions"
        out_dir.mkdir()

        create_work_versions(matches_dir, out_dir)

        rows = list(read_parquet_files([out_dir]))
        assert len(rows) == 1
        assert rows[0]["doi"] == "10.0000/work"
        assert rows[0]["workType"] == "article"
        assert rows[0]["sourceName"] == "OpenAlex"

    def test_deduplicates_by_doi(self, tmp_path: pathlib.Path):
        # Two match rows for the same work DOI
        input_rows = [
            make_match_data_row(dmp_doi="10.0000/dmp1"),
            make_match_data_row(dmp_doi="10.0000/dmp2"),
        ]
        matches_dir = write_match_data(tmp_path, input_rows)
        out_dir = tmp_path / "work_versions"
        out_dir.mkdir()

        create_work_versions(matches_dir, out_dir)

        result = list(read_parquet_files([out_dir]))
        assert len(result) == 1

    def test_multiple_distinct_works(self, tmp_path: pathlib.Path):
        input_rows = [
            make_match_data_row(work_doi="10.0000/work1"),
            make_match_data_row(work_doi="10.0000/work2"),
        ]
        matches_dir = write_match_data(tmp_path, input_rows)
        out_dir = tmp_path / "work_versions"
        out_dir.mkdir()

        create_work_versions(matches_dir, out_dir)

        result = list(read_parquet_files([out_dir]))
        assert len(result) == 2
        dois = {r["doi"] for r in result}
        assert dois == {"10.0000/work1", "10.0000/work2"}

    def test_writes_multiple_files_on_rotation(self, tmp_path: pathlib.Path):
        input_rows = [make_match_data_row(work_doi=f"10.0000/work{i}") for i in range(3)]
        matches_dir = write_match_data(tmp_path, input_rows)
        out_dir = tmp_path / "work_versions"
        out_dir.mkdir()

        # row_group_size=1, row_groups_per_file=1 → one file per unique work
        create_work_versions(matches_dir, out_dir, row_group_size=1, row_groups_per_file=1)

        parquet_files = sorted(out_dir.glob("*.parquet"))
        assert len(parquet_files) == 3
        assert len(list(read_parquet_files([out_dir]))) == 3


class TestCreateRelatedWorks:
    def test_basic(self, tmp_path: pathlib.Path):
        matches_dir = write_match_data(tmp_path, [make_match_data_row()])
        out_dir = tmp_path / "related_works"
        out_dir.mkdir()

        create_related_works(matches_dir, out_dir)

        rows = list(read_parquet_files([out_dir]))
        assert len(rows) == 1
        assert rows[0]["dmpDoi"] == "10.0000/dmp"
        assert rows[0]["workDoi"] == "10.0000/work"
        assert rows[0]["planId"] is None
        assert rows[0]["sourceType"] == "SYSTEM_MATCHED"
        assert rows[0]["score"] == 0.8
        assert rows[0]["scoreMax"] == 1.0

    def test_preserves_json_match_fields(self, tmp_path: pathlib.Path):
        matches_dir = write_match_data(tmp_path, [make_match_data_row()])
        out_dir = tmp_path / "related_works"
        out_dir.mkdir()

        create_related_works(matches_dir, out_dir)

        rows = list(read_parquet_files([out_dir]))
        doi_match = json.loads(rows[0]["doiMatch"])
        assert doi_match["found"] is False

    def test_one_row_per_input_row(self, tmp_path: pathlib.Path):
        input_rows = [
            make_match_data_row(dmp_doi="10.0000/dmp1", work_doi="10.0000/work1"),
            make_match_data_row(dmp_doi="10.0000/dmp2", work_doi="10.0000/work2"),
        ]
        matches_dir = write_match_data(tmp_path, input_rows)
        out_dir = tmp_path / "related_works"
        out_dir.mkdir()

        create_related_works(matches_dir, out_dir)

        result = list(read_parquet_files([out_dir]))
        assert len(result) == 2

    def test_writes_multiple_files_on_rotation(self, tmp_path: pathlib.Path):
        input_rows = [make_match_data_row(dmp_doi=f"10.0000/dmp{i}") for i in range(3)]
        matches_dir = write_match_data(tmp_path, input_rows)
        out_dir = tmp_path / "related_works"
        out_dir.mkdir()

        create_related_works(matches_dir, out_dir, row_group_size=1, row_groups_per_file=1)

        parquet_files = sorted(out_dir.glob("*.parquet"))
        assert len(parquet_files) == 3
        assert len(list(read_parquet_files([out_dir]))) == 3
