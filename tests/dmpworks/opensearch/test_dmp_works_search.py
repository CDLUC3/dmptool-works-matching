import json
import pathlib

from dmpworks.opensearch.dmp_works_search import MATCH_DATA_SCHEMA
from dmpworks.utils import ParquetBatchWriter, read_parquet_files
import pytest


def make_match_data_row(
    dmp_doi: str = "10.0000/dmp",
    work_doi: str = "10.0000/work",
) -> dict:
    """Build a minimal match data Parquet row."""
    work = {"doi": work_doi, "hash": "a" * 64}
    return {
        "dmpDoi": dmp_doi,
        "work": json.dumps(work),
        "score": 0.9,
        "scoreMax": 1.0,
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


class TestMatchDataSchema:
    def test_roundtrip(self, tmp_path: pathlib.Path):
        row = make_match_data_row()
        with ParquetBatchWriter(
            output_dir=tmp_path,
            schema=MATCH_DATA_SCHEMA,
            row_group_size=100,
            row_groups_per_file=4,
        ) as writer:
            writer.write_rows([row])

        result = list(read_parquet_files([tmp_path]))
        assert len(result) == 1
        assert result[0]["dmpDoi"] == "10.0000/dmp"
        assert result[0]["score"] == pytest.approx(0.9)
        assert json.loads(result[0]["work"])["doi"] == "10.0000/work"

    def test_multiple_files_on_rotation(self, tmp_path: pathlib.Path):
        # row_group_size=1, row_groups_per_file=1 → one file per row
        rows = [make_match_data_row(dmp_doi=f"10.0000/dmp{i}") for i in range(3)]
        with ParquetBatchWriter(
            output_dir=tmp_path,
            schema=MATCH_DATA_SCHEMA,
            row_group_size=1,
            row_groups_per_file=1,
        ) as writer:
            writer.write_rows(rows)

        parquet_files = sorted(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 3

        result = list(read_parquet_files([tmp_path]))
        assert len(result) == 3
