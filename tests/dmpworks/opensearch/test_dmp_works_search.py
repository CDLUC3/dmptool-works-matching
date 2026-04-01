import pathlib

from dmpworks.utils import JsonlGzBatchWriter
from tests.utils import read_jsonl_gz


def make_match_record(
    dmp_doi: str = "10.0000/dmp",
    work_doi: str = "10.0000/work",
) -> dict:
    """Build a minimal JSONL match record."""
    work = {"doi": work_doi, "hash": "a" * 64}
    return {
        "dmpDoi": dmp_doi,
        "works": [
            {
                "work": work,
                "score": 0.9,
                "scoreMax": 1.0,
                "doiMatch": {"found": False, "score": 0.0, "sources": []},
                "contentMatch": {"score": 0.0, "titleHighlight": None, "abstractHighlights": []},
                "authorMatches": [],
                "institutionMatches": [],
                "funderMatches": [],
                "awardMatches": [],
                "intraWorkDoiMatches": [],
                "possibleSharedProjectDoiMatches": [],
                "datasetCitationDoiMatches": [],
            }
        ],
    }


class TestMatchDataJsonlRoundtrip:
    def test_roundtrip(self, tmp_path: pathlib.Path):
        record = make_match_record()
        with JsonlGzBatchWriter(output_dir=tmp_path, records_per_file=100) as writer:
            writer.write_record(record)

        files = sorted(tmp_path.glob("*.jsonl.gz"))
        assert len(files) == 1
        result = read_jsonl_gz(files[0])
        assert len(result) == 1
        assert result[0]["dmpDoi"] == "10.0000/dmp"
        assert result[0]["works"][0]["work"]["doi"] == "10.0000/work"
        assert result[0]["works"][0]["score"] == 0.9

    def test_multiple_files_on_rotation(self, tmp_path: pathlib.Path):
        records = [make_match_record(dmp_doi=f"10.0000/dmp{i}") for i in range(3)]
        with JsonlGzBatchWriter(output_dir=tmp_path, records_per_file=1) as writer:
            for r in records:
                writer.write_record(r)

        jsonl_files = sorted(tmp_path.glob("*.jsonl.gz"))
        assert len(jsonl_files) == 3

        all_records = []
        for f in jsonl_files:
            all_records.extend(read_jsonl_gz(f))
        assert len(all_records) == 3
