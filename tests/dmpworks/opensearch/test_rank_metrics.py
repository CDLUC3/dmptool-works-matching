import pathlib

import pytest

from dmpworks.opensearch.rank_metrics import load_published_outputs_file, save_published_outputs_file


class TestPublishedOutputsFileRoundtrip:
    @pytest.mark.parametrize(
        ("pairs", "expected"),
        [
            pytest.param(
                [("10.0/dmp1", "10.0/work1"), ("10.0/dmp1", "10.0/work2"), ("10.0/dmp2", "10.0/work3")],
                {"10.0/dmp1": ["10.0/work1", "10.0/work2"], "10.0/dmp2": ["10.0/work3"]},
                id="multiple_dmps",
            ),
            pytest.param(
                [("10.0/dmp1", "10.0/work1")],
                {"10.0/dmp1": ["10.0/work1"]},
                id="single_pair",
            ),
            pytest.param(
                [],
                {},
                id="empty",
            ),
        ],
    )
    def test_roundtrip(self, tmp_path: pathlib.Path, pairs: list[tuple[str, str]], expected: dict[str, list[str]]):
        file_path = tmp_path / "anchors.csv"
        save_published_outputs_file(file_path, pairs)

        loaded = load_published_outputs_file(file_path)

        assert dict(loaded) == expected

    def test_preserves_work_order(self, tmp_path: pathlib.Path):
        file_path = tmp_path / "anchors.csv"
        pairs = [("10.0/dmp1", f"10.0/work{i}") for i in range(5)]
        save_published_outputs_file(file_path, pairs)

        loaded = load_published_outputs_file(file_path)

        assert loaded["10.0/dmp1"] == [f"10.0/work{i}" for i in range(5)]
