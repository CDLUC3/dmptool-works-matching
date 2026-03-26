import pathlib

from dmpworks.utils import ParquetBatchWriter, read_parquet_files, run_process, write_rows_to_parquet
import pyarrow as pa
import pytest


class TestRunProcess:
    def test_logs_command(self, caplog):
        cmd = ["echo", "hello world"]
        with caplog.at_level("INFO"):
            run_process(cmd)

        out = caplog.text
        assert "run_process command: `echo 'hello world'`" in out


SIMPLE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("label", pa.string()),
    ]
)


class TestWriteReadParquet:
    def test_roundtrip(self, tmp_path: pathlib.Path):
        rows = [
            {"id": "a", "value": 1.0, "label": "first"},
            {"id": "b", "value": 2.5, "label": "second"},
        ]
        out = tmp_path / "test.parquet"
        write_rows_to_parquet(rows, out, SIMPLE_SCHEMA)

        result = list(read_parquet_files([out]))
        assert result == rows

    def test_nullable_fields(self, tmp_path: pathlib.Path):
        rows = [{"id": "x", "value": 0.0, "label": None}]
        out = tmp_path / "test.parquet"
        write_rows_to_parquet(rows, out, SIMPLE_SCHEMA)

        result = list(read_parquet_files([out]))
        assert result[0]["label"] is None

    def test_reads_multiple_files_and_directories(self, tmp_path: pathlib.Path):
        file_a = tmp_path / "a.parquet"
        file_b = tmp_path / "b.parquet"
        write_rows_to_parquet([{"id": "a", "value": 1.0, "label": "a"}], file_a, SIMPLE_SCHEMA)
        write_rows_to_parquet([{"id": "b", "value": 2.0, "label": "b"}], file_b, SIMPLE_SCHEMA)

        # Explicit file paths
        result = list(read_parquet_files([file_a, file_b]))
        assert len(result) == 2
        assert result[0]["id"] == "a"
        assert result[1]["id"] == "b"

        # Directory path
        result = list(read_parquet_files([tmp_path]))
        assert len(result) == 2

    def test_empty_rows(self, tmp_path: pathlib.Path):
        out = tmp_path / "empty.parquet"
        write_rows_to_parquet([], out, SIMPLE_SCHEMA)

        result = list(read_parquet_files([out]))
        assert result == []


def make_row(id: str = "a", value: float = 1.0, label: str = "x") -> dict:
    return {"id": id, "value": value, "label": label}


class TestParquetBatchWriter:
    def test_writes_rows_on_close(self, tmp_path: pathlib.Path):
        with ParquetBatchWriter(
            output_dir=tmp_path, schema=SIMPLE_SCHEMA, row_group_size=100, row_groups_per_file=4
        ) as writer:
            writer.write_rows([make_row("a"), make_row("b")])

        result = list(read_parquet_files([tmp_path]))
        assert len(result) == 2
        assert result[0]["id"] == "a"
        assert result[1]["id"] == "b"

    def test_single_file_below_rotation_threshold(self, tmp_path: pathlib.Path):
        # 3 row groups, rotation at 4 — should produce exactly one file
        rows = [make_row(str(i)) for i in range(3)]
        with ParquetBatchWriter(
            output_dir=tmp_path, schema=SIMPLE_SCHEMA, row_group_size=1, row_groups_per_file=4
        ) as writer:
            writer.write_rows(rows)

        parquet_files = sorted(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 1
        assert len(list(read_parquet_files([tmp_path]))) == 3

    def test_rotates_file_after_row_groups_per_file(self, tmp_path: pathlib.Path):
        # row_group_size=1, row_groups_per_file=1 → one file per row
        rows = [make_row(str(i)) for i in range(3)]
        with ParquetBatchWriter(
            output_dir=tmp_path, schema=SIMPLE_SCHEMA, row_group_size=1, row_groups_per_file=1
        ) as writer:
            writer.write_rows(rows)

        parquet_files = sorted(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 3
        assert len(list(read_parquet_files([tmp_path]))) == 3

    def test_output_filenames_use_batch_and_file_index(self, tmp_path: pathlib.Path):
        rows = [make_row(str(i)) for i in range(2)]
        with ParquetBatchWriter(
            output_dir=tmp_path,
            schema=SIMPLE_SCHEMA,
            row_group_size=1,
            row_groups_per_file=1,
            batch_index=7,
        ) as writer:
            writer.write_rows(rows)

        names = {f.name for f in tmp_path.glob("*.parquet")}
        assert "batch_00007_part_00000.parquet" in names
        assert "batch_00007_part_00001.parquet" in names

    def test_file_prefix(self, tmp_path: pathlib.Path):
        with ParquetBatchWriter(
            output_dir=tmp_path,
            schema=SIMPLE_SCHEMA,
            row_group_size=100,
            row_groups_per_file=4,
            file_prefix="works_",
        ) as writer:
            writer.write_rows([make_row()])

        names = list(tmp_path.glob("*.parquet"))
        assert len(names) == 1
        assert names[0].name.startswith("works_")

    def test_has_buffered_rows(self, tmp_path: pathlib.Path):
        writer = ParquetBatchWriter(output_dir=tmp_path, schema=SIMPLE_SCHEMA, row_group_size=10, row_groups_per_file=4)
        assert not writer.has_buffered_rows
        writer.write_rows([make_row()])
        assert writer.has_buffered_rows
        writer.close()
        assert not writer.has_buffered_rows

    def test_no_file_written_for_empty_input(self, tmp_path: pathlib.Path):
        with ParquetBatchWriter(output_dir=tmp_path, schema=SIMPLE_SCHEMA, row_group_size=100, row_groups_per_file=4):
            pass

        assert list(tmp_path.glob("*.parquet")) == []

    def test_exception_does_not_flush_remaining_rows(self, tmp_path: pathlib.Path):
        # 10 rows buffered, row_group_size=100 so none flushed yet — exception should
        # close the writer without writing the buffered rows.
        rows = [make_row(str(i)) for i in range(10)]
        with (
            pytest.raises(RuntimeError),
            ParquetBatchWriter(
                output_dir=tmp_path, schema=SIMPLE_SCHEMA, row_group_size=100, row_groups_per_file=4
            ) as writer,
        ):
            writer.write_rows(rows)
            raise RuntimeError("boom")

        assert list(read_parquet_files([tmp_path])) == []

    def test_write_rows_called_multiple_times(self, tmp_path: pathlib.Path):
        with ParquetBatchWriter(
            output_dir=tmp_path, schema=SIMPLE_SCHEMA, row_group_size=2, row_groups_per_file=4
        ) as writer:
            writer.write_rows([make_row("a")])
            writer.write_rows([make_row("b")])
            writer.write_rows([make_row("c")])

        result = list(read_parquet_files([tmp_path]))
        assert len(result) == 3
        assert {r["id"] for r in result} == {"a", "b", "c"}
