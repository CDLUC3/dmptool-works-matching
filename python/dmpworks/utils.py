from collections.abc import Callable, Generator, Mapping
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import gzip
import importlib
import json
import logging
from multiprocessing import log_to_stderr
import os
import pathlib
import shlex
import shutil
import subprocess
import zipfile

import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
import simdjson
from urllib3 import Retry

log = logging.getLogger(__name__)


def thread_map[T, R](fn: Callable[[T], R], items: list[T], *, max_workers: int = 5) -> list[R]:
    """Apply fn to each item in parallel using threads, returning results in input order.

    Args:
        fn: Function to apply to each item.
        items: Items to process.
        max_workers: Maximum number of concurrent threads.

    Returns:
        List of results in the same order as items.
    """
    if not items:
        return []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(items))) as pool:
        return list(pool.map(fn, items))


def timed(func):
    """Log execution time of a function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = pendulum.now()
        try:
            return func(*args, **kwargs)
        finally:
            end = pendulum.now()
            diff = end - start
            log.info(f"Execution time: {diff.in_words()}")

    return wrapper


def run_process(
    args: list[str],
    env: Mapping[str, str] | None = None,
):
    """Run a shell script.

    Args:
        args: The command and arguments to run.
        env: Environment variables to set for the process.
    """
    log.info(f"run_process command: `{shlex.join(args)}`")

    with subprocess.Popen(  # noqa: S603
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
        shell=False,
    ) as proc:
        for line in proc.stdout:
            log.info(line)

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, args)


def copy_dict(original_dict: dict, keys_to_remove: list) -> dict:
    """Create a copy of a dictionary with specific keys removed.

    Args:
        original_dict: The dictionary to copy.
        keys_to_remove: A list of keys to exclude from the copy.

    Returns:
        A new dictionary containing all items from the original dictionary except those with keys in keys_to_remove.
    """
    return {k: v for k, v in original_dict.items() if k not in keys_to_remove}


def to_batches[T](items: list[T], batch_size: int) -> Generator[list[T], None, None]:
    """Yield successive batches from a list.

    Args:
        items: The list of items to batch.
        batch_size: The size of each batch.

    Yields:
        A generator yielding lists of items of size batch_size.
    """
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def retry_session(
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
    raise_on_status: bool = True,
) -> requests.Session:
    """Create a requests session with retry logic.

    Args:
        total_retries: Total number of retries to allow.
        backoff_factor: A backoff factor to apply between attempts.
        status_forcelist: A set of HTTP status codes that we should force a retry on.
        raise_on_status: Whether to raise an exception on status codes.

    Returns:
        A requests.Session object configured with the specified retry strategy.
    """
    retry_strategy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=raise_on_status,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def import_from_path(path: str):
    """Import a module or attribute from a string path.

    Args:
        path: The dotted path to the module or attribute (e.g., 'package.module.attribute').

    Returns:
        The imported module or attribute.
    """
    module_path, attr_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def setup_multiprocessing_logging(log_level: int):
    """Setup logging for multiprocessing.

    Args:
        log_level: The logging level.
    """
    logging.basicConfig(
        level=log_level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] [%(threadName)s] %(message)s"
    )
    if log_level == logging.DEBUG:
        # Make multi-processing print logs
        log_to_stderr(logging.DEBUG)


def yield_objects_from_jsonl(file_path: pathlib.Path) -> Generator[simdjson.Object, None, None]:
    """Yields JSON objects from a plain or gzipped JSON lines file.

    Args:
        file_path: the path to the file.

    Returns: generator.

    """
    parser = simdjson.Parser()
    opener = gzip.open if file_path.suffix == ".gz" else open
    line_num = 0

    with opener(file_path, "rb") as f:
        for line in f:
            line_num += 1

            # Skip emtpy lines
            if not line.strip():
                continue

            try:
                row = parser.parse(line)
                yield row
            except ValueError:
                log.exception(f"yield_jsonl: error parsing line {line_num} in {file_path}")
                continue
            finally:
                # Clear original reference for simdjson parser
                row = None


def yield_objects_from_json(file_path: pathlib.Path) -> Generator[simdjson.Object, None, None]:
    """Yields JSON objects from a plain or gzipped JSON file.

    Args:
        file_path: the path to the file.

    Returns: generator.

    """
    parser = simdjson.Parser()

    if file_path.suffix == ".gz":
        with gzip.open(file_path, "rb") as f:
            content = f.read()
            doc = parser.parse(content)
    else:
        doc = parser.load(file_path)

    if isinstance(doc, list):
        yield from doc
    else:
        yield doc


def output_file_name(batch_index: int, file_index: int, file_prefix: str | None = None) -> str:
    """Generate a Parquet output filename using batch and file indices.

    Because files may be written concurrently by multiple processes, each
    filename is namespaced by a `batch_index` to avoid collisions. The
    `file_index` distinguishes multiple output files produced within the
    same batch.

    Args:
        batch_index: integer identifying the process-specific batch.
        file_index: integer identifying the file within the batch.
        file_prefix: an optional file prefix.

    Returns: the generated Parquet filename as a string.

    """
    parts = []
    if file_prefix is not None:
        parts.append(file_prefix)
    parts.append(f"batch_{batch_index:05d}_part_{file_index:05d}.parquet")
    return "".join(parts)


def debug_arrow_type_error(row_buffer: list[dict], schema: pa.Schema) -> None:
    """Iterate through a buffer of rows to find and log the row causing a PyArrow ArrowTypeError."""
    for row in row_buffer:
        try:
            pa.Table.from_pylist([row], schema=schema)
        except pa.lib.ArrowTypeError:
            log.exception("PyArrow Type Error")
            log.exception(f"Offending Row Data: {row}")
            break


class ParquetBatchWriter:
    """Incrementally write rows to Parquet files with row group and file rotation.

    Buffers rows in memory and writes them to disk as Parquet row groups once
    `row_group_size` is reached. When a file accumulates `row_groups_per_file`
    row groups, it is closed and a new file is opened.

    Filenames are generated using `output_file_name(batch_index, file_index, file_prefix)`.

    Use as a context manager: on clean exit `close()` is called, flushing any
    remaining buffered rows. On exception the underlying writer is closed without
    flushing, preserving the original error semantics of `transform_json_to_parquet`.

    Args:
        output_dir: Directory to write Parquet files into.
        schema: PyArrow schema for the output files.
        row_group_size: Number of rows per Parquet row group.
        row_groups_per_file: Number of row groups before rotating to a new file.
        batch_index: Namespace prefix for filenames (avoids collisions across workers).
        file_prefix: Optional string prefix prepended to every output filename.
    """

    def __init__(
        self,
        *,
        output_dir: pathlib.Path,
        schema: pa.Schema,
        row_group_size: int,
        row_groups_per_file: int,
        batch_index: int = 0,
        file_prefix: str | None = None,
    ):
        """Initialize the writer.

        Args:
            output_dir: Directory to write Parquet files into.
            schema: PyArrow schema for the output files.
            row_group_size: Number of rows per Parquet row group.
            row_groups_per_file: Number of row groups before rotating to a new file.
            batch_index: Namespace prefix for filenames.
            file_prefix: Optional string prefix for output filenames.
        """
        self.output_dir = output_dir
        self.schema = schema
        self.row_group_size = row_group_size
        self.row_groups_per_file = row_groups_per_file
        self.batch_index = batch_index
        self.file_prefix = file_prefix
        output_dir.mkdir(parents=True, exist_ok=True)
        self.file_index = 0
        self.num_row_groups = 0
        self.row_buffer: list[dict] = []
        self.writer: pq.ParquetWriter | None = None

    @property
    def has_buffered_rows(self) -> bool:
        """Return True if there are rows waiting to be flushed."""
        return bool(self.row_buffer)

    def ensure_writer(self) -> None:
        """Open a new ParquetWriter for the current file index if one is not already open."""
        if self.writer is None:
            output_file = self.output_dir / output_file_name(
                self.batch_index, self.file_index, file_prefix=self.file_prefix
            )
            self.writer = pq.ParquetWriter(output_file, schema=self.schema, compression="snappy")

    def flush_row_group(self) -> None:
        """Write the next row group to disk and rotate to a new file if the limit is reached."""
        self.ensure_writer()
        rows_to_write = self.row_buffer[: self.row_group_size]
        del self.row_buffer[: self.row_group_size]
        try:
            table = pa.Table.from_pylist(rows_to_write, schema=self.schema)
        except pa.lib.ArrowTypeError:
            debug_arrow_type_error(rows_to_write, self.schema)
            raise
        self.writer.write_table(table)
        self.num_row_groups += 1

        if self.num_row_groups >= self.row_groups_per_file:
            self.writer.close()
            self.writer = None
            self.file_index += 1
            self.num_row_groups = 0

    def write_rows(self, rows: list[dict]) -> None:
        """Buffer rows and flush a row group to disk when row_group_size is reached.

        Args:
            rows: Rows to add to the buffer.
        """
        self.row_buffer.extend(rows)
        while len(self.row_buffer) >= self.row_group_size:
            self.flush_row_group()

    def close(self) -> None:
        """Flush any remaining buffered rows and close the underlying writer."""
        if self.row_buffer:
            self.ensure_writer()
            try:
                table = pa.Table.from_pylist(self.row_buffer, schema=self.schema)
            except pa.lib.ArrowTypeError:
                debug_arrow_type_error(self.row_buffer, self.schema)
                raise
            self.writer.write_table(table)
            self.row_buffer.clear()

        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def __enter__(self):
        """Enter the context manager.

        Returns:
            self
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager.

        Args:
            exc_type: Exception type, or None on clean exit.
            exc_val: Exception value.
            exc_tb: Exception traceback.
        """
        if exc_type is None:
            self.close()
        elif self.writer is not None:
            # On exception, close the underlying writer without flushing remaining rows.
            self.writer.close()
            self.writer = None


class JsonlGzBatchWriter:
    """Write JSON records to gzip-compressed JSONL files with file rotation.

    Each call to `write_record` writes a single JSON line to the current
    `.jsonl.gz` file. After `records_per_file` records the current file is
    closed and a new one is opened.

    Args:
        output_dir: Directory to write .jsonl.gz files into.
        records_per_file: Number of records per file before rotating.
        file_prefix: Prefix for output filenames.
    """

    def __init__(
        self,
        *,
        output_dir: pathlib.Path,
        records_per_file: int = 1000,
        file_prefix: str = "matches",
    ):
        """Initialize the writer.

        Args:
            output_dir: Directory to write .jsonl.gz files into.
            records_per_file: Number of records per file before rotating.
            file_prefix: Prefix for output filenames.
        """
        self.output_dir = output_dir
        self.records_per_file = records_per_file
        self.file_prefix = file_prefix
        output_dir.mkdir(parents=True, exist_ok=True)
        self._file_index = 0
        self._record_count = 0
        self._file: gzip.GzipFile | None = None

    def _open_new_file(self):
        """Close the current file (if any) and open a new .jsonl.gz file."""
        if self._file is not None:
            self._file.close()
        path = self.output_dir / f"{self.file_prefix}_{self._file_index:04d}.jsonl.gz"
        self._file = gzip.open(path, mode="wb")  # noqa: SIM115
        self._file_index += 1
        self._record_count = 0

    def write_record(self, record: dict):
        """Write a single JSON record as one line.

        Args:
            record: Dictionary to serialize as JSON.
        """
        if self._file is None:
            self._open_new_file()
        self._file.write(json.dumps(record, separators=(",", ":")).encode("utf-8") + b"\n")
        self._record_count += 1
        if self._record_count >= self.records_per_file:
            self._file.close()
            self._file = None

    def close(self):
        """Close the current file."""
        if self._file is not None:
            self._file.close()
            self._file = None
        elif self._file_index == 0:
            # No records were written; create an empty .jsonl.gz so downstream uploads succeed
            path = self.output_dir / f"{self.file_prefix}_{self._file_index:04d}.jsonl.gz"
            with gzip.open(path, mode="wb"):
                pass

    def __enter__(self):
        """Enter the context manager.

        Returns:
            self
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager.

        Args:
            exc_type: Exception type, or None on clean exit.
            exc_val: Exception value.
            exc_tb: Exception traceback.
        """
        self.close()


def write_rows_to_parquet(rows: list[dict], output_file: pathlib.Path, schema: pa.Schema) -> None:
    """Write a list of rows to a single Parquet file.

    Args:
        rows: Rows to write.
        output_file: Destination file path.
        schema: PyArrow schema for the output file.
    """
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, output_file, compression="snappy")


def read_parquet_files(paths: list[pathlib.Path]) -> Generator[dict, None, None]:
    """Read rows from one or more Parquet files or directories.

    Args:
        paths: List of Parquet file paths or directories. Directories are
            expanded to all ``*.parquet`` files sorted by name.

    Yields:
        Row dictionaries.
    """
    for path in paths:
        files = sorted(path.glob("*.parquet")) if path.is_dir() else [path]
        for file in files:
            yield from pq.read_table(file).to_pylist()


def extract_zip_to_gzip(file_path: pathlib.Path) -> list[pathlib.Path]:
    """Extract JSON files from a ZIP archive, compressing each directly to gzip.

    Args:
        file_path: Path to the ZIP file.

    Returns:
        List of paths to the gzipped output files.
    """
    out_paths = []
    with zipfile.ZipFile(file_path) as zf:
        log.info(f"Files in archive: {file_path}")
        for name in zf.namelist():
            log.info(name)
            if not name.lower().endswith(".json"):
                continue
            out_path = file_path.parent / (pathlib.Path(name).name + ".gz")
            with zf.open(name) as src, gzip.open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
            log.info(f"Compressed {name} into {out_path.name}")
            out_paths.append(out_path)
    return out_paths


def fetch_datacite_aws_credentials(
    *, account_id: str | None = None, password: str | None = None
) -> tuple[str, str, str]:
    """Fetches DataCite AWS credentials.

    Retrieves credentials from the provided arguments or environment variables, then
    exchanges them for temporary AWS credentials via the DataCite API.

    Args:
        account_id: DataCite account ID. Falls back to ``DATACITE_ACCOUNT_ID`` env var if not provided.
        password: DataCite password. Falls back to ``DATACITE_PASSWORD`` env var if not provided.

    Returns:
        A tuple containing (access_key_id, secret_access_key, session_token).

    Raises:
        RuntimeError: If credentials are missing or the API request fails.
    """
    account_id = account_id or os.getenv("DATACITE_ACCOUNT_ID")
    password = password or os.getenv("DATACITE_PASSWORD")

    if not account_id or not password:
        raise RuntimeError(
            "DataCite account_id and password must be provided or set via DATACITE_ACCOUNT_ID and DATACITE_PASSWORD environment variables."
        )

    url = "https://api.datacite.org/credentials/datafile"

    try:
        response = requests.get(url, auth=(account_id, password), timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError("Failed to fetch DataCite credentials") from e

    try:
        data = response.json()
        access_key_id = data["access_key_id"]
        secret_access_key = data["secret_access_key"]
        session_token = data["session_token"]
    except (KeyError, ValueError) as e:
        raise RuntimeError("Unexpected response format from DataCite credentials endpoint.") from e

    return access_key_id, secret_access_key, session_token
