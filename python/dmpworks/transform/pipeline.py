import logging
import multiprocessing as mp
import os
import pathlib
import random
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Generator, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import simdjson
import time
from tqdm import tqdm

from dmpworks.utils import to_batches

log = logging.getLogger(__name__)

# Shared multiprocessing objects
SHARED_FILES_PROCESSED: Optional[mp.Value] = None
SHARED_COUNTER_LOCK: Optional[mp.Lock] = None
SHARED_ABORT_EVENT: Optional[mp.Event] = None


def init_process_logs(shared_files_processed: mp.Value, shared_lock: mp.Lock, abort_event: mp.Event, level: int):
    """Initialize global logging and shared multiprocessing objects for a worker process."""

    global SHARED_FILES_PROCESSED, SHARED_COUNTER_LOCK, SHARED_ABORT_EVENT

    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")

    SHARED_FILES_PROCESSED = shared_files_processed
    SHARED_COUNTER_LOCK = shared_lock
    SHARED_ABORT_EVENT = abort_event


def process_files(
    *,
    files: list[pathlib.Path],
    output_dir: pathlib.Path,
    batch_size: int,
    row_group_size: int,
    row_groups_per_file: int,
    schema: pa.lib.Schema,
    read_func: Callable[[pathlib.Path], Generator[simdjson.Object, None, None]],
    transform_func: Callable[[simdjson.Object], dict | None],
    tqdm_description: str = "Transforming Files",
    max_workers: int = os.cpu_count(),
    file_prefix: Optional[str] = None,
    log_level: int = logging.INFO,
):
    """
    Transform JSON-based input files (e.g. gzipped JSON Lines) into Parquet.

    Streams rows from one or more JSON inputs, applies a transformation
    function to each row, and writes the results to Parquet.

    Rows are accumulated in memory and written as a single Parquet row group
    (controlled by `row_group_size`). Multiple row groups can be written to the
    same Parquet file (controlled by `row_groups_per_file`), allowing multiple
    input files to be consolidated into fewer, larger Parquet files.

    For efficient downstream querying, target row group sizes of 128–512MB
    and file sizes of 512MB–1GB.

    Row groups are buffered fully in memory before being flushed to disk.
    Increasing `max_workers`, `row_group_size`, or `row_groups_per_file`
    will increase memory pressure and should be tuned accordingly.
    """

    log.debug("running process files")

    total_files = len(files)
    ctx = mp.get_context("spawn")
    shared_files_processed = ctx.Value("i", 0)
    shared_lock = ctx.Lock()
    last_seen_processed_count = 0
    abort_event = ctx.Event()
    shuffled_files = random.sample(files, k=len(files))

    with tqdm(
        total=total_files,
        desc=tqdm_description,
        unit="file",
    ) as pbar:
        with ProcessPoolExecutor(
            mp_context=ctx,
            max_workers=max_workers,
            initializer=init_process_logs,
            initargs=(
                shared_files_processed,
                shared_lock,
                abort_event,
                log_level,
            ),
        ) as executor:
            futures = []
            # natsorted
            for idx, batch in enumerate(to_batches(shuffled_files, batch_size=batch_size)):
                future = executor.submit(
                    transform_json_to_parquet,
                    batch_index=idx,
                    batch=batch,
                    output_dir=output_dir,
                    schema=schema,
                    row_group_size=row_group_size,
                    row_groups_per_file=row_groups_per_file,
                    read_func=read_func,
                    transform_func=transform_func,
                    file_prefix=file_prefix,
                )
                futures.append(future)

            while futures:
                # Update progress from shared file counter
                with shared_lock:
                    current_processed_count = shared_files_processed.value
                    delta = current_processed_count - last_seen_processed_count
                    if delta > 0:
                        pbar.update(delta)
                        last_seen_processed_count = current_processed_count

                finished_futures = []
                for future in futures:
                    if future.done():
                        try:
                            future.result()
                        except Exception as e:
                            log.error(f"Worker crashed! Signaling other workers to flush and exit. Error: {e}")
                            abort_event.set()
                        finally:
                            finished_futures.append(future)

                for future in finished_futures:
                    futures.remove(future)

                time.sleep(1)

    log.debug("finished process files")


def output_file_name(batch_index: int, file_index: int, file_prefix: Optional[str] = None) -> str:
    """
    Generate a Parquet output filename using batch and file indices.

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


def transform_json_to_parquet(
    *,
    batch_index: int,
    batch: list[pathlib.Path],
    output_dir: pathlib.Path,
    schema: pa.lib.Schema,
    row_group_size: int,
    row_groups_per_file: int,
    read_func: Callable[[pathlib.Path], Generator[simdjson.Object, None, None]],
    transform_func: Callable[[simdjson.Object], dict | None],
    file_prefix: Optional[str] = None,
):
    """
    Process a batch of input files, transforming and writing them to Parquet format.

    Iterates through the provided batch of input files, applies the transformation
    function to each record, and accumulates rows in memory.

    Buffering and File Rotation:
    - Rows are buffered in memory until `row_group_size` is reached. At this point,
      the buffer is flushed to disk as a single Parquet Row Group.
    - Multiple Row Groups are written to a single Parquet file until the count
      reaches `row_groups_per_file`. Once reached, the current file is closed,
      and a new file is started (incrementing the file part index).
    """

    file_index = 0
    num_row_groups = 0
    row_buffer = []
    writer = None
    current_input_file: pathlib.Path | None = None

    try:
        for input_file in batch:
            for obj in read_func(input_file):
                if SHARED_ABORT_EVENT.is_set():
                    break

                transformed_obj = transform_func(obj)

                # Clear original reference for simdjson parser
                obj = None

                if transformed_obj is not None:
                    row_buffer.append(transformed_obj)

                if len(row_buffer) >= row_group_size:
                    # Only create writer when we have data
                    if writer is None:
                        output_file = output_dir / output_file_name(
                            batch_index,
                            file_index,
                            file_prefix=file_prefix,
                        )
                        writer = pq.ParquetWriter(output_file, schema=schema, compression="snappy")

                    try:
                        table = pa.Table.from_pylist(row_buffer, schema=schema)
                    except pa.lib.ArrowTypeError:
                        debug_arrow_type_error(row_buffer, schema)
                        raise
                    writer.write_table(table)
                    row_buffer.clear()
                    num_row_groups += 1

                    # Check if the current file has reached its row group limit
                    if num_row_groups >= row_groups_per_file:
                        writer.close()
                        writer = None
                        file_index += 1
                        num_row_groups = 0

            # Increment file counter
            with SHARED_COUNTER_LOCK:
                SHARED_FILES_PROCESSED.value += 1

        if SHARED_ABORT_EVENT.is_set():
            log.warning(f"Batch {batch_index} aborted.{' Flushing remaining buffer to disk.' if row_buffer else ''}")

        # Flush any remaining data in the buffer after all files are processed
        if row_buffer:
            if writer is None:
                output_file = output_dir / output_file_name(
                    batch_index,
                    file_index,
                    file_prefix=file_prefix,
                )
                writer = pq.ParquetWriter(output_file, schema=schema, compression="snappy")

            try:
                table = pa.Table.from_pylist(row_buffer, schema=schema)
            except pa.lib.ArrowTypeError:
                debug_arrow_type_error(row_buffer, schema)
                raise
            writer.write_table(table)
            row_buffer.clear()

    except Exception:
        # This logs a full traceback from inside the worker process
        log.exception(
            "Worker crashed in batch_index=%s while processing file=%s",
            batch_index,
            str(current_input_file) if current_input_file else None,
        )
        raise
    finally:
        # Ensure last writer was closed
        if writer is not None:
            writer.close()


def debug_arrow_type_error(row_buffer: list[dict], schema: pa.Schema) -> None:
    """Iterates through a buffer of rows to find and logs the row causing a PyArrow ArrowTypeError."""

    for row in row_buffer:
        try:
            pa.Table.from_pylist([row], schema=schema)
        except pa.lib.ArrowTypeError as e:
            log.error(f"PyArrow Type Error: {e}")
            log.error(f"Offending Row Data: {row}")
            break
