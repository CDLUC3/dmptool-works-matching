from collections.abc import Callable, Generator
from concurrent.futures import ProcessPoolExecutor
import logging
import multiprocessing as mp
from multiprocessing import synchronize
from multiprocessing.sharedctypes import Synchronized
import os
import pathlib
import random
import time

import pyarrow as pa
import simdjson
from tqdm import tqdm

from dmpworks.utils import ParquetBatchWriter, to_batches

log = logging.getLogger(__name__)

# Shared multiprocessing objects
SHARED_FILES_PROCESSED: Synchronized | None = None
SHARED_COUNTER_LOCK: synchronize.Lock | None = None
SHARED_ABORT_EVENT: synchronize.Event | None = None


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
    file_prefix: str | None = None,
    log_level: int = logging.INFO,
):
    """Transform JSON-based input files (e.g. gzipped JSON Lines) into Parquet.

    Streams rows from one or more JSON inputs, applies a transformation
    function to each row, and writes the results to Parquet.

    Rows are accumulated in memory and written as a single Parquet row group
    (controlled by `row_group_size`). Multiple row groups can be written to the
    same Parquet file (controlled by `row_groups_per_file`), allowing multiple
    input files to be consolidated into fewer, larger Parquet files.

    For efficient downstream querying, target row group sizes of 128-512MB
    and file sizes of 512MB-1GB.

    Row groups are buffered fully in memory before being flushed to disk.
    Increasing `max_workers`, `row_group_size`, or `row_groups_per_file`
    will increase memory pressure and should be tuned accordingly.

    Args:
        files: List of input file paths.
        output_dir: Directory to write Parquet files to.
        batch_size: Number of files to process per batch.
        row_group_size: Number of rows per Parquet row group.
        row_groups_per_file: Number of row groups per Parquet file.
        schema: PyArrow schema for the output Parquet files.
        read_func: Function to read JSON objects from a file.
        transform_func: Function to transform a JSON object into a dictionary.
        tqdm_description: Description for the progress bar.
        max_workers: Maximum number of worker processes.
        file_prefix: Optional prefix for output filenames.
        log_level: Logging level.
    """
    log.debug("running process files")

    total_files = len(files)
    ctx = mp.get_context("spawn")
    shared_files_processed = ctx.Value("i", 0)
    shared_lock = ctx.Lock()
    last_seen_processed_count = 0
    abort_event = ctx.Event()

    # Shuffle files before batching.
    # OpenAlex orders files by `updated_date`, and recent files typically contain
    # significantly more records. If we process files sequentially, later batches
    # will be much larger than earlier ones, leading to workload imbalance where
    # only a subset of workers handle the heaviest batches.
    #
    # By shuffling first, we distribute high-volume and low-volume files more
    # evenly across batches, improving parallel load balancing.
    shuffled_files = random.sample(files, k=len(files))

    with (
        tqdm(
            total=total_files,
            desc=tqdm_description,
            unit="file",
        ) as pbar,
        ProcessPoolExecutor(
            mp_context=ctx,
            max_workers=max_workers,
            initializer=init_process_logs,
            initargs=(
                shared_files_processed,
                shared_lock,
                abort_event,
                log_level,
            ),
        ) as executor,
    ):
        futures = []
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
                    except Exception:
                        log.exception("Worker crashed! Signaling other workers to flush and exit")
                        abort_event.set()
                    finally:
                        finished_futures.append(future)

            for future in finished_futures:
                futures.remove(future)

            time.sleep(1)

    log.debug("finished process files")


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
    file_prefix: str | None = None,
):
    """Process a batch of input files, transforming and writing them to Parquet format.

    Iterates through the provided batch of input files, applies the transformation
    function to each record, and accumulates rows in memory.

    Buffering and File Rotation:
    - Rows are buffered in memory until `row_group_size` is reached. At this point,
      the buffer is flushed to disk as a single Parquet Row Group.
    - Multiple Row Groups are written to a single Parquet file until the count
      reaches `row_groups_per_file`. Once reached, the current file is closed,
      and a new file is started (incrementing the file part index).

    Args:
        batch_index: Index of the current batch.
        batch: List of input file paths in the batch.
        output_dir: Directory to write Parquet files to.
        schema: PyArrow schema for the output Parquet files.
        row_group_size: Number of rows per row group.
        row_groups_per_file: Number of row groups per file.
        read_func: Function to read JSON objects from a file.
        transform_func: Function to transform a JSON object into a dictionary.
        file_prefix: Optional prefix for output filenames.
    """
    current_input_file: pathlib.Path | None = None

    try:
        with ParquetBatchWriter(
            output_dir=output_dir,
            schema=schema,
            row_group_size=row_group_size,
            row_groups_per_file=row_groups_per_file,
            batch_index=batch_index,
            file_prefix=file_prefix,
        ) as writer:
            for input_file in batch:
                for obj in read_func(input_file):
                    if SHARED_ABORT_EVENT.is_set():
                        break

                    transformed_obj = transform_func(obj)

                    # Clear original reference for simdjson parser
                    obj = None  # noqa: PLW2901

                    if transformed_obj is not None:
                        writer.write_rows([transformed_obj])

                # Increment file counter
                with SHARED_COUNTER_LOCK:
                    SHARED_FILES_PROCESSED.value += 1

            if SHARED_ABORT_EVENT.is_set():
                log.warning(
                    f"Batch {batch_index} aborted."
                    f"{' Flushing remaining buffer to disk.' if writer.has_buffered_rows else ''}"
                )

    except Exception:
        # This logs a full traceback from inside the worker process
        log.exception(
            "Worker crashed in batch_index=%s while processing file=%s",
            batch_index,
            str(current_input_file) if current_input_file else None,
        )
        raise
