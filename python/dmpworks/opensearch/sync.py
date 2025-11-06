import json
import logging
import math
import multiprocessing as mp
import os
import pathlib
import queue
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from multiprocessing import current_process
from typing import Callable, Iterator, List, Optional, TypedDict

import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
from opensearchpy import OpenSearch
from opensearchpy.helpers import streaming_bulk
from tqdm import tqdm

from dmpworks.opensearch.utils import (
    CHUNK_SIZE,
    count_records,
    INITIAL_BACKOFF,
    make_opensearch_client,
    MAX_BACKOFF,
    MAX_CHUNK_BYTES,
    MAX_ERROR_SAMPLES,
    MAX_RETRIES,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
)
from dmpworks.utils import timed

log = logging.getLogger(__name__)


# Global OpenSearch client, one created for each process
open_search: Optional[OpenSearch] = None
success_counter: Optional[mp.Value] = None
failure_counter: Optional[mp.Value] = None
counter_lock: Optional[mp.Value] = None
chunk_sizes_queue: Optional[mp.Queue] = None


BatchToActions = Callable[[str, pa.RecordBatch], Iterator[dict]]


class ErrorSample(TypedDict):
    doc_id: str
    error: dict


class ErrorSummary(TypedDict):
    count: int
    samples: list[ErrorSample]


ErrorMap = dict[int, ErrorSummary]


def stream_parquet_batches(
    source: pathlib.Path,
    columns: Optional[list[str]] = None,
    batch_size: Optional[int] = None,
) -> Iterator[pa.RecordBatch]:
    # Yield batches from a parquet file
    with pq.ParquetFile(source) as parquet_file:
        for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
            yield batch


def init_process(
    config: OpenSearchClientConfig,
    success_value: mp.Value,
    failure_value: mp.Value,
    lock: mp.Lock,
    chunk_sizes: mp.Queue,
    create_open_search: bool,
    level: int,
):
    global open_search, success_counter, failure_counter, counter_lock, chunk_sizes_queue
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")
    logging.getLogger("opensearch").setLevel(logging.WARNING)
    if create_open_search:
        open_search = make_opensearch_client(config)
    success_counter = success_value
    failure_counter = failure_value
    counter_lock = lock
    chunk_sizes_queue = chunk_sizes


def measure_chunk_bytes(chunk):
    payload = "\n".join(json.dumps(doc, separators=(",", ":")) for doc in chunk) + "\n"
    return len(payload.encode("utf-8"))


def collect_completed_futures(
    futures: list,
    error_map: ErrorMap,
    max_error_samples: int = MAX_ERROR_SAMPLES,
):
    finished = []
    for fut in futures:
        if fut.done():
            try:
                new_errors = fut.result()
                # None if dry run or measure chunk size
                if new_errors is not None:
                    merge_error_maps(
                        error_map,
                        new_errors,
                        max_error_samples=max_error_samples,
                    )
            except Exception as e:
                log.exception(f"Error processing future: {e}")
            finished.append(fut)
    for fut in finished:
        futures.remove(fut)


def update_progress_bar(
    pbar: tqdm,
    success_count: int,
    failure_count: int,
    total: int,
    postfix_extra: Optional[dict] = None,
):
    new_total = success_count + failure_count
    delta = new_total - total
    pbar.update(delta)
    postfix = {"Success": f"{success_count:,}", "Fail": f"{failure_count:,}"}
    if postfix_extra:
        postfix.update(postfix_extra)
    pbar.set_postfix(postfix)
    return new_total


def collect_chunk_sizes(
    chunk_sizes: mp.Queue,
    min_chunk_size: float,
    max_chunk_size: float,
    sum_chunk_size: float,
    total_chunks: int,
    drain_queue: bool = False,
    timeout_seconds: int = 1,
):
    start_time = time.monotonic()
    while drain_queue or (time.monotonic() - start_time < timeout_seconds):
        try:
            chunk_size = chunk_sizes.get(block=True, timeout=1)
            min_chunk_size = min(min_chunk_size, chunk_size)
            max_chunk_size = max(max_chunk_size, chunk_size)
            sum_chunk_size += chunk_size
            total_chunks += 1
        except queue.Empty:
            # If drain_queue is True and we get an empty queue
            # then we can break
            if drain_queue:
                break

    return min_chunk_size, max_chunk_size, sum_chunk_size, total_chunks


def default_error() -> ErrorSummary:
    return {"count": 0, "samples": []}


def index_actions(
    actions: Iterator[dict],
    chunk_size: int,
    max_chunk_bytes: int,
    max_retries: int,
    initial_backoff: int,
    max_backoff: int,
):
    errors: ErrorMap = defaultdict(default_error)

    for ok, info in streaming_bulk(
        open_search,
        actions,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
        raise_on_error=False,
        raise_on_exception=False,
    ):
        if ok:
            with counter_lock:
                success_counter.value += 1
        else:
            with counter_lock:
                failure_counter.value += 1

            error = info_to_error_map(info)
            merge_error_maps(errors, error)

    return errors


def info_to_error_map(info: dict) -> ErrorMap:
    update = info.get("update", {})
    doc_id: str = update.get("_id")
    status: int = update.get("status")
    error: dict = update.get("error")
    return {
        status: {
            "count": 1,
            "samples": [
                {
                    "doc_id": doc_id,
                    "error": error,
                }
            ],
        }
    }


def merge_error_maps(
    merged_errors: ErrorMap,
    new_errors: ErrorMap,
    max_error_samples: int = MAX_ERROR_SAMPLES,
):
    for status, error_summary in new_errors.items():
        merged_summary = merged_errors[status]
        merged_summary["count"] += error_summary["count"]

        for doc_id in error_summary["samples"]:
            if len(merged_summary["samples"]) >= max_error_samples:
                break
            merged_summary["samples"].append(doc_id)


def measure_chunks(
    actions: Iterator[dict],
    chunk_size: int,
):
    chunk = []
    for action in actions:
        chunk.append(action)
        if len(chunk) >= chunk_size:
            size_bytes = measure_chunk_bytes(chunk)
            chunk_sizes_queue.put(size_bytes)
            chunk = []

        with counter_lock:
            success_counter.value += 1

    # Process final chunk if it's non-empty
    if chunk:
        size_bytes = measure_chunk_bytes(chunk)
        chunk_sizes_queue.put(size_bytes)


def dry_run_actions(
    actions: Iterator[dict],
):
    for _ in actions:
        with counter_lock:
            success_counter.value += 1


@lru_cache(maxsize=None)
def wait_first_run(proc_idx: int):
    # Runs once per process in a ProcessPoolExecutor. The return value is
    # cached so the function body is executed only the first time it is called
    # in each process.
    sleep_secs = (proc_idx - 1) * 60
    log.debug(f"Staggered start, process {proc_idx} (PID {os.getpid()}) sleeping {sleep_secs}s")
    time.sleep(sleep_secs)


def get_process_index() -> int:
    proc = current_process()
    if proc._identity:
        idx = proc._identity[0]
        log.debug(f"proc._identity: {idx}")
        return idx

    # Fallback
    idx = int(proc.name.split('-')[-1])
    log.debug(f"proc.name: {idx}")
    return idx


def index_file(
    *,
    file_path: pathlib.Path,
    index_name: str,
    batch_to_actions_func: BatchToActions,
    columns: Optional[List[str]],
    chunk_size: int = CHUNK_SIZE,
    max_chunk_bytes: int = MAX_CHUNK_BYTES,
    max_retries: int = MAX_RETRIES,
    initial_backoff: int = INITIAL_BACKOFF,
    max_backoff: int = MAX_BACKOFF,
    dry_run: bool = False,
    measure_chunk_size: bool = False,
    staggered_start: bool = False,
):
    if staggered_start:
        log.debug("Staggered start")
        idx = get_process_index()
        wait_first_run(idx)

    batches = stream_parquet_batches(source=file_path, columns=columns, batch_size=chunk_size)
    actions = (action for batch in batches for action in batch_to_actions_func(index_name, batch))

    if dry_run:
        log.debug(f"Dry run on file: {file_path}")
        dry_run_actions(
            actions,
        )
    elif measure_chunk_size:
        log.debug(f"Measuring chunks from file: {file_path}")
        measure_chunks(
            actions,
            chunk_size,
        )
    else:
        log.debug(f"Indexing file: {file_path}")
        error_map = index_actions(
            actions,
            chunk_size,
            max_chunk_bytes,
            max_retries,
            initial_backoff,
            max_backoff,
        )
        return error_map


def bytes_to_mb(n):
    return n / 1024 / 1024


@timed
def sync_docs(
    *,
    index_name: str,
    in_dir: pathlib.Path,
    batch_to_actions_func: BatchToActions,
    include_columns: list[str],
    client_config: OpenSearchClientConfig,
    sync_config: OpenSearchSyncConfig,
    log_level: int = logging.INFO,
):
    parquet_files = list(in_dir.rglob("*.parquet"))
    log.info("Counting records...")
    total_records = count_records(in_dir)
    log.info(f"Total records {total_records:,}")

    total = 0
    error_map: ErrorMap = defaultdict(default_error)
    ctx = mp.get_context("spawn")
    success = ctx.Value("i", 0)
    failure = ctx.Value("i", 0)
    chunk_sizes: mp.Queue = ctx.Queue()
    lock = ctx.Lock()

    # Chunk size stats
    min_chunk_size = math.inf
    max_chunk_size = -math.inf
    sum_chunk_size = 0
    total_chunks = 0

    start = pendulum.now()
    with tqdm(
        total=total_records,
        desc="Sync Docs with OpenSearch",
        unit="doc",
    ) as pbar:
        with ProcessPoolExecutor(
            mp_context=ctx,
            max_workers=sync_config.max_processes,
            initializer=init_process,
            initargs=(
                client_config,
                success,
                failure,
                lock,
                chunk_sizes,
                not (sync_config.dry_run or sync_config.measure_chunk_size),
                log_level,
            ),
        ) as executor:
            log.debug("Queuing futures...")
            futures = [
                executor.submit(
                    index_file,
                    file_path=file_path,
                    index_name=index_name,
                    batch_to_actions_func=batch_to_actions_func,
                    columns=include_columns,
                    chunk_size=sync_config.chunk_size,
                    max_chunk_bytes=sync_config.max_chunk_bytes,
                    max_retries=sync_config.max_retries,
                    initial_backoff=sync_config.initial_backoff,
                    max_backoff=sync_config.max_backoff,
                    dry_run=sync_config.dry_run,
                    measure_chunk_size=sync_config.measure_chunk_size,
                    staggered_start=sync_config.staggered_start,
                )
                for file_path in parquet_files
            ]
            log.debug("Finished queuing futures.")

            while futures:
                # Get counts
                log.debug("Get counts")
                with lock:
                    success_count = success.value
                    failure_count = failure.value

                # Collect any futures have finished and are done and save failed_ids
                log.debug("Collect futures")
                collect_completed_futures(
                    futures,
                    error_map,
                    max_error_samples=sync_config.max_error_samples,
                )

                # Collect chunk sizes
                if sync_config.measure_chunk_size:
                    log.debug("Collect chunk sizes")
                    min_chunk_size, max_chunk_size, sum_chunk_size, total_chunks = collect_chunk_sizes(
                        chunk_sizes,
                        min_chunk_size,
                        max_chunk_size,
                        sum_chunk_size,
                        total_chunks,
                    )

                # Update progress bar
                log.debug("Update progress bar")
                postfix_extra = None
                if sync_config.measure_chunk_size and math.isfinite(sum_chunk_size) and total_chunks > 0:
                    log.debug(f"Calculating chunk size")
                    mean_chunk_size = bytes_to_mb(sum_chunk_size / total_chunks)
                    postfix_extra = {"Avg Chunk Size": f"{mean_chunk_size:.2f} MB"}
                total = update_progress_bar(pbar, success_count, failure_count, total, postfix_extra)

                # Sleep
                if not sync_config.measure_chunk_size:
                    log.debug("Sleep")
                    time.sleep(1)

            if sync_config.measure_chunk_size:
                log.debug("Collect any remaining chunk sizes")
                min_chunk_size, max_chunk_size, sum_chunk_size, total_chunks = collect_chunk_sizes(
                    chunk_sizes,
                    min_chunk_size,
                    max_chunk_size,
                    sum_chunk_size,
                    total_chunks,
                    drain_queue=True,
                )

            log.debug("Exiting ProcessPoolExecutor...")
    log.debug("Exited ProcessPoolExecutor")

    end = pendulum.now()
    duration_seconds = float((end - start).seconds)  # Prevent divide by zero
    docs_per_sec = total / max(duration_seconds, 1e-6)

    log.info(f"Bulk indexing complete.")
    log.info(f"Total docs: {total:,}")
    log.info(f"Num success: {success_count:,}")
    log.info(f"Num failures: {failure_count:,}")
    log.info(f"Docs/s: {round(docs_per_sec):,}")

    if sync_config.measure_chunk_size:
        avg_bytes = sum_chunk_size / total_chunks
        log.info(f"Analyzed {total_chunks} chunks from ({total:,} docs total)")
        log.info(f"Min chunk size: {bytes_to_mb(min_chunk_size):.2f} MB")
        log.info(f"Max chunk size: {bytes_to_mb(max_chunk_size):.2f} MB")
        log.info(f"Avg chunk size: {bytes_to_mb(avg_bytes):.2f} MB")

    # Log error info
    for status, error_summary in error_map.items():
        log.error(
            f"Summary of errors with status {status}: %s",
            json.dumps(error_summary),
        )
