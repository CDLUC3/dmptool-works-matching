from collections import defaultdict
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from functools import cache
import json
import logging
import math
import multiprocessing as mp
from multiprocessing import current_process
from multiprocessing.queues import Queue
from multiprocessing.sharedctypes import Synchronized
import os
import pathlib
import queue
import time
from typing import TypedDict

from opensearchpy import OpenSearch
from opensearchpy.helpers import streaming_bulk
import pendulum
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tqdm import tqdm

from dmpworks.cli_utils import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.opensearch.utils import count_records, make_opensearch_client
from dmpworks.utils import timed

CHUNK_SIZE = 1000
INITIAL_BACKOFF = 2
MAX_BACKOFF = 600
MAX_CHUNK_BYTES = 100 * 1024 * 1024
MAX_ERROR_SAMPLES = 50
MAX_RETRIES = 10


log = logging.getLogger(__name__)


# Global OpenSearch client, one created for each process
OPEN_SEARCH: OpenSearch | None = None
SUCCESS_COUNTER: Synchronized | None = None
FAILURE_COUNTER: Synchronized | None = None
COUNTER_LOCK: Synchronized | None = None
CHUNK_SIZES_QUEUE: Queue | None = None


BatchToActions = Callable[[str, pa.RecordBatch], Iterator[dict]]


class ErrorSample(TypedDict):
    """A sample error from OpenSearch bulk indexing.

    Attributes:
        doc_id: The document ID that failed.
        error: The error details.
    """

    doc_id: str
    error: dict


class ErrorSummary(TypedDict):
    """Summary of errors for a specific status code.

    Attributes:
        count: The number of errors with this status.
        samples: A list of sample errors.
    """

    count: int
    samples: list[ErrorSample]


ErrorMap = dict[int, ErrorSummary]


def stream_parquet_batches(
    source: pathlib.Path,
    columns: list[str] | None = None,
    batch_size: int | None = None,
) -> Iterator[pa.RecordBatch]:
    """Yield batches from a parquet file.

    Args:
        source: Path to the parquet file.
        columns: List of columns to read.
        batch_size: Number of rows per batch.

    Yields:
        pa.RecordBatch: A batch of records.
    """
    # Yield batches from a parquet file
    with pq.ParquetFile(source) as parquet_file:
        yield from parquet_file.iter_batches(batch_size=batch_size, columns=columns)


def init_process(
    config: OpenSearchClientConfig,
    success_value: mp.Value,
    failure_value: mp.Value,
    lock: mp.Lock,
    chunk_sizes: mp.Queue,
    create_open_search: bool,
    level: int,
):
    """Initialize a worker process.

    Args:
        config: OpenSearch client configuration.
        success_value: Shared counter for successful operations.
        failure_value: Shared counter for failed operations.
        lock: Shared lock for counters.
        chunk_sizes: Shared queue for chunk sizes.
        create_open_search: Whether to create an OpenSearch client.
        level: Logging level.
    """
    global OPEN_SEARCH, SUCCESS_COUNTER, FAILURE_COUNTER, COUNTER_LOCK, CHUNK_SIZES_QUEUE
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")
    logging.getLogger("opensearch").setLevel(logging.WARNING)
    if create_open_search:
        OPEN_SEARCH = make_opensearch_client(config)
    SUCCESS_COUNTER = success_value
    FAILURE_COUNTER = failure_value
    COUNTER_LOCK = lock
    CHUNK_SIZES_QUEUE = chunk_sizes


def measure_chunk_bytes(chunk):
    """Measure the size of a chunk in bytes.

    Args:
        chunk: A list of documents.

    Returns:
        int: The size of the chunk in bytes.
    """
    payload = "\n".join(json.dumps(doc, separators=(",", ":")) for doc in chunk) + "\n"
    return len(payload.encode("utf-8"))


def collect_completed_futures(
    futures: list,
    error_map: ErrorMap,
    max_error_samples: int = MAX_ERROR_SAMPLES,
):
    """Collect results from completed futures and update the error map.

    Args:
        futures: List of futures to check.
        error_map: Dictionary to store error summaries.
        max_error_samples: Maximum number of error samples to keep.
    """
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
            except Exception:
                log.exception("Error processing future")
            finished.append(fut)
    for fut in finished:
        futures.remove(fut)


def update_progress_bar(
    pbar: tqdm,
    success_count: int,
    failure_count: int,
    total: int,
    postfix_extra: dict | None = None,
):
    """Update the progress bar.

    Args:
        pbar: The tqdm progress bar.
        success_count: Number of successful operations.
        failure_count: Number of failed operations.
        total: Total number of operations processed so far.
        postfix_extra: Extra information to display in the progress bar.

    Returns:
        int: The new total count.
    """
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
    """Collect chunk sizes from the queue.

    Args:
        chunk_sizes: The queue containing chunk sizes.
        min_chunk_size: Current minimum chunk size.
        max_chunk_size: Current maximum chunk size.
        sum_chunk_size: Current sum of chunk sizes.
        total_chunks: Current total number of chunks.
        drain_queue: Whether to drain the queue completely.
        timeout_seconds: Timeout for getting items from the queue.

    Returns:
        tuple: Updated (min_chunk_size, max_chunk_size, sum_chunk_size, total_chunks).
    """
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
    """Create a default error summary.

    Returns:
        ErrorSummary: A default error summary dictionary.
    """
    return {"count": 0, "samples": []}


def stream_actions(
    client: OpenSearch,
    actions: Iterator[dict],
    chunk_size: int,
    max_chunk_bytes: int,
    max_retries: int,
    initial_backoff: int,
    max_backoff: int,
):
    """Stream actions to OpenSearch using the bulk API.

    Args:
        client: The OpenSearch client.
        actions: Iterator of actions to perform.
        chunk_size: Number of actions per chunk.
        max_chunk_bytes: Maximum size of a chunk in bytes.
        max_retries: Maximum number of retries.
        initial_backoff: Initial backoff time in seconds.
        max_backoff: Maximum backoff time in seconds.

    Returns:
        ErrorMap: A map of errors encountered.
    """
    errors: ErrorMap = defaultdict(default_error)

    for ok, info in streaming_bulk(
        client,
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
            with COUNTER_LOCK:
                SUCCESS_COUNTER.value += 1
        else:
            with COUNTER_LOCK:
                FAILURE_COUNTER.value += 1

            error = info_to_error_map(info)
            merge_error_maps(errors, error)

    return errors


def info_to_error_map(info: dict) -> ErrorMap:
    """Convert bulk API info to an error map.

    Args:
        info: The info dictionary returned by the bulk API.

    Returns:
        ErrorMap: An error map containing the error details.
    """
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
    """Merge new errors into an existing error map.

    Args:
        merged_errors: The existing error map.
        new_errors: The new errors to merge.
        max_error_samples: Maximum number of error samples to keep.
    """
    for status, error_summary in new_errors.items():
        merged_summary = merged_errors[status]
        merged_summary["count"] += error_summary["count"]

        for sample in error_summary["samples"]:
            if len(merged_summary["samples"]) >= max_error_samples:
                break
            merged_summary["samples"].append(sample)


def measure_chunks(
    actions: Iterator[dict],
    chunk_size: int,
):
    """Measure chunk sizes without sending them to OpenSearch.

    Args:
        actions: Iterator of actions.
        chunk_size: Number of actions per chunk.
    """
    chunk = []
    for action in actions:
        chunk.append(action)
        if len(chunk) >= chunk_size:
            size_bytes = measure_chunk_bytes(chunk)
            CHUNK_SIZES_QUEUE.put(size_bytes)
            chunk = []

        with COUNTER_LOCK:
            SUCCESS_COUNTER.value += 1

    # Process final chunk if it's non-empty
    if chunk:
        size_bytes = measure_chunk_bytes(chunk)
        CHUNK_SIZES_QUEUE.put(size_bytes)


def dry_run_actions(
    actions: Iterator[dict],
):
    """Consume actions without performing any operations.

    Args:
        actions: Iterator of actions.
    """
    for _ in actions:
        with COUNTER_LOCK:
            SUCCESS_COUNTER.value += 1


@cache
def wait_first_run(proc_idx: int):
    """Wait for a staggered start based on the process index.

    Args:
        proc_idx: The process index.
    """
    # Runs once per process in a ProcessPoolExecutor. The return value is
    # cached so the function body is executed only the first time it is called
    # in each process.
    sleep_secs = (proc_idx - 1) * 60
    log.debug(f"Staggered start, process {proc_idx} (PID {os.getpid()}) sleeping {sleep_secs}s")
    time.sleep(sleep_secs)


def get_process_index() -> int:
    """Get the index of the current process.

    Returns:
        int: The process index.
    """
    proc = current_process()
    if proc._identity:
        idx = proc._identity[0]
        log.debug(f"proc._identity: {idx}")
        return idx

    # Fallback
    idx = int(proc.name.split("-")[-1])
    log.debug(f"proc.name: {idx}")
    return idx


def index_file(
    *,
    file_path: pathlib.Path,
    index_name: str,
    batch_to_actions_func: BatchToActions,
    columns: list[str] | None,
    chunk_size: int = CHUNK_SIZE,
    max_chunk_bytes: int = MAX_CHUNK_BYTES,
    max_retries: int = MAX_RETRIES,
    initial_backoff: int = INITIAL_BACKOFF,
    max_backoff: int = MAX_BACKOFF,
    dry_run: bool = False,
    measure_chunk_size: bool = False,
    staggered_start: bool = False,
):
    """Index a file into OpenSearch.

    Args:
        file_path: Path to the file to index.
        index_name: Name of the OpenSearch index.
        batch_to_actions_func: Function to convert a batch to actions.
        columns: List of columns to read from the file.
        chunk_size: Number of actions per chunk.
        max_chunk_bytes: Maximum size of a chunk in bytes.
        max_retries: Maximum number of retries.
        initial_backoff: Initial backoff time in seconds.
        max_backoff: Maximum backoff time in seconds.
        dry_run: Whether to perform a dry run.
        measure_chunk_size: Whether to measure chunk sizes.
        staggered_start: Whether to stagger the start of indexing.

    Returns:
        ErrorMap: A map of errors encountered (if not dry run or measure chunk size).
    """
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
        return stream_actions(
            OPEN_SEARCH,
            actions,
            chunk_size,
            max_chunk_bytes,
            max_retries,
            initial_backoff,
            max_backoff,
        )

    return None


def bytes_to_mb(n):
    """Convert bytes to megabytes.

    Args:
        n: Number of bytes.

    Returns:
        float: Number of megabytes.
    """
    return n / 1024 / 1024


@timed
def delete_docs(
    *,
    index_name: str,
    doi_state_dir: pathlib.Path,
    run_id: str,
    client_config: OpenSearchClientConfig,
    chunk_size: int = CHUNK_SIZE,
    max_chunk_bytes: int = MAX_CHUNK_BYTES,
    max_retries: int = MAX_RETRIES,
    initial_backoff: int = INITIAL_BACKOFF,
    max_backoff: int = MAX_BACKOFF,
):
    """Delete documents from OpenSearch based on DOI state.

    Args:
        index_name: Name of the OpenSearch index.
        doi_state_dir: Directory containing DOI state parquet files.
        run_id: Run ID (date string) to filter records.
        client_config: OpenSearch client configuration.
        chunk_size: Number of actions per chunk.
        max_chunk_bytes: Maximum size of a chunk in bytes.
        max_retries: Maximum number of retries.
        initial_backoff: Initial backoff time in seconds.
        max_backoff: Maximum backoff time in seconds.
    """
    try:
        updated_date = pendulum.parse(run_id, strict=True)
    except Exception as e:
        raise ValueError(f"Invalid run_id '{run_id}'. Expected ISO date string (YYYY-MM-DD).") from e

    dataset = ds.dataset(doi_state_dir, format="parquet")
    filter_expr = (pc.field("state") == "DELETE") & (pc.field("updated_date") == updated_date)
    total_records = dataset.count_rows(filter=filter_expr)
    log.info(f"Total records to delete: {total_records}")

    error_map: ErrorMap = defaultdict(default_error)
    opensearch_client = make_opensearch_client(client_config)
    total = 0
    success_count = 0
    failure_count = 0

    start = pendulum.now()
    with tqdm(
        total=total_records,
        desc="Delete Docs in OpenSearch",
        unit="doc",
    ) as pbar:
        scanner = dataset.scanner(
            columns=["doi", "state", "updated_date"],
            filter=filter_expr,
            batch_size=chunk_size,
        )
        for batch in scanner.to_batches():
            rows = batch.to_pylist()
            actions = yield_delete_actions(
                index_name=index_name,
                batch=rows,
            )

            for ok, info in streaming_bulk(
                opensearch_client,
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
                    success_count += 1
                else:
                    failure_count += 1

                error = info_to_error_map(info)
                merge_error_maps(error_map, error)

                total = update_progress_bar(
                    pbar,
                    success_count,
                    failure_count,
                    total,
                )

    end = pendulum.now()
    duration_seconds = float((end - start).seconds)  # Prevent divide by zero
    docs_per_sec = total / max(duration_seconds, 1e-6)

    log.info("Bulk delete complete.")
    log.info(f"Total docs: {total:,}")
    log.info(f"Num success: {success_count:,}")
    log.info(f"Num failures: {failure_count:,}")
    log.info(f"Docs/s: {round(docs_per_sec):,}")


def yield_delete_actions(
    *,
    index_name: str,
    batch: list[dict],
):
    """Yield delete actions for a batch of records.

    Args:
        index_name: Name of the OpenSearch index.
        batch: List of records containing DOIs.

    Yields:
        dict: A delete action dictionary.
    """
    for row in batch:
        doi = row.get("doi")
        if doi is not None:
            yield {
                "_op_type": "delete",
                "_index": index_name,
                "_id": doi,
            }


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
    """Sync documents from parquet files to OpenSearch.

    Args:
        index_name: Name of the OpenSearch index.
        in_dir: Directory containing parquet files.
        batch_to_actions_func: Function to convert batches to actions.
        include_columns: List of columns to read from parquet files.
        client_config: OpenSearch client configuration.
        sync_config: Sync configuration.
        log_level: Logging level.
    """
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
    with (
        tqdm(
            total=total_records,
            desc="Sync Docs with OpenSearch",
            unit="doc",
        ) as pbar,
        ProcessPoolExecutor(
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
        ) as executor,
    ):
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
                log.debug("Calculating chunk size")
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

    log.info("Bulk indexing complete.")
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
