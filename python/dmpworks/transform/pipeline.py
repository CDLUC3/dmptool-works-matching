import logging
import multiprocessing as mp
import os
import pathlib
import queue
import shutil
import threading
from abc import ABC, abstractmethod
from concurrent.futures import as_completed, ProcessPoolExecutor
from pathlib import Path
from typing import Callable, Optional

from tqdm import tqdm

import polars as pl
from dmpworks.transform.utils_file import extract_gzip, read_jsonls, write_parquet
from dmpworks.utils import timed, to_batches
from polars._typing import SchemaDefinition

TransformFunc = Callable[[pl.LazyFrame], list[tuple[str, pl.LazyFrame]]]

log = logging.getLogger(__name__)


class FileExtractor:
    def __init__(self, extract_func: Callable[[Path, Path], None], in_dir: Path, out_dir: Path):
        self.extract_func = extract_func
        self.in_dir = in_dir
        self.out_dir = out_dir

    def __call__(self, in_file: Path) -> Path:
        out_file = self.out_dir / "extract" / in_file.relative_to(self.in_dir).with_suffix("")
        out_file.parent.mkdir(parents=True, exist_ok=True)
        self.extract_func(in_file, out_file)
        return out_file


class BatchTransformer:
    def __init__(
        self,
        read_func: Callable[[list[Path], SchemaDefinition, bool], pl.LazyFrame],
        transform_func: TransformFunc,
        schema: SchemaDefinition,
        low_memory: bool,
        out_dir: Path,
    ):
        self.read_func = read_func
        self.transform_func = transform_func
        self.schema = schema
        self.low_memory = low_memory
        self.out_dir = out_dir

    def __call__(self, idx: int, batch: list[Path]):
        # batch_non_empty = [file for file in batch if file.stat().st_size > 0]
        lz = self.read_func(batch, self.schema, self.low_memory)
        results = self.transform_func(lz)
        for table_name, lz_frame in results:
            parquet_file = self.out_dir / "parquets" / f"{table_name}_{idx:05d}.parquet"
            parquet_file.parent.mkdir(parents=True, exist_ok=True)
            write_parquet(lz_frame, parquet_file)


class BaseWorker(threading.Thread, ABC):
    def __init__(
        self,
        *,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        name: Optional[str] = None,
        log_level: int = logging.INFO,
    ):
        super().__init__(name=name)  # daemon=False,
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.log_level = log_level

    def run(self):
        log.debug("running worker")

        while True:
            log.debug(f"Waiting for task")
            task = self.input_queue.get()
            if task is None:
                log.debug(f"Received exit signal")
                self.input_queue.task_done()
                break

            idx, batch = task
            log.debug(f"Picked up task batch={idx}")
            try:
                self.process_task(idx, batch)
            except Exception:
                log.exception(f"Error processing batch={idx}")
            finally:
                self.input_queue.task_done()
                log.debug(f"Task done batch={idx}")

        log.debug("worker shutdown")

    @abstractmethod
    def process_task(self, idx: int, batch: list[Path]):
        """Process the given task. Must be implemented by subclasses."""
        pass


def log_stage(logger: logging.Logger, stage: str, status: str, batch: int):
    logger.debug(f"[{stage:<10}] {status:<5} batch={batch}")


def init_process_logs(level: int):
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")


class ExtractWorker(BaseWorker):
    def __init__(
        self,
        *,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        file_extractor: Optional[FileExtractor] = None,
        max_processes: int = os.cpu_count(),
        name: str = None,
        log_level: int = logging.INFO,
    ):
        super().__init__(input_queue=input_queue, output_queue=output_queue, name=name, log_level=log_level)
        self.file_extractor = file_extractor
        self.max_processes = max_processes
        self.executor: Optional[ProcessPoolExecutor] = None

    def run(self):
        log.debug("Extract outer run start")
        executor = ProcessPoolExecutor(
            mp_context=mp.get_context("spawn"),
            max_workers=self.max_processes,
            initializer=init_process_logs,
            initargs=(self.log_level,),
        )
        self.executor = executor
        super().run()
        executor.shutdown(wait=True, cancel_futures=True)
        log.debug("Extract outer run end")

    def process_task(self, idx: int, batch: list[Path]):
        # Extract files with ProcessPoolExecutor
        log_stage(log, "EXTRACT", "start", idx)

        futures = []
        if self.file_extractor is not None:
            for file in batch:
                futures.append(self.executor.submit(self.file_extractor, file))

        # Wait for batch to finish
        extracted_files = []
        for future in as_completed(futures):
            file_path = future.result()
            extracted_files.append(file_path)

        # Queue output
        self.output_queue.put((idx, extracted_files))
        log_stage(log, "EXTRACT", "end", idx)


class TransformWorker(BaseWorker):
    def __init__(
        self,
        *,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        batch_transformer: BatchTransformer,
        name: str = None,
        log_level: int = logging.INFO,
    ):
        super().__init__(input_queue=input_queue, output_queue=output_queue, name=name, log_level=log_level)
        self.batch_transformer = batch_transformer

    def process_task(self, idx: int, batch: list[Path]):
        log_stage(log, "TRANSFORM", "start", idx)
        self.batch_transformer(idx, batch)

        # Queue output
        self.output_queue.put((idx, batch))
        log_stage(log, "TRANSFORM", "end", idx)


class CleanupWorker(BaseWorker):
    def __init__(
        self,
        *,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        name: str = None,
        log_level: int = logging.INFO,
    ):
        super().__init__(input_queue=input_queue, output_queue=output_queue, name=name, log_level=log_level)

    def process_task(self, idx: int, batch: list[Path]):
        log_stage(log, "CLEANUP", "start", idx)
        [file.unlink(missing_ok=True) for file in batch]
        self.output_queue.put(idx)
        log_stage(log, "CLEANUP", "end", idx)


class Pipeline:
    def __init__(
        self,
        *,
        file_extractor: Optional[FileExtractor],
        batch_transformer: BatchTransformer,
        extract_workers: int = 1,
        transform_workers: int = 1,
        cleanup_workers: int = 1,
        extract_queue_size: int = 0,
        transform_queue_size: int = 0,
        cleanup_queue_size: int = 0,
        max_file_processes: int = os.cpu_count(),
        log_level: logging.INFO,
    ):
        self.extract_queue = queue.Queue(maxsize=extract_queue_size)
        self.transform_queue = queue.Queue(maxsize=transform_queue_size)
        self.cleanup_queue = queue.Queue(maxsize=cleanup_queue_size)
        self.completed_queue = queue.Queue()
        self.extract_workers = [
            ExtractWorker(
                input_queue=self.extract_queue,
                output_queue=self.transform_queue,
                file_extractor=file_extractor,
                max_processes=max_file_processes,
                name=f"Extract-Thread-{i}",
                log_level=log_level,
            )
            for i in range(extract_workers)
        ]
        self.transform_workers = [
            TransformWorker(
                input_queue=self.transform_queue,
                output_queue=self.cleanup_queue,
                batch_transformer=batch_transformer,
                name=f"Transform-Thread-{i}",
                log_level=log_level,
            )
            for i in range(transform_workers)
        ]
        self.cleanup_workers = [
            CleanupWorker(
                input_queue=self.cleanup_queue,
                output_queue=self.completed_queue,
                name=f"Cleanup-Thread-{i}",
                log_level=log_level,
            )
            for i in range(cleanup_workers)
        ]

    def start(self, batches: list[list[Path]]):
        num_batches = len(batches)
        workers = self.extract_workers + self.transform_workers + self.cleanup_workers

        try:
            # Start workers
            for worker in workers:
                worker.start()

            with tqdm(
                total=num_batches,
                desc="Transformation Pipeline",
                unit="batch",
            ) as pbar:
                # Fill extract queue
                for idx, batch in enumerate(batches):
                    log.debug(f"Queuing batch: {idx}")
                    self.extract_queue.put((idx, batch))

                # Wait for tasks to complete
                num_completed = 0
                while num_completed < len(batches):
                    try:
                        idx = self.completed_queue.get(timeout=1)
                        log.debug(f"Task completed: {idx}")
                        if idx is None:
                            break

                        num_completed += 1
                        pbar.update(1)
                        self.completed_queue.task_done()
                    except queue.Empty:
                        log.debug(f"Completed queue empty")
                        continue

        except KeyboardInterrupt:
            log.info("Interrupted by user")
        finally:
            # Signal shutdown
            # Each worker will eventually get a None
            for q, ws in [
                (self.extract_queue, self.extract_workers),
                (self.transform_queue, self.transform_workers),
                (self.cleanup_queue, self.cleanup_workers),
            ]:
                for _ in ws:
                    while True:
                        try:
                            q.put(None, timeout=1)
                            break
                        except queue.Full:
                            log.warning("Queue full, retrying shutdown...")

                    q.put(None)

            # Join threads
            log.debug("Joining threads")
            for worker in workers:
                log.debug(f"Joining worker: {worker}")
                worker.join()
            log.debug("Workers joined")


@timed
def process_files_parallel(
    *,
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    schema: SchemaDefinition,
    transform_func: TransformFunc,
    file_glob: str = "**/*.gz",
    extract_func: Callable[[Path, Path], None] = extract_gzip,
    read_func: Callable[[list[Path], SchemaDefinition, bool], pl.LazyFrame] = read_jsonls,
    batch_size: int = os.cpu_count(),
    extract_workers: int = 1,
    transform_workers: int = 1,
    cleanup_workers: int = 1,
    extract_queue_size: int = 0,
    transform_queue_size: int = 0,
    cleanup_queue_size: int = 0,
    max_file_processes: int = os.cpu_count(),
    n_batches: Optional[int] = None,
    low_memory: bool = False,
    log_level: int = logging.INFO,
):
    log.info(f"in_dir: {in_dir}")
    log.info(f"out_dir: {out_dir}")
    log.info(f"schema: {schema}")
    log.info(f"transform_func: {transform_func.__name__}")
    log.info(f"batch_size: {batch_size}")
    log.info(f"extract_workers: {extract_workers}")
    log.info(f"transform_workers: {transform_workers}")
    log.info(f"cleanup_workers: {cleanup_workers}")
    log.info(f"extract_queue_size: {extract_queue_size}")
    log.info(f"transform_queue_size: {transform_queue_size}")
    log.info(f"cleanup_queue_size: {cleanup_queue_size}")
    log.info(f"max_file_processes: {max_file_processes}")
    log.info(f"n_batches: {n_batches}")
    log.info(f"low_memory: {low_memory}")
    log.info(f"log_level: {logging.getLevelName(log_level)}")

    # Cleanup existing output directory
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build file extract and read functions
    file_extractor = None if extract_func is None else FileExtractor(extract_func, in_dir, out_dir)
    batch_transformer = BatchTransformer(read_func, transform_func, schema, low_memory, out_dir)

    # Process batches in parallel
    files = list(Path(in_dir).glob(file_glob))
    batches = list(to_batches(files, batch_size))
    if n_batches is not None:
        batches = batches[:n_batches]
    pipeline = Pipeline(
        file_extractor=file_extractor,
        batch_transformer=batch_transformer,
        extract_workers=extract_workers,
        transform_workers=transform_workers,
        cleanup_workers=cleanup_workers,
        extract_queue_size=extract_queue_size,
        transform_queue_size=transform_queue_size,
        cleanup_queue_size=cleanup_queue_size,
        max_file_processes=max_file_processes,
        log_level=log_level,
    )
    pipeline.start(batches)
