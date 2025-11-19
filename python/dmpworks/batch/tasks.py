import logging
import pathlib
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generator

from dmpworks.batch.utils import clean_s3_prefix, download_files_from_s3, local_path, s3_uri, upload_files_to_s3

log = logging.getLogger(__name__)


@dataclass
class DownloadTaskContext:
    download_dir: pathlib.Path
    target_uri: str


@contextmanager
def download_source_task(bucket_name: str, dataset: str, run_id: str) -> Generator[DownloadTaskContext, Any, None]:
    target_uri = s3_uri(bucket_name, dataset, run_id, "download/")
    download_dir = local_path(dataset, run_id, "download")

    clean_s3_prefix(target_uri)

    log.info(f"Downloading {dataset}")
    ctx = DownloadTaskContext(
        download_dir=download_dir,
        target_uri=target_uri,
    )
    yield ctx

    upload_files_to_s3(download_dir, target_uri)

    # Cleanup files as we can't guarantee that we will end up on the same worker
    # again, and we don't want to take disk space that other tasks might use
    shutil.rmtree(download_dir, ignore_errors=True)


@dataclass
class DatasetSubsetTaskContext:
    download_dir: pathlib.Path
    subset_dir: pathlib.Path
    target_uri: str


@contextmanager
def dataset_subset_task(bucket_name: str, dataset: str, run_id: str) -> Generator[DatasetSubsetTaskContext, Any, None]:
    download_dir = local_path(dataset, run_id, "download")
    subset_dir = local_path(dataset, run_id, "subset")
    target_uri = s3_uri(bucket_name, dataset, run_id, "subset/")

    clean_s3_prefix(target_uri)

    download_uri = s3_uri(bucket_name, dataset, run_id, "download/*")
    download_files_from_s3(download_uri, download_dir)
    subset_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Transforming {dataset}")
    ctx = DatasetSubsetTaskContext(
        download_dir=download_dir,
        subset_dir=subset_dir,
        target_uri=target_uri,
    )
    yield ctx

    upload_files_to_s3(subset_dir, target_uri, "*")

    # Cleanup files as we can't guarantee that we will end up on the same worker
    # again, and we don't want to take disk space that other tasks might use
    shutil.rmtree(download_dir, ignore_errors=True)
    shutil.rmtree(subset_dir, ignore_errors=True)


@dataclass
class TransformTaskContext:
    download_dir: pathlib.Path
    transform_dir: pathlib.Path
    target_uri: str


@contextmanager
def transform_parquets_task(
    bucket_name: str, dataset: str, run_id: str, use_subset: bool = False
) -> Generator[TransformTaskContext, Any, None]:
    download_dir = local_path(dataset, run_id, "subset" if use_subset else "download")
    transform_dir = local_path(dataset, run_id, "transform")
    target_uri = s3_uri(bucket_name, dataset, run_id, "transform/")

    clean_s3_prefix(target_uri)

    download_uri = s3_uri(bucket_name, dataset, run_id, "subset/*" if use_subset else "download/*")
    download_files_from_s3(download_uri, download_dir)
    transform_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Transforming {dataset}")
    ctx = TransformTaskContext(
        download_dir=download_dir,
        transform_dir=transform_dir,
        target_uri=target_uri,
    )
    yield ctx

    upload_files_to_s3(transform_dir / "parquets", f"{target_uri}parquets/", "*.parquet")

    # Cleanup files as we can't guarantee that we will end up on the same worker
    # again, and we don't want to take disk space that other tasks might use
    shutil.rmtree(download_dir, ignore_errors=True)
    shutil.rmtree(transform_dir, ignore_errors=True)
