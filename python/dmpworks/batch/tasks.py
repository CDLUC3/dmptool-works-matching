from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
import logging
import pathlib
import shutil
from typing import Any

from dmpworks.batch.utils import (
    clean_s3_prefix,
    download_file_from_s3,
    download_files_from_s3,
    local_path,
    s3_uri,
    upload_files_to_s3,
)
from dmpworks.cli_utils import DatasetSubsetAWS
from dmpworks.dataset_subset import load_dois, load_institutions
from dmpworks.model.common import Institution

log = logging.getLogger(__name__)


@dataclass
class DownloadTaskContext:
    """Context for a download task.

    Attributes:
        download_dir: The local directory where files are downloaded.
        target_uri: The S3 URI where files will be uploaded.
    """

    download_dir: pathlib.Path
    target_uri: str


@contextmanager
def download_source_task(bucket_name: str, dataset: str, run_id: str) -> Generator[DownloadTaskContext, Any, None]:
    """Context manager for downloading source data.

    Prepares the download directory and target S3 URI.
    Cleans up the download directory after the task is complete.

    Args:
        bucket_name: The name of the S3 bucket.
        dataset: The name of the dataset.
        run_id: The unique identifier for the run.

    Yields:
        A DownloadTaskContext object.
    """
    target_uri = s3_uri(bucket_name, f"{dataset}-download", run_id) + "/"
    download_dir = local_path(f"{dataset}-download", run_id)

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
class DatasetSubsetAWSTaskContext:
    """Context for a dataset subset task.

    Attributes:
        download_dir: The local directory where source files are downloaded.
        subset_dir: The local directory where the subset will be created.
        target_uri: The S3 URI where the subset will be uploaded.
        institutions: A list of institutions to filter by.
        dois: A list of DOIs to filter by.
    """

    download_dir: pathlib.Path
    subset_dir: pathlib.Path
    target_uri: str
    institutions: list[Institution]
    dois: list[str]


@contextmanager
def dataset_subset_task(
    *,
    bucket_name: str,
    dataset: str,
    run_id: str,
    prev_run_id: str | None = None,
    dataset_subset: DatasetSubsetAWS,
) -> Generator[DatasetSubsetAWSTaskContext, Any, None]:
    """Context manager for creating a dataset subset.

    Downloads institutions and DOIs for filtering.
    Downloads source files from S3.
    Yields a context for processing the subset.
    Uploads the subset to S3 and cleans up local files.

    Args:
        bucket_name: The name of the S3 bucket.
        dataset: The name of the dataset.
        run_id: The unique identifier for the run.
        prev_run_id: Run ID of the prior download job to read source data from (defaults to run_id).
        dataset_subset: Configuration for the dataset subset.

    Yields:
        A DatasetSubsetAWSTaskContext object.
    """
    src_run_id = prev_run_id or run_id
    meta_dir = local_path(f"{dataset}-meta", run_id)
    download_dir = local_path(f"{dataset}-download", src_run_id)
    subset_dir = local_path(f"{dataset}-subset", run_id)
    target_uri = s3_uri(bucket_name, f"{dataset}-subset", run_id) + "/"

    # Download institutions
    institutions_uri = s3_uri(bucket_name, dataset_subset.institutions_s3_path)
    institutions_path = meta_dir / "institutions.json"
    download_file_from_s3(institutions_uri, institutions_path)
    institutions = load_institutions(institutions_path)
    log.info(f"institutions: {institutions}")

    # Download DOIs
    dois_uri = s3_uri(bucket_name, dataset_subset.dois_s3_path)
    dois_path = meta_dir / "dois.json"
    download_file_from_s3(dois_uri, dois_path)
    dois = load_dois(dois_path)
    log.info(f"dois: {dois}")

    # Download files
    clean_s3_prefix(target_uri)
    download_uri = s3_uri(bucket_name, f"{dataset}-download", src_run_id, "*")
    download_files_from_s3(download_uri, download_dir)
    subset_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Transforming {dataset}")
    ctx = DatasetSubsetAWSTaskContext(
        download_dir=download_dir,
        subset_dir=subset_dir,
        target_uri=target_uri,
        institutions=institutions,
        dois=dois,
    )
    yield ctx

    upload_files_to_s3(subset_dir, target_uri, "*")

    # Cleanup files as we can't guarantee that we will end up on the same worker
    # again, and we don't want to take disk space that other tasks might use
    shutil.rmtree(download_dir, ignore_errors=True)
    shutil.rmtree(subset_dir, ignore_errors=True)
    shutil.rmtree(meta_dir, ignore_errors=True)


@dataclass
class TransformTaskContext:
    """Context for a transform task.

    Attributes:
        download_dir: The local directory where source files are downloaded.
        transform_dir: The local directory where transformed files are saved.
        target_uri: The S3 URI where transformed files will be uploaded.
    """

    download_dir: pathlib.Path
    transform_dir: pathlib.Path
    target_uri: str


@contextmanager
def transform_parquets_task(
    bucket_name: str, dataset: str, run_id: str, use_subset: bool = False, source_run_id: str | None = None
) -> Generator[TransformTaskContext, Any, None]:
    """Context manager for transforming Parquet files.

    Downloads source files (either full dataset or subset).
    Yields a context for transformation.
    Uploads transformed Parquet files to S3 and cleans up local files.

    Args:
        bucket_name: The name of the S3 bucket.
        dataset: The name of the dataset.
        run_id: The unique identifier for this transform run.
        use_subset: Whether to use the subset of the dataset.
        source_run_id: Run ID of the download job to read from (defaults to run_id).

    Yields:
        A TransformTaskContext object.
    """
    src_run_id = source_run_id or run_id
    phase = "subset" if use_subset else "download"
    download_dir = local_path(f"{dataset}-{phase}", src_run_id)
    transform_dir = local_path(f"{dataset}-transform", run_id)
    target_uri = s3_uri(bucket_name, f"{dataset}-transform", run_id) + "/"

    clean_s3_prefix(target_uri)

    download_uri = s3_uri(bucket_name, f"{dataset}-{phase}", src_run_id, "*")
    download_files_from_s3(download_uri, download_dir)
    transform_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Transforming {dataset}")
    ctx = TransformTaskContext(
        download_dir=download_dir,
        transform_dir=transform_dir,
        target_uri=target_uri,
    )
    yield ctx

    upload_files_to_s3(transform_dir, target_uri, "*.parquet")

    # Cleanup files as we can't guarantee that we will end up on the same worker
    # again, and we don't want to take disk space that other tasks might use
    shutil.rmtree(download_dir, ignore_errors=True)
    shutil.rmtree(transform_dir, ignore_errors=True)
