import logging
import pathlib
from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from dmpworks.utils import run_process

log = logging.getLogger(__name__)


def s3_uri(bucket_name: str, *parts: str) -> str:
    path = "/".join(parts)
    return f"s3://{bucket_name}/{path}" if path else f"s3://{bucket_name}"


def data_path() -> pathlib.Path:
    return pathlib.Path("/") / "data"


def local_path(*parts: str) -> pathlib.Path:
    return pathlib.Path(data_path(), *parts)


def clean_s3_prefix(s3_uri: str):
    log.info(f"Checking and cleaning S3 URI: {s3_uri}")
    if s3_uri_has_files(s3_uri):
        log.info(f"Objects found at {s3_uri}, deleting...")
        run_process(["s5cmd", "rm", f"{s3_uri}*"])
    else:
        log.info(f"No objects found at {s3_uri}")


def upload_files_to_s3(local_dir: pathlib.Path, s3_uri: str, glob_pattern: str = "*"):
    log.info(f"Uploading from {local_dir}/{glob_pattern} to {s3_uri}")
    run_process(["s5cmd", "cp", f"{local_dir}/{glob_pattern}", s3_uri])


def upload_file_to_s3(file: pathlib.Path, s3_uri: str):
    log.info(f"Uploading {file} to {s3_uri}")
    run_process(["s5cmd", "cp", f"{file}", s3_uri])


def download_files_from_s3(source_uri: str, target_dir: pathlib.Path):
    log.info(f"Downloading from {source_uri} to {target_dir}")
    run_process(["s5cmd", "cp", source_uri, f"{target_dir}/"])


def download_file_from_s3(source_uri: str, target_file: pathlib.Path):
    log.info(f"Downloading from {source_uri} to {target_file}")
    run_process(["s5cmd", "cp", source_uri, str(target_file)])


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def s3_uri_has_files(
    s3_uri: str,
    *,
    s3_client: Optional[boto3.client] = None,
) -> bool:
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, prefix = parse_s3_uri(s3_uri)

    try:
        resp = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
    except ClientError as err:
        raise RuntimeError(f"Unable to list {s3_uri}") from err

    return "Contents" in resp
