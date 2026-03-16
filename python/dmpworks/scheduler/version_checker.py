from __future__ import annotations

import contextlib
import json
import logging
import re
from urllib.parse import urlencode

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
import pendulum

from dmpworks.model.common import parse_pendulum_date
from dmpworks.model.dataset_version_model import DatasetRelease
from dmpworks.model.zenodo_model import ZenodoFile, ZenodoRecord
from dmpworks.utils import fetch_datacite_aws_credentials, retry_session

log = logging.getLogger(__name__)


def list_zenodo_records(
    *,
    conceptrecid: int,
    start_date: pendulum.DateTime | None = None,
    end_date: pendulum.DateTime,
    page_size: int = 10,
    timeout: float = 30.0,
) -> list[ZenodoRecord]:
    """Fetch all Zenodo record versions for a concept within an optional date range.

    Args:
        conceptrecid: Zenodo concept record ID shared across all versions.
        start_date: Inclusive lower bound on publication date; no lower bound if None.
        end_date: Inclusive upper bound on publication date.
        page_size: Number of records to request per API page.
        timeout: HTTP request timeout in seconds.

    Returns:
        List of ZenodoRecord objects whose publication dates fall within the given range.
    """
    records: list[ZenodoRecord] = []
    page = 1

    start_bound = start_date.start_of("day") if start_date else None
    end_bound = end_date.end_of("day")

    while True:
        params = urlencode(
            {
                "q": f"conceptrecid:{conceptrecid}",
                "all_versions": "true",
                "sort": "mostrecent",
                "page": page,
                "size": page_size,
            }
        )

        url = f"https://zenodo.org/api/records?{params}"
        log.debug(f"Fetching Zenodo records: {url}")

        resp = retry_session().get(
            url,
            timeout=timeout,
            headers={
                "Accept-Encoding": "gzip",
            },
        )
        resp.raise_for_status()
        data = resp.json()

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        stop_paging = False

        for hit in hits:
            pub_date_str = hit.get("metadata", {}).get("publication_date")
            if not pub_date_str:
                continue

            try:
                pub_date = pendulum.from_format(pub_date_str, "YYYY-MM-DD", tz="UTC")
            except ValueError:
                log.warning("Could not parse Zenodo publication_date: %s", pub_date_str)
                continue

            if pub_date > end_bound:
                continue

            if start_bound and pub_date < start_bound:
                stop_paging = True
                break

            files: list[ZenodoFile] = [
                ZenodoFile(
                    link=f.get("links", {}).get("self"),
                    file_hash=f.get("checksum"),
                    file_name=f.get("key"),
                    file_type=f.get("type"),
                )
                for f in hit.get("files", [])
            ]

            records.append(
                ZenodoRecord(
                    publication_date=pub_date.date(),
                    files=files,
                )
            )

        if stop_paging or len(hits) < page_size:
            break

        page += 1

    records.sort(key=lambda r: r.publication_date, reverse=True)
    return records


def detect_openalex_version(
    *, openalex_bucket_name: str, start_dt: pendulum.DateTime | None = None
) -> DatasetRelease | None:
    """Detect the latest OpenAlex works version by reading its S3 manifest.

    The manifest is a JSON file with an ``entries`` list, each entry having a ``url``
    field of the form ``s3://.../updated_date=YYYY-MM-DD/...``. We find the latest
    updated_date from those urls.

    Args:
        openalex_bucket_name: Name of the OpenAlex S3 bucket.
        start_dt: Only return a version if its publication date is on or after this datetime.
            If None, no lower bound is applied.

    Returns:
        DatasetRelease with publication_date set, or None if unavailable or no new version detected.
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    try:
        resp = s3.get_object(Bucket=openalex_bucket_name, Key="data/works/manifest")
        manifest = json.loads(resp["Body"].read())
    except (ClientError, json.JSONDecodeError):
        log.exception("Failed to read OpenAlex manifest")
        return None

    parsed_dates = []
    for entry in manifest.get("entries", []):
        url = entry.get("url")
        if not isinstance(url, str):
            continue

        match = re.search(r"updated_date=(\d{4}-\d{2}-\d{2})", url)
        if match:
            with contextlib.suppress(ValueError):
                parsed_dates.append(parse_pendulum_date(match.group(1)))

    if not parsed_dates:
        return None

    latest_date = max(parsed_dates)
    log.info(f"OpenAlex latest date from manifest: {latest_date}")

    if start_dt is not None and latest_date < start_dt.date():
        return None

    return DatasetRelease(publication_date=latest_date)


def detect_datacite_version(
    *,
    datacite_bucket_name: str,
    datacite_bucket_region: str,
    start_dt: pendulum.DateTime | None = None,
    account_id: str | None = None,
    password: str | None = None,
) -> DatasetRelease | None:
    """Detect the latest DataCite version by reading its STATUS.json.

    DataCite's S3 bucket requires credentials obtained via ``fetch_datacite_aws_credentials``,
    the same mechanism used by the download job.

    Args:
        datacite_bucket_name: Name of the DataCite S3 bucket.
        datacite_bucket_region: Region of the DataCite S3 bucket.
        start_dt: Only return a version if its publication date is on or after this datetime.
            If None, no lower bound is applied.
        account_id: DataCite account ID. Falls back to ``DATACITE_ACCOUNT_ID`` env var if not provided.
        password: DataCite password. Falls back to ``DATACITE_PASSWORD`` env var if not provided.

    Returns:
        DatasetRelease with publication_date set, or None if unavailable or no new version detected.
    """
    try:
        access_key_id, secret_access_key, session_token = fetch_datacite_aws_credentials(
            account_id=account_id, password=password
        )
    except RuntimeError:
        log.exception("Failed to obtain DataCite AWS credentials")
        return None

    s3 = boto3.client(
        "s3",
        region_name=datacite_bucket_region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
    )
    try:
        resp = s3.get_object(Bucket=datacite_bucket_name, Key="STATUS.json")
        status = json.loads(resp["Body"].read())
    except (ClientError, json.JSONDecodeError):
        log.exception("Failed to read DataCite STATUS.json")
        return None

    if status.get("status") != "Complete":
        log.info(f"DataCite status is not Complete: {status.get('status')}")
        return None

    dt_str = status.get("datetime")
    if not dt_str:
        return None

    try:
        publication_date = parse_pendulum_date(dt_str)
    except (TypeError, ValueError):
        log.exception(f"Failed to parse DataCite datetime from STATUS.json: {dt_str}")
        return None

    log.info(f"DataCite STATUS.json: status={status.get('status')} publication_date={publication_date}")

    if start_dt is not None and publication_date < start_dt.date():
        return None

    return DatasetRelease(publication_date=publication_date)


def detect_crossref_version(
    *, crossref_bucket_name: str, start_dt: pendulum.DateTime | None = None
) -> DatasetRelease | None:
    """Detect the latest Crossref Metadata version by listing its S3 bucket.

    Args:
        crossref_bucket_name: Name of the Crossref Metadata S3 bucket.
        start_dt: Only return a version if its publication date is on or after this datetime.
            If None, no lower bound is applied.

    Returns:
        DatasetRelease with publication_date and file_name set, or None if unavailable or no new version detected.
    """
    s3 = boto3.client("s3")
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=crossref_bucket_name, RequestPayer="requester")
    except ClientError:
        log.exception("Failed to create paginator for Crossref Metadata bucket")
        return None

    best_date: pendulum.Date | None = None
    best_file: str | None = None

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".tar"):
                continue

            match = re.search(r"([A-Za-z]+_\d{4})_Public_Data_File", key)
            if match:
                try:
                    date = pendulum.from_format(match.group(1), "MMMM_YYYY").date()
                    if best_date is None or date > best_date:
                        best_date = date
                        best_file = key
                except ValueError:
                    pass

    if best_date is not None:
        log.info(f"Crossref best file found: file_name={best_file} publication_date={best_date}")

    if best_date is None:
        return None

    if start_dt is not None and best_date < start_dt.date():
        return None

    return DatasetRelease(publication_date=best_date, file_name=best_file)


def detect_ror_version(*, start_dt: pendulum.DateTime | None = None) -> DatasetRelease | None:
    """Detect the latest ROR version from Zenodo.

    Args:
        start_dt: Publication datetime of the most recent known version. Used as the
            inclusive lower bound for the Zenodo search. If None, no lower bound is applied.

    Returns:
        DatasetRelease with publication_date, download_url, file_name, and file_hash set,
        or None if no dataset or no new version was detected.
    """
    end_date = pendulum.now("UTC")
    records = list_zenodo_records(conceptrecid=6347574, start_date=start_dt, end_date=end_date)
    log.info(f"ROR Zenodo records found: {len(records)}")
    if not records:
        return None

    log.info(f"ROR latest record: publication_date={records[0].publication_date}")
    latest = records[0]
    if not latest.files:
        return None

    file = latest.files[0]
    return DatasetRelease(
        publication_date=latest.publication_date,
        download_url=file.link,
        file_name=file.file_name,
        file_hash=file.file_hash,
    )


def detect_dcc_version(*, start_dt: pendulum.DateTime | None = None) -> DatasetRelease | None:
    """Detect the latest Data Citation Corpus version from Zenodo.

    Args:
        start_dt: Publication datetime of the most recent known version. Used as the
            inclusive lower bound for the Zenodo search. If None, no lower bound is applied.

    Returns:
        DatasetRelease with publication_date, download_url, file_name, and file_hash set,
        or None if no dataset or no new version was detected.
    """
    end_date = pendulum.now("UTC")
    records = list_zenodo_records(conceptrecid=11196858, start_date=start_dt, end_date=end_date)
    log.info(f"DCC Zenodo records found: {len(records)}")
    if not records:
        return None

    log.info(f"DCC latest record: publication_date={records[0].publication_date} files={len(records[0].files)}")
    latest = records[0]
    json_file: ZenodoFile | None = None
    for f in latest.files:
        if f.file_name and f.file_name.endswith("-json.zip"):
            json_file = f
            break

    if json_file is None:
        log.warning("No -json.zip file found in DCC record")
        return None

    log.info(f"DCC selected json file: file_name={json_file.file_name}")
    return DatasetRelease(
        publication_date=latest.publication_date,
        download_url=json_file.link,
        file_name=json_file.file_name,
        file_hash=json_file.file_hash,
    )
