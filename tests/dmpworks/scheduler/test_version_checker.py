from __future__ import annotations

from functools import partial
import json
from unittest.mock import MagicMock, patch

from dmpworks.model.dataset_version_model import DatasetRelease
from dmpworks.scheduler.version_checker import (
    detect_crossref_version,
    detect_datacite_version,
    detect_dcc_version,
    detect_openalex_version,
    detect_ror_version,
    list_zenodo_records,
)
import pendulum
import vcr

from tests.utils import get_fixtures_path

FIXTURES_FOLDER = get_fixtures_path()
SCHEDULER_FIXTURES = FIXTURES_FOLDER / "scheduler"

# AWS signing and auth headers to strip before saving cassettes.
# Covers both the DataCite credential API (basic auth) and subsequent S3 calls
# (AWS Signature v4 + session token).
CREDENTIAL_HEADERS = [
    "Authorization",
    "User-Agent",
    "X-Amz-Content-SHA256",
    "X-Amz-Date",
    "X-Amz-Security-Token",
    "amz-sdk-invocation-id",
    "amz-sdk-request",
    "x-amz-request-payer",
    "x-amz-checksum-mode",
]
HEADER_KEYS = [
    "ETag",
    "X-Credential-Username",
    "X-Request-Id",
    "x-amz-checksum-crc64nvme",
    "x-amz-id-2",
    "x-amz-request-id",
    "x-amz-version-id",
]

OPENALEX_CASSETTE = SCHEDULER_FIXTURES / "openalex_manifest.yaml"
DATACITE_CASSETTE = SCHEDULER_FIXTURES / "datacite_status.yaml"
CROSSREF_CASSETTE = SCHEDULER_FIXTURES / "crossref_versions.yaml"


class TestDetectOpenAlexVersion:
    def test_returns_latest_publication_date(self):
        """Parses the S3 manifest and returns the latest updated_date across all entries."""
        with vcr.use_cassette(
            OPENALEX_CASSETTE,
            filter_headers=CREDENTIAL_HEADERS,
            before_record_response=partial(
                vcrpy_clean_response,
                header_keys=HEADER_KEYS,
            ),
        ):
            result = detect_openalex_version(openalex_bucket_name="openalex")

        assert isinstance(result, DatasetRelease)
        assert isinstance(result.publication_date, pendulum.Date)
        assert result.publication_date == pendulum.date(2026, 2, 25)

    def test_empty_entries_returns_none(self):
        """Returns None when the manifest has no entries."""
        body = MagicMock()
        body.read.return_value = b'{"meta": {}, "entries": []}'

        with patch("boto3.client") as mock_boto:
            mock_boto.return_value.get_object.return_value = {"Body": body}
            result = detect_openalex_version(openalex_bucket_name="openalex")

        assert result is None

    def test_future_start_dt_returns_none(self):
        """Returns None when start_dt is after the detected publication date."""
        with vcr.use_cassette(
            OPENALEX_CASSETTE,
            filter_headers=CREDENTIAL_HEADERS,
            before_record_response=partial(
                vcrpy_clean_response,
                header_keys=HEADER_KEYS,
            ),
        ):
            result = detect_openalex_version(
                openalex_bucket_name="openalex",
                start_dt=pendulum.datetime(2099, 1, 1, tz="UTC"),
            )

        assert result is None


def make_dummy(key: str) -> str:
    return "DUMMY_" + key.upper().replace("-", "_")


def vcrpy_clean_response(
    response: dict,
    body_keys: list[str] | None = None,
    header_keys: list[str] | None = None,
) -> dict:
    """Vcrpy before_record_response callback that replaces sensitive values with dummy placeholders.

    Bind body_keys/header_keys with functools.partial before passing to vcr.use_cassette.

    Args:
        response: The vcrpy response dict.
        body_keys: JSON body keys to redact. Each key is replaced with DUMMY_{KEY_UPPERCASED}.
        header_keys: Response header names to redact. Each value is replaced with DUMMY_{HEADER_UPPERCASED}.
    """
    if body_keys:
        try:
            body = response["body"]["string"]
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            data = json.loads(body)
            for key in body_keys:
                if key in data:
                    data[key] = make_dummy(key)
            body_bytes = json.dumps(data).encode("utf-8")
            response["body"]["string"] = body_bytes
            if "Content-Length" in response.get("headers", {}):
                response["headers"]["Content-Length"] = [str(len(body_bytes))]
        except Exception:
            pass

    if header_keys:
        headers = response.get("headers", {})
        for key in header_keys:
            if key in headers:
                headers[key] = [make_dummy(key)]

    return response


class TestDetectDataCiteVersion:
    def test_returns_latest_publication_date(self):
        """Reads STATUS.json via DataCite credentials and returns the publication date."""
        env = {"DATACITE_ACCOUNT_ID": "dummy", "DATACITE_PASSWORD": "dummy"}
        with (
            patch.dict("os.environ", env),
            vcr.use_cassette(
                DATACITE_CASSETTE,
                filter_headers=CREDENTIAL_HEADERS,
                before_record_response=partial(
                    vcrpy_clean_response,
                    body_keys=["access_key_id", "secret_access_key", "session_token"],
                    header_keys=HEADER_KEYS,
                ),
            ),
        ):
            result = detect_datacite_version(
                datacite_bucket_name="monthly-datafile.datacite.org",
                datacite_bucket_region="eu-west-1",
            )

        assert isinstance(result, DatasetRelease)
        assert isinstance(result.publication_date, pendulum.Date)
        assert result.publication_date == pendulum.date(2026, 3, 1)


class TestDetectCrossrefVersion:
    def test_returns_latest_tar_file(self):
        """Lists the requester-pays S3 bucket and returns the most recent .tar release."""
        env = {
            "AWS_ACCESS_KEY_ID": "dummy",
            "AWS_SECRET_ACCESS_KEY": "dummy",
            "AWS_DEFAULT_REGION": "us-east-1",
        }
        with (
            patch.dict("os.environ", env),
            vcr.use_cassette(
                CROSSREF_CASSETTE,
                filter_headers=CREDENTIAL_HEADERS,
                before_record_response=partial(
                    vcrpy_clean_response,
                    header_keys=HEADER_KEYS,
                ),
            ),
        ):
            result = detect_crossref_version(crossref_bucket_name="api-snapshots-reqpays-crossref")

        assert isinstance(result, DatasetRelease)
        assert result.publication_date == pendulum.date(2025, 4, 1)
        assert result.file_name == "April_2025_Public_Data_File_from_Crossref.tar"


class TestDetectRorVersion:
    def test_returns_latest_release(self):
        """Returns the most recent ROR release with download_url and file_hash."""
        with vcr.use_cassette(SCHEDULER_FIXTURES / "ror_zenodo.yaml"):
            result = detect_ror_version(start_dt=None)

        assert isinstance(result, DatasetRelease)
        assert result.publication_date == pendulum.date(2026, 3, 12)
        assert (
            result.download_url == "https://zenodo.org/api/records/18985120/files/v2.4-2026-03-12-ror-data.zip/content"
        )
        assert result.file_hash == "md5:b04f7419253f96846365a0a36b5041aa"
        assert result.file_name == "v2.4-2026-03-12-ror-data.zip"

    def test_future_start_dt_returns_none(self):
        """Returns None when start_dt is in the future."""
        with vcr.use_cassette(SCHEDULER_FIXTURES / "ror_zenodo.yaml"):
            result = detect_ror_version(start_dt=pendulum.datetime(2099, 1, 1, tz="UTC"))

        assert result is None


class TestDetectDccVersion:
    def test_returns_latest_json_zip(self):
        """Returns the most recent DCC -json.zip release with download_url and file_hash."""
        with vcr.use_cassette(SCHEDULER_FIXTURES / "dcc_zenodo.yaml"):
            result = detect_dcc_version(start_dt=None)

        assert isinstance(result, DatasetRelease)
        assert result.publication_date == pendulum.date(2025, 8, 15)
        assert (
            result.download_url
            == "https://zenodo.org/api/records/16901115/files/2025-08-15-data-citation-corpus-v4.1-json.zip/content"
        )
        assert result.file_hash == "md5:601293df895148b315fdf5395484e768"
        assert result.file_name == "2025-08-15-data-citation-corpus-v4.1-json.zip"

    def test_future_start_dt_returns_none(self):
        """Returns None when start_dt is in the future."""
        with vcr.use_cassette(SCHEDULER_FIXTURES / "dcc_zenodo.yaml"):
            result = detect_dcc_version(start_dt=pendulum.datetime(2099, 1, 1, tz="UTC"))

        assert result is None


class TestListZenodoRecords:
    def test_sorted_descending(self):
        """Returns records sorted by publication_date descending."""
        with vcr.use_cassette(
            SCHEDULER_FIXTURES / "ror_zenodo.yaml",
            decode_compressed_response=False,
        ):
            records = list_zenodo_records(conceptrecid=6347574, end_date=pendulum.now("UTC"))

        assert len(records) > 0
        dates = [r.publication_date for r in records]
        assert dates == sorted(dates, reverse=True)

    def test_start_date_filters_older_records(self):
        """Excludes records with publication_date before start_date."""
        with vcr.use_cassette(
            SCHEDULER_FIXTURES / "ror_zenodo.yaml",
            decode_compressed_response=False,
        ):
            all_records = list_zenodo_records(conceptrecid=6347574, end_date=pendulum.now("UTC"))
        start_date = pendulum.datetime(2025, 1, 1)
        assert len(all_records) > 1
        assert any(r.publication_date <= start_date.date() for r in all_records)

        with vcr.use_cassette(SCHEDULER_FIXTURES / "ror_zenodo.yaml"):
            filtered = list_zenodo_records(
                conceptrecid=6347574,
                start_date=start_date,
                end_date=pendulum.now("UTC"),
            )
        assert all(r.publication_date >= start_date.date() for r in filtered)
