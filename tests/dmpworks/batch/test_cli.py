import logging

from dmpworks.cli import cli
import pytest

SETUP_LOGGING = "dmpworks.utils.setup_multiprocessing_logging"

CROSSREF_TRANSFORM_VARS = [
    "CROSSREF_METADATA_TRANSFORM_BATCH_SIZE",
    "CROSSREF_METADATA_TRANSFORM_ROW_GROUP_SIZE",
    "CROSSREF_METADATA_TRANSFORM_ROW_GROUPS_PER_FILE",
    "CROSSREF_METADATA_TRANSFORM_MAX_WORKERS",
]
DATACITE_TRANSFORM_VARS = [
    "DATACITE_TRANSFORM_BATCH_SIZE",
    "DATACITE_TRANSFORM_ROW_GROUP_SIZE",
    "DATACITE_TRANSFORM_ROW_GROUPS_PER_FILE",
    "DATACITE_TRANSFORM_MAX_WORKERS",
]
OPENALEX_TRANSFORM_VARS = [
    "OPENALEX_WORKS_TRANSFORM_BATCH_SIZE",
    "OPENALEX_WORKS_TRANSFORM_ROW_GROUP_SIZE",
    "OPENALEX_WORKS_TRANSFORM_ROW_GROUPS_PER_FILE",
    "OPENALEX_WORKS_TRANSFORM_MAX_WORKERS",
]
OPENSEARCH_CLIENT_VARS = [
    "OPENSEARCH_HOST",
    "OPENSEARCH_PORT",
    "OPENSEARCH_USE_SSL",
    "OPENSEARCH_VERIFY_CERTS",
    "OPENSEARCH_AUTH_TYPE",
    "OPENSEARCH_USERNAME",
    "OPENSEARCH_PASSWORD",
    "OPENSEARCH_REGION",
    "OPENSEARCH_SERVICE",
    "OPENSEARCH_POOL_MAXSIZE",
    "OPENSEARCH_TIMEOUT",
]
OPENSEARCH_SYNC_VARS = [
    "OPENSEARCH_SYNC_MAX_PROCESSES",
    "OPENSEARCH_SYNC_CHUNK_SIZE",
    "OPENSEARCH_SYNC_MAX_CHUNK_BYTES",
    "OPENSEARCH_SYNC_MAX_RETRIES",
    "OPENSEARCH_SYNC_INITIAL_BACKOFF",
    "OPENSEARCH_SYNC_MAX_BACKOFF",
    "OPENSEARCH_SYNC_DRY_RUN",
    "OPENSEARCH_SYNC_MEASURE_CHUNK_SIZE",
    "OPENSEARCH_SYNC_MAX_ERROR_SAMPLES",
    "OPENSEARCH_SYNC_STAGGERED_START",
]
RUN_ID_VARS = [
    "RUN_ID_OPENALEX_WORKS",
    "RUN_ID_DATACITE",
    "RUN_ID_CROSSREF_METADATA",
    "RUN_ID_ROR",
    "RUN_ID_DATA_CITATION_CORPUS",
    "RUN_ID_PROCESS_WORKS_PREV",
    "RUN_ID_PROCESS_WORKS",
    "RUN_ID_DMPS",
]
MYSQL_VARS = [
    "MYSQL_HOST",
    "MYSQL_TCP_PORT",
    "MYSQL_USER",
    "MYSQL_DATABASE",
    "MYSQL_PWD",
]


def clear_env(monkeypatch, *var_groups):
    for group in var_groups:
        for var in group:
            monkeypatch.delenv(var, raising=False)


class TestCrossrefMetadataCLI:
    @pytest.fixture(autouse=True)
    def mock_setup_logging(self, mocker):
        return mocker.patch(SETUP_LOGGING)

    @pytest.fixture(autouse=True)
    def isolate_env(self, monkeypatch):
        clear_env(monkeypatch, CROSSREF_TRANSFORM_VARS)

    @pytest.fixture
    def mock_download(self, mocker):
        return mocker.patch("dmpworks.batch.crossref_metadata.download")

    @pytest.fixture
    def mock_transform(self, mocker):
        return mocker.patch("dmpworks.batch.crossref_metadata.transform")

    def test_download(self, mock_download):
        cli(
            [
                "aws-batch",
                "crossref-metadata",
                "download",
                "my-bucket",
                "2025-01-01",
                "April_2025.tar",
                "crossref-source-bucket",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            file_name="April_2025.tar",
            crossref_bucket_name="crossref-source-bucket",
        )

    def test_transform_defaults(self, mock_transform):
        from dmpworks.cli_utils import CrossrefMetadataTransformConfig

        cli(["aws-batch", "crossref-metadata", "transform", "my-bucket", "2025-01-01"])

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            config=CrossrefMetadataTransformConfig(),
            use_subset=False,
            log_level=logging.INFO,
        )

    def test_transform_config_from_env_var(self, monkeypatch, mock_transform):
        from dmpworks.cli_utils import CrossrefMetadataTransformConfig

        monkeypatch.setenv("CROSSREF_METADATA_TRANSFORM_MAX_WORKERS", "4")

        cli(["aws-batch", "crossref-metadata", "transform", "my-bucket", "2025-01-01"])

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            config=CrossrefMetadataTransformConfig(max_workers=4),
            use_subset=False,
            log_level=logging.INFO,
        )

    def test_transform_use_subset(self, mock_transform):
        from dmpworks.cli_utils import CrossrefMetadataTransformConfig

        cli(
            [
                "aws-batch",
                "crossref-metadata",
                "transform",
                "my-bucket",
                "2025-01-01",
                "--use-subset=true",
            ]
        )

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            config=CrossrefMetadataTransformConfig(),
            use_subset=True,
            log_level=logging.INFO,
        )


class TestDataCiteCLI:
    @pytest.fixture(autouse=True)
    def mock_setup_logging(self, mocker):
        return mocker.patch(SETUP_LOGGING)

    @pytest.fixture(autouse=True)
    def isolate_env(self, monkeypatch):
        clear_env(monkeypatch, DATACITE_TRANSFORM_VARS)

    @pytest.fixture
    def mock_download(self, mocker):
        return mocker.patch("dmpworks.batch.datacite.download")

    @pytest.fixture
    def mock_transform(self, mocker):
        return mocker.patch("dmpworks.batch.datacite.transform")

    def test_download(self, mock_download):
        cli(
            [
                "aws-batch",
                "datacite",
                "download",
                "my-bucket",
                "2025-01-01",
                "datacite-source-bucket",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            datacite_bucket_name="datacite-source-bucket",
        )

    def test_transform_defaults(self, mock_transform):
        from dmpworks.cli_utils import DataCiteTransformConfig

        cli(["aws-batch", "datacite", "transform", "my-bucket", "2025-01-01"])

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            config=DataCiteTransformConfig(),
            use_subset=False,
            log_level=logging.INFO,
        )


class TestOpenAlexWorksCLI:
    @pytest.fixture(autouse=True)
    def mock_setup_logging(self, mocker):
        return mocker.patch(SETUP_LOGGING)

    @pytest.fixture(autouse=True)
    def isolate_env(self, monkeypatch):
        clear_env(monkeypatch, OPENALEX_TRANSFORM_VARS)

    @pytest.fixture
    def mock_download(self, mocker):
        return mocker.patch("dmpworks.batch.openalex_works.download")

    @pytest.fixture
    def mock_transform(self, mocker):
        return mocker.patch("dmpworks.batch.openalex_works.transform")

    def test_download(self, mock_download):
        cli(
            [
                "aws-batch",
                "openalex-works",
                "download",
                "my-bucket",
                "2025-01-01",
                "openalex-source-bucket",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            openalex_bucket_name="openalex-source-bucket",
        )

    def test_transform_defaults(self, mock_transform):
        from dmpworks.cli_utils import OpenAlexWorksTransformConfig

        cli(["aws-batch", "openalex-works", "transform", "my-bucket", "2025-01-01"])

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            config=OpenAlexWorksTransformConfig(),
            use_subset=False,
            log_level=logging.INFO,
        )


class TestRorCLI:
    @pytest.fixture(autouse=True)
    def mock_setup_logging(self, mocker):
        return mocker.patch(SETUP_LOGGING)

    @pytest.fixture
    def mock_download(self, mocker):
        return mocker.patch("dmpworks.batch.ror.download")

    def test_download(self, mock_download):
        cli(
            [
                "aws-batch",
                "ror",
                "download",
                "my-bucket",
                "2025-01-01",
                "https://zenodo.org/records/123/files/ror.zip",
                "--hash",
                "md5:abc123",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            download_url="https://zenodo.org/records/123/files/ror.zip",
            hash="md5:abc123",
        )

    def test_download_no_hash(self, mock_download):
        cli(
            [
                "aws-batch",
                "ror",
                "download",
                "my-bucket",
                "2025-01-01",
                "https://zenodo.org/records/123/files/ror.zip",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            download_url="https://zenodo.org/records/123/files/ror.zip",
            hash=None,
        )


class TestSQLMeshCLI:
    @pytest.fixture(autouse=True)
    def mock_setup_logging(self, mocker):
        return mocker.patch(SETUP_LOGGING)

    @pytest.fixture(autouse=True)
    def isolate_env(self, monkeypatch):
        clear_env(monkeypatch, RUN_ID_VARS)
        # Clear a representative sample of SQLMesh config vars
        for var in ["DUCKDB_THREADS", "DUCKDB_MEMORY_LIMIT"]:
            monkeypatch.delenv(var, raising=False)

    @pytest.fixture
    def mock_plan(self, mocker):
        return mocker.patch("dmpworks.batch.sql.plan")

    def test_plan(self, monkeypatch, mock_plan):
        from dmpworks.cli_utils import RunIdentifiers, SQLMeshConfig

        monkeypatch.setenv("RUN_ID_PROCESS_WORKS", "2025-01-01")
        monkeypatch.setenv("RUN_ID_PROCESS_WORKS_PREV", "2024-12-01")

        cli(["aws-batch", "sqlmesh", "plan", "my-bucket"])

        expected_run_identifiers = RunIdentifiers(
            run_id_process_works="2025-01-01",
            run_id_process_works_prev="2024-12-01",
        )
        mock_plan.assert_called_once_with(
            bucket_name="my-bucket",
            run_identifiers=expected_run_identifiers,
            sqlmesh_config=SQLMeshConfig(),
        )


class TestOpenSearchCLI:
    @pytest.fixture(autouse=True)
    def mock_setup_logging(self, mocker):
        return mocker.patch(SETUP_LOGGING)

    @pytest.fixture(autouse=True)
    def isolate_env(self, monkeypatch):
        clear_env(
            monkeypatch,
            OPENSEARCH_CLIENT_VARS,
            OPENSEARCH_SYNC_VARS,
            RUN_ID_VARS,
            MYSQL_VARS,
        )

    @pytest.fixture
    def mock_sync_works(self, mocker):
        return mocker.patch("dmpworks.batch.opensearch.sync_works_cmd")

    @pytest.fixture
    def mock_enrich_dmps(self, mocker):
        return mocker.patch("dmpworks.batch.opensearch.enrich_dmps_cmd")

    @pytest.fixture
    def mock_dmp_works_search(self, mocker):
        return mocker.patch("dmpworks.batch.opensearch.dmp_works_search_cmd")

    @pytest.fixture
    def mock_merge_related_works(self, mocker):
        return mocker.patch("dmpworks.batch.opensearch.merge_related_works_cmd")

    def test_sync_works(self, monkeypatch, mock_sync_works):
        from dmpworks.cli_utils import OpenSearchClientConfig, OpenSearchSyncConfig

        monkeypatch.setenv("RUN_ID_PROCESS_WORKS", "2025-01-01")

        cli(["aws-batch", "opensearch", "sync-works", "my-bucket", "works-index"])

        mock_sync_works.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="2025-01-01",
            index_name="works-index",
            client_config=OpenSearchClientConfig(),
            sync_config=OpenSearchSyncConfig(),
            log_level="INFO",
        )

    def test_enrich_dmps(self, mock_enrich_dmps):
        from dmpworks.cli_utils import OpenSearchClientConfig

        cli(["aws-batch", "opensearch", "enrich-dmps", "dmps-index"])

        mock_enrich_dmps.assert_called_once_with(
            index_name="dmps-index",
            client_config=OpenSearchClientConfig(),
            bucket_name=None,
            dmp_subset=None,
        )

    def test_dmp_works_search(self, mock_dmp_works_search):
        from dmpworks.cli_utils import OpenSearchClientConfig

        cli(
            [
                "aws-batch",
                "opensearch",
                "dmp-works-search",
                "my-bucket",
                "run-123",
                "dmps-index",
                "works-index",
            ]
        )

        mock_dmp_works_search.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="run-123",
            dmps_index_name="dmps-index",
            works_index_name="works-index",
            scroll_time="360m",
            batch_size=250,
            max_results=100,
            project_end_buffer_years=3,
            parallel_search=False,
            include_named_queries_score=True,
            max_concurrent_searches=125,
            max_concurrent_shard_requests=12,
            client_config=OpenSearchClientConfig(),
            dmp_subset=None,
            start_date=None,
            end_date=None,
        )

    def test_merge_related_works(self, monkeypatch, mock_merge_related_works):
        from dmpworks.cli_utils import MySQLConfig

        monkeypatch.setenv("MYSQL_HOST", "db.example.com")
        monkeypatch.setenv("MYSQL_TCP_PORT", "3306")
        monkeypatch.setenv("MYSQL_USER", "admin")
        monkeypatch.setenv("MYSQL_DATABASE", "dmpworks")
        monkeypatch.setenv("MYSQL_PWD", "secret")

        cli(["aws-batch", "opensearch", "merge-related-works", "my-bucket", "run-123"])

        mock_merge_related_works.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="run-123",
            mysql_config=MySQLConfig(
                mysql_host="db.example.com",
                mysql_tcp_port=3306,
                mysql_user="admin",
                mysql_database="dmpworks",
                mysql_pwd="secret",
            ),
            batch_size=1000,
        )
