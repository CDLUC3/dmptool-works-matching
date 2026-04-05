import logging

from dmpworks.cli import cli
import pytest

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
    "RUN_ID_SQLMESH_PREV",
    "RUN_ID_SQLMESH",
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
                "20250101T060000-a1b2c3d4",
                "April_2025.tar",
                "crossref-source-bucket",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            file_name="April_2025.tar",
            crossref_bucket_name="crossref-source-bucket",
        )

    def test_transform_defaults(self, mock_transform):
        from dmpworks.cli_utils import CrossrefMetadataTransformConfig

        cli(["aws-batch", "crossref-metadata", "transform", "my-bucket", "20250101T060000-a1b2c3d4"])

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            config=CrossrefMetadataTransformConfig(),
            use_subset=False,
            source_run_id=None,
            log_level=logging.INFO,
        )

    def test_transform_config_from_env_var(self, monkeypatch, mock_transform):
        from dmpworks.cli_utils import CrossrefMetadataTransformConfig

        monkeypatch.setenv("CROSSREF_METADATA_TRANSFORM_MAX_WORKERS", "4")

        cli(["aws-batch", "crossref-metadata", "transform", "my-bucket", "20250101T060000-a1b2c3d4"])

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            config=CrossrefMetadataTransformConfig(max_workers=4),
            use_subset=False,
            source_run_id=None,
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
                "20250101T060000-a1b2c3d4",
                "--use-subset=true",
            ]
        )

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            config=CrossrefMetadataTransformConfig(),
            use_subset=True,
            source_run_id=None,
            log_level=logging.INFO,
        )


class TestDataCiteCLI:
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
                "20250101T060000-a1b2c3d4",
                "datacite-source-bucket",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            datacite_bucket_name="datacite-source-bucket",
        )

    def test_transform_defaults(self, mock_transform):
        from dmpworks.cli_utils import DataCiteTransformConfig

        cli(["aws-batch", "datacite", "transform", "my-bucket", "20250101T060000-a1b2c3d4"])

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            config=DataCiteTransformConfig(),
            use_subset=False,
            source_run_id=None,
            log_level=logging.INFO,
        )


class TestOpenAlexWorksCLI:
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
                "20250101T060000-a1b2c3d4",
                "openalex-source-bucket",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            openalex_bucket_name="openalex-source-bucket",
        )

    def test_transform_defaults(self, mock_transform):
        from dmpworks.cli_utils import OpenAlexWorksTransformConfig

        cli(["aws-batch", "openalex-works", "transform", "my-bucket", "20250101T060000-a1b2c3d4"])

        mock_transform.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            config=OpenAlexWorksTransformConfig(),
            use_subset=False,
            source_run_id=None,
            log_level=logging.INFO,
        )


class TestRorCLI:
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
                "20250101T060000-a1b2c3d4",
                "https://zenodo.org/records/123/files/ror.zip",
                "--file-hash",
                "md5:abc123",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            download_url="https://zenodo.org/records/123/files/ror.zip",
            file_hash="md5:abc123",
        )

    def test_download_no_hash(self, mock_download):
        cli(
            [
                "aws-batch",
                "ror",
                "download",
                "my-bucket",
                "20250101T060000-a1b2c3d4",
                "https://zenodo.org/records/123/files/ror.zip",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            download_url="https://zenodo.org/records/123/files/ror.zip",
            file_hash=None,
        )


class TestSQLMeshCLI:
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

        monkeypatch.setenv("RUN_ID_SQLMESH", "20250101T060000-a1b2c3d4")
        monkeypatch.setenv("RUN_ID_SQLMESH_PREV", "20241201T060000-b2c3d4e5")

        cli(["aws-batch", "sqlmesh", "plan", "my-bucket"])

        expected_run_identifiers = RunIdentifiers(
            run_id_sqlmesh="20250101T060000-a1b2c3d4",
            run_id_sqlmesh_prev="20241201T060000-b2c3d4e5",
        )
        mock_plan.assert_called_once_with(
            bucket_name="my-bucket",
            run_identifiers=expected_run_identifiers,
            sqlmesh_config=SQLMeshConfig(),
        )


class TestOpenSearchCLI:
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

        monkeypatch.setenv("RUN_ID_SQLMESH", "20250101T060000-a1b2c3d4")
        monkeypatch.setenv("RELEASE_DATE_PROCESS_WORKS", "2025-01-01")

        cli(["aws-batch", "opensearch", "sync-works", "my-bucket", "works-index"])

        mock_sync_works.assert_called_once_with(
            bucket_name="my-bucket",
            release_date="2025-01-01",
            sqlmesh_run_id="20250101T060000-a1b2c3d4",
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
                "20250101T060000-a1b2c3d4",
                "dmps-index",
                "works-index",
            ]
        )

        mock_dmp_works_search.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            dmps_index_name="dmps-index",
            works_index_name="works-index",
            client_config=OpenSearchClientConfig(),
            dmp_subset=None,
            dmp_works_search_config=None,
        )

    def test_merge_related_works(self, monkeypatch, mock_merge_related_works):
        from dmpworks.cli_utils import MySQLConfig

        monkeypatch.setenv("MYSQL_HOST", "db.example.com")
        monkeypatch.setenv("MYSQL_TCP_PORT", "3306")
        monkeypatch.setenv("MYSQL_USER", "admin")
        monkeypatch.setenv("MYSQL_DATABASE", "dmpworks")
        monkeypatch.setenv("MYSQL_PWD", "secret")

        cli(
            [
                "aws-batch",
                "opensearch",
                "merge-related-works",
                "my-bucket",
                "20250101T060000-a1b2c3d4",
                "20250101T070000-c3d4e5f6",
            ]
        )

        mock_merge_related_works.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            search_run_id="20250101T070000-c3d4e5f6",
            mysql_config=MySQLConfig(
                mysql_host="db.example.com",
                mysql_tcp_port=3306,
                mysql_user="admin",
                mysql_database="dmpworks",
                mysql_pwd="secret",
            ),
            insert_batch_size=1000,
        )

    def test_sync_dmps(self, monkeypatch, mocker):
        from dmpworks.cli_utils import MySQLConfig, OpenSearchClientConfig

        monkeypatch.setenv("MYSQL_HOST", "db.example.com")
        monkeypatch.setenv("MYSQL_TCP_PORT", "3306")
        monkeypatch.setenv("MYSQL_USER", "admin")
        monkeypatch.setenv("MYSQL_DATABASE", "dmpworks")
        monkeypatch.setenv("MYSQL_PWD", "secret")

        mock = mocker.patch("dmpworks.batch.opensearch.sync_dmps_cmd")
        cli(["aws-batch", "opensearch", "sync-dmps", "my-bucket", "dmps-index"])

        mock.assert_called_once_with(
            bucket_name="my-bucket",
            index_name="dmps-index",
            client_config=OpenSearchClientConfig(),
            mysql_config=MySQLConfig(
                mysql_host="db.example.com",
                mysql_tcp_port=3306,
                mysql_user="admin",
                mysql_database="dmpworks",
                mysql_pwd="secret",
            ),
            dmp_subset=None,
        )


class TestDataCitationCorpusCLI:
    @pytest.fixture
    def mock_download(self, mocker):
        return mocker.patch("dmpworks.batch.data_citation_corpus.download")

    def test_download(self, mock_download):
        cli(
            [
                "aws-batch",
                "data-citation-corpus",
                "download",
                "my-bucket",
                "20250101T060000-a1b2c3d4",
                "https://zenodo.org/records/123/files/dcc.json.zip",
            ]
        )

        mock_download.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            download_url="https://zenodo.org/records/123/files/dcc.json.zip",
            file_hash=None,
        )


class TestDatasetSubsetCLI:
    @pytest.fixture
    def mock_datacite_subset(self, mocker):
        return mocker.patch("dmpworks.batch.datacite.dataset_subset")

    @pytest.fixture
    def mock_crossref_subset(self, mocker):
        return mocker.patch("dmpworks.batch.crossref_metadata.dataset_subset")

    @pytest.fixture
    def mock_openalex_subset(self, mocker):
        return mocker.patch("dmpworks.batch.openalex_works.dataset_subset")

    def test_datacite_subset(self, mock_datacite_subset):
        from dmpworks.cli_utils import DatasetSubsetAWS

        cli(
            [
                "aws-batch",
                "datacite",
                "subset",
                "my-bucket",
                "20250101T060000-a1b2c3d4",
                "--dataset-subset.enable=false",
            ]
        )

        mock_datacite_subset.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            ds_config=DatasetSubsetAWS(),
            prev_run_id=None,
        )

    def test_crossref_metadata_subset(self, mock_crossref_subset):
        from dmpworks.cli_utils import DatasetSubsetAWS

        cli(
            [
                "aws-batch",
                "crossref-metadata",
                "subset",
                "my-bucket",
                "20250101T060000-a1b2c3d4",
                "--dataset-subset.enable=false",
            ]
        )

        mock_crossref_subset.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            ds_config=DatasetSubsetAWS(),
            prev_run_id=None,
        )

    def test_openalex_works_subset(self, mock_openalex_subset):
        from dmpworks.cli_utils import DatasetSubsetAWS

        cli(
            [
                "aws-batch",
                "openalex-works",
                "subset",
                "my-bucket",
                "20250101T060000-a1b2c3d4",
                "--dataset-subset.enable=false",
            ]
        )

        mock_openalex_subset.assert_called_once_with(
            bucket_name="my-bucket",
            run_id="20250101T060000-a1b2c3d4",
            ds_config=DatasetSubsetAWS(),
            prev_run_id=None,
        )
