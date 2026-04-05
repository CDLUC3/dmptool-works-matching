from dmpworks.cli import cli
import pytest

MYSQL_VARS = [
    "MYSQL_HOST",
    "MYSQL_TCP_PORT",
    "MYSQL_USER",
    "MYSQL_DATABASE",
    "MYSQL_PWD",
]
OPENSEARCH_VARS = [
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


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch):
    for var in MYSQL_VARS + OPENSEARCH_VARS:
        monkeypatch.delenv(var, raising=False)


def set_mysql_env(monkeypatch):
    monkeypatch.setenv("MYSQL_HOST", "db.example.com")
    monkeypatch.setenv("MYSQL_TCP_PORT", "3306")
    monkeypatch.setenv("MYSQL_USER", "admin")
    monkeypatch.setenv("MYSQL_DATABASE", "dmpworks")
    monkeypatch.setenv("MYSQL_PWD", "secret")


class TestLoadMigration:
    def test_calls_load_related_works(self, monkeypatch, mocker):
        set_mysql_env(monkeypatch)
        mocker.patch("pymysql.connect")
        mocker.patch("dmpworks.opensearch.utils.make_opensearch_client")
        mocker.patch("dmpworks.dmsp.migration.fetch_migration_related_works")
        mock_load = mocker.patch("dmpworks.dmsp.loader.load_related_works")

        cli(["dmsp", "related-works", "load-migration", "--opensearch-config.host=localhost"])

        mock_load.assert_called_once()


class TestLoadGroundTruth:
    def test_calls_load_related_works(self, monkeypatch, mocker, tmp_path):
        set_mysql_env(monkeypatch)
        csv_file = tmp_path / "matches.csv"
        csv_file.write_text("header\nrow\n")

        mocker.patch("pymysql.connect")
        mocker.patch("dmpworks.opensearch.utils.make_opensearch_client")
        mocker.patch("dmpworks.dmsp.ground_truth.read_related_works_csv")
        mock_load = mocker.patch("dmpworks.dmsp.loader.load_related_works")

        cli(["dmsp", "related-works", "load-ground-truth", str(csv_file), "--opensearch-config.host=localhost"])

        mock_load.assert_called_once()


class TestMerge:
    def test_calls_merge_related_works(self, monkeypatch, mocker, tmp_path):
        set_mysql_env(monkeypatch)
        mock_merge = mocker.patch("dmpworks.dmsp.merge.merge_related_works")

        cli(["dmsp", "related-works", "merge", str(tmp_path)])

        mock_merge.assert_called_once()
        call_kwargs = mock_merge.call_args
        assert call_kwargs[0][0] == tmp_path
        assert call_kwargs[1]["insert_batch_size"] == 1000
        assert call_kwargs[1]["mysql_config"].mysql_host == "db.example.com"
