import logging
import pathlib

import pytest
from opensearchpy import OpenSearch

from dmpworks.cli import cli
from dmpworks.cli_utils import MySQLConfig
from dmpworks.opensearch.utils import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.utils import InstanceOf

CLI_MODULE = "dmpworks.opensearch.cli"


class TestOpenSearchCLI:

    @pytest.fixture
    def mock_create_index(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.create_index")

    def test_opensearch_create_index(self, mock_create_index):
        cli(["opensearch", "create-index", "works-index", "works-mapping.json"])

        mock_create_index.assert_called_once_with(
            InstanceOf(OpenSearch),
            "works-index",
            "works-mapping.json",
        )

    @pytest.fixture
    def mock_update_mapping(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.update_mapping")

    def test_opensearch_update_mapping(self, mock_update_mapping):
        cli(["opensearch", "update-mapping", "works-index", "works-mapping.json"])

        mock_update_mapping.assert_called_once_with(
            InstanceOf(OpenSearch),
            "works-index",
            "works-mapping.json",
        )

    @pytest.fixture
    def mock_sync_works(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.sync_works")

    def test_opensearch_sync_works(self, mock_sync_works, tmp_path: pathlib.Path):
        works_export = tmp_path / "works_export"
        doi_export = tmp_path / "doi_export"
        works_export.mkdir()
        doi_export.mkdir()

        cli(["opensearch", "sync-works", "works-index", str(works_export), str(doi_export), "run-123"])

        mock_sync_works.assert_called_once_with(
            index_name="works-index",
            works_index_export=works_export,
            doi_state_export=doi_export,
            run_id="run-123",
            client_config=OpenSearchClientConfig(),
            sync_config=OpenSearchSyncConfig(),
            log_level=logging.INFO,
        )

    @pytest.fixture
    def mock_sync_dmps(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.sync_dmps")

    def test_opensearch_sync_dmps(self, mock_sync_dmps):
        cli(
            [
                "opensearch",
                "sync-dmps",
                "dmps-index",
                "--mysql-config.mysql-host",
                "localhost",
                "--mysql-config.mysql-tcp-port",
                "3306",
                "--mysql-config.mysql-user",
                "root",
                "--mysql-config.mysql-database",
                "testdb",
                "--mysql-config.mysql-pwd",
                "password",
            ]
        )

        # We assert the structure of the mocked call, ignoring exact MySQLConfig object ID
        # but validating its contents.
        mock_sync_dmps.assert_called_once()
        kwargs = mock_sync_dmps.call_args.kwargs
        args = mock_sync_dmps.call_args.args

        assert args[0] == "dmps-index"
        assert isinstance(args[1], MySQLConfig)
        assert args[1].mysql_host == "localhost"
        assert kwargs["opensearch_config"] == OpenSearchClientConfig()
        assert kwargs["chunk_size"] == 1000

    @pytest.fixture
    def mock_enrich_dmps(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.enrich_dmps")

    def test_opensearch_enrich_dmps(self, mock_enrich_dmps):
        cli(["opensearch", "enrich-dmps", "dmps-index"])

        mock_enrich_dmps.assert_called_once_with(
            "dmps-index",
            OpenSearchClientConfig(),
        )

    @pytest.fixture
    def mock_dmp_works_search(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.dmp_works_search")

    def test_opensearch_dmp_works_search(self, mock_dmp_works_search, tmp_path: pathlib.Path):
        out_file = tmp_path / "results.json"

        cli(["opensearch", "dmp-works-search", "dmps-index", "works-index", str(out_file)])

        mock_dmp_works_search.assert_called_once_with(
            "dmps-index",
            "works-index",
            out_file,
            OpenSearchClientConfig(),
            query_builder_name="build_dmp_works_search_baseline_query",
            rerank_model_name=None,
            scroll_time="360m",
            batch_size=250,
            max_results=100,
            project_end_buffer_years=3,
            parallel_search=False,
            include_named_queries_score=True,
            max_concurrent_searches=125,
            max_concurrent_shard_requests=12,
            institutions=None,
            dois=None,
            start_date=None,
            end_date=None,
        )

    @pytest.fixture
    def mock_rank_metrics(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.related_works_calculate_metrics")

    def test_opensearch_rank_metrics(self, mock_rank_metrics, tmp_path: pathlib.Path):
        gt_file = tmp_path / "ground_truth.csv"
        gt_file.touch()  # Cyclopts validator requires exists=True
        out_file = tmp_path / "metrics.json"

        cli(["opensearch", "rank-metrics", str(gt_file), "dmps-index", "works-index", str(out_file)])

        mock_rank_metrics.assert_called_once_with(
            gt_file,
            "dmps-index",
            "works-index",
            out_file,
            OpenSearchClientConfig(),
            query_builder_name="build_dmp_works_search_baseline_query",
            rerank_model_name=None,
            scroll_time="360m",
            project_end_buffer_years=3,
            include_named_queries_score=True,
            inner_hits_size=50,
            batch_size=100,
            max_results=100,
            ks=None,
        )

    @pytest.fixture
    def mock_create_featureset(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.create_featureset")

    def test_opensearch_create_featureset(self, mock_create_featureset):
        cli(["opensearch", "create-featureset", "my-featureset"])

        mock_create_featureset.assert_called_once_with(
            InstanceOf(OpenSearch),
            "my-featureset",
        )

    @pytest.fixture
    def mock_upload_ranklib_model(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.upload_ranklib_model")

    def test_opensearch_upload_ranklib_model(self, mock_upload_ranklib_model, tmp_path: pathlib.Path):
        model_file = tmp_path / "model.txt"
        features_file = tmp_path / "features.txt"
        training_file = tmp_path / "training.txt"

        # Cyclopts validators require exists=True
        model_file.touch()
        features_file.touch()
        training_file.touch()

        cli(
            [
                "opensearch",
                "upload-ranklib-model",
                "my-featureset",
                "my-model",
                str(model_file),
                str(features_file),
                str(training_file),
            ]
        )

        mock_upload_ranklib_model.assert_called_once_with(
            InstanceOf(OpenSearch),
            "my-featureset",
            "my-model",
            model_file,
            features_file,
            training_file,
        )

    @pytest.fixture
    def mock_generate_training_dataset(self, mocker):
        return mocker.patch(f"{CLI_MODULE}.generate_training_dataset")

    def test_opensearch_generate_training_dataset(self, mock_generate_training_dataset, tmp_path: pathlib.Path):
        gt_file = tmp_path / "ground_truth.csv"
        gt_file.touch()  # Cyclopts validator requires exists=True
        out_file = tmp_path / "training_data.txt"

        cli(
            [
                "opensearch",
                "generate-training-dataset",
                str(gt_file),
                "dmps-index",
                "works-index",
                str(out_file),
                "my-featureset",
            ]
        )

        mock_generate_training_dataset.assert_called_once_with(
            gt_file,
            "dmps-index",
            "works-index",
            "my-featureset",
            out_file,
            OpenSearchClientConfig(),
            query_builder_name="build_dmp_works_search_baseline_query",
            scroll_time="360m",
            project_end_buffer_years=3,
            include_named_queries_score=True,
            inner_hits_size=50,
            batch_size=100,
            max_results=100,
        )
