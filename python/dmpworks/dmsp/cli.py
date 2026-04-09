import logging
import pathlib
from typing import Annotated

from cyclopts import App, Parameter, validators

from dmpworks.cli_utils import LogLevel, MergeRelatedWorksConfig, MySQLConfig
from dmpworks.opensearch.utils import OpenSearchClientConfig

app = App(name="dmsp", help="Utilities for the DMSP database.")
related_works_app = App(name="related-works", help="DMSP related works utilities.")
app.command(related_works_app)


@related_works_app.command(name="load-migration")
def load_migration_related_works(
    mysql_config: MySQLConfig,
    opensearch_config: OpenSearchClientConfig,
    batch_size: int = 1000,
    log_level: LogLevel = "INFO",
):
    """Load related works from the migration database.

    Args:
        mysql_config: MySQL connection configuration.
        opensearch_config: OpenSearch connection configuration.
        batch_size: Number of records to process in a batch.
        log_level: Logging level.
    """
    from dmpworks.dmsp.loader import load_related_works, make_connection
    from dmpworks.dmsp.migration import fetch_migration_related_works
    from dmpworks.opensearch.utils import make_opensearch_client

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)

    conn = make_connection(mysql_config)
    os_client = make_opensearch_client(opensearch_config)
    records = fetch_migration_related_works(conn)
    load_related_works(conn, os_client, records, batch_size)


@related_works_app.command(name="load-ground-truth")
def load_ground_truth_related_works(
    matches_path: Annotated[
        pathlib.Path, Parameter(validator=validators.Path(dir_okay=False, file_okay=True, exists=True))
    ],
    mysql_config: MySQLConfig,
    opensearch_config: OpenSearchClientConfig,
    batch_size: int = 1000,
    log_level: LogLevel = "INFO",
):
    """Load ground truth related works from a CSV file.

    Args:
        matches_path: Path to the CSV file containing matches.
        mysql_config: MySQL connection configuration.
        opensearch_config: OpenSearch connection configuration.
        batch_size: Number of records to process in a batch.
        log_level: Logging level.
    """
    from dmpworks.dmsp.ground_truth import read_related_works_csv
    from dmpworks.dmsp.loader import load_related_works, make_connection
    from dmpworks.opensearch.utils import make_opensearch_client

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)

    conn = make_connection(mysql_config)
    os_client = make_opensearch_client(opensearch_config)
    records = read_related_works_csv(matches_path)
    load_related_works(conn, os_client, records, batch_size)


@related_works_app.command(name="merge")
def merge_related_works_cmd(
    matches_path: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=True,
                file_okay=False,
                exists=True,
            )
        ),
    ],
    mysql_config: MySQLConfig,
    merge_config: MergeRelatedWorksConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Merge related works from .jsonl.gz match files into the database.

    Args:
        matches_path: Path to the directory containing .jsonl.gz match files.
        mysql_config: MySQL connection configuration.
        merge_config: Merge related works configuration.
        log_level: Logging level.
    """
    from dmpworks.dmsp.merge import merge_related_works

    merge_config = MergeRelatedWorksConfig() if merge_config is None else merge_config
    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)

    merge_related_works(
        matches_path,
        mysql_config=mysql_config,
        insert_batch_size=merge_config.insert_batch_size,
    )


if __name__ == "__main__":
    app()
