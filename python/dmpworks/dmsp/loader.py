from collections.abc import Callable, Iterable
from dataclasses import dataclass
import logging
from typing import Any

from opensearchpy import OpenSearch, exceptions

from dmpworks.dmsp.related_works import json_work_to_work_version
from dmpworks.dmsp.utils import serialise_json
from dmpworks.model.work_model import WorkModel
from dmpworks.transform.simdjson_transforms import extract_doi

log = logging.getLogger(__name__)

ALLOWED_TABLES = {"works", "workVersions", "relatedWorks", "stagingWorkVersions", "stagingRelatedWorks"}


@dataclass
class RelatedWorkReference:
    """Represents a reference to a related work.

    Attributes:
        plan_id: The ID of the plan.
        dmp_id: The ID of the DMP.
        work_doi: The DOI of the related work.
    """

    plan_id: str | None
    dmp_id: str | None
    work_doi: str


def to_sql_work_version_row(row: dict) -> list:
    """Convert a work version dictionary to a list suitable for SQL insertion.

    Args:
        row: Dictionary containing work version data.

    Returns:
        A list of values.
    """
    row_hash = row["hash"]
    if isinstance(row_hash, str):
        row_hash = bytes.fromhex(row_hash)

    return [
        row["doi"],
        row_hash,
        row.get("workType"),
        row.get("publicationDate"),
        row.get("title"),
        row.get("abstractText"),
        serialise_json(row.get("authors")),
        serialise_json(row.get("institutions")),
        serialise_json(row.get("funders")),
        serialise_json(row.get("awards")),
        row.get("publicationVenue"),
        row.get("sourceName"),
        row.get("sourceUrl"),
    ]


def to_sql_related_work_row(row: dict) -> list:
    """Convert a related work dictionary to a list suitable for SQL insertion.

    Args:
        row: Dictionary containing related work data.

    Returns:
        A list of values.
    """
    row_hash = row["hash"]
    if isinstance(row_hash, str):
        row_hash = bytes.fromhex(row_hash)

    return [
        row.get("planId"),
        row["dmpDoi"],
        row["workDoi"],
        row_hash,
        row["sourceType"],
        row.get("score", 1),
        row.get("scoreMax", 1),
        row["status"],
        serialise_json(row.get("doiMatch", {"found": False, "score": 0.0, "sources": []})),
        serialise_json(row.get("contentMatch", {"score": 0.0, "titleHighlight": None, "abstractHighlights": []})),
        serialise_json(row.get("authorMatches", [])),
        serialise_json(row.get("institutionMatches", [])),
        serialise_json(row.get("funderMatches", [])),
        serialise_json(row.get("awardMatches", [])),
    ]


def fetch_opensearch_work(client: OpenSearch, doi: str, works_index: str = "works-index") -> WorkModel | None:
    """Fetch a work from OpenSearch by DOI.

    Args:
        client: OpenSearch client.
        doi: The DOI of the work to fetch.
        works_index: The name of the OpenSearch index.

    Returns:
        A WorkModel object if found, otherwise None.
    """
    try:
        response = client.get(index=works_index, id=doi)
        return WorkModel.model_validate(response.get("_source", {}), by_name=True, by_alias=False)
    except exceptions.NotFoundError:
        log.exception(f"Work with doi='{doi}' was not found")
        return None


def load_related_works(conn, os_client: OpenSearch, records: list[RelatedWorkReference], batch_size: int = 1000):
    """Load related works into the database.

    Args:
        conn: Database connection object.
        os_client: OpenSearch client.
        records: List of RelatedWorkReference objects to load.
        batch_size: Number of records to process in a batch.
    """
    log.info(f"Processing {len(records)} records...")

    seen = set()
    work_versions = []
    related_works = []

    for record in records:
        work_doi = extract_doi(record.work_doi)
        work = fetch_opensearch_work(os_client, work_doi)
        if not work:
            log.warning(
                f"Skipping plan_id={record.plan_id}, dmp_id={record.dmp_id}, work_doi={work_doi} as work could not be found in OpenSearch"
            )
            continue

        if work_doi not in seen:
            work_dict = work.model_dump(by_alias=True)
            work_version_dict = json_work_to_work_version(work_dict)
            work_versions.append(to_sql_work_version_row(work_version_dict))
            seen.add(work_doi)

        related_work = {
            "planId": record.plan_id,
            "dmpDoi": record.dmp_id,
            "workDoi": work.doi,
            "hash": work.hash,
            "sourceType": "USER_ADDED",
            "status": "ACCEPTED",
            "score": 1,
            "scoreMax": 1,
        }
        related_works.append(to_sql_related_work_row(related_work))

    log.info(f"Loading {len(work_versions)} work versions and {len(related_works)} related works...")

    with RelatedWorksLoader(conn) as loader:
        loader.prepare_staging_tables()
        loader.insert_work_versions(work_versions, batch_size=batch_size)
        loader.insert_related_works(related_works, batch_size=batch_size)
        loader.run_update_procedure(system_matched=False)


class RelatedWorksLoader:
    """Helper class for loading related works into the database.

    Attributes:
        conn: Database connection object.
    """

    def __init__(self, conn):
        """Initialize the RelatedWorksLoader.

        Args:
            conn: Database connection object.
        """
        self.conn = conn

    def __enter__(self):
        """Enter the context manager.

        Returns:
            self: The RelatedWorksLoader instance.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager.

        Args:
            exc_type: Exception type.
            exc_val: Exception value.
            exc_tb: Exception traceback.
        """
        if self.conn:
            if exc_type:
                self.conn.rollback()
                log.error("Transaction rolled back due to error.")
            else:
                self.conn.commit()
                log.info("Transaction committed successfully.")
            self.conn.close()

    def prepare_staging_tables(self):
        """Prepare staging tables in the database."""
        log.info("Preparing staging tables...")
        with self.conn.cursor() as cursor:
            cursor.callproc("create_related_works_staging_tables")

    def insert_work_versions(self, rows_iterator: Iterable[list[Any]], batch_size: int = 1000):
        """Insert work versions into the staging table.

        Args:
            rows_iterator: Iterator yielding rows to insert.
            batch_size: Number of rows to insert per batch.
        """
        log.info("Loading work versions into staging table...")
        sql = "INSERT INTO stagingWorkVersions (doi,hash,workType,publicationDate,title,abstractText,authors,institutions,funders,awards,publicationVenue,sourceName,sourceUrl) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.batch_insert(sql, rows_iterator, batch_size)

        with self.conn.cursor() as cursor:
            self.print_table(cursor, "stagingWorkVersions", lambda r: f"doi={r.get('doi')}")

    def insert_related_works(self, rows_iterator: Iterable[list[Any]], batch_size: int = 1000):
        """Insert related works into the staging table.

        Args:
            rows_iterator: Iterator yielding rows to insert.
            batch_size: Number of rows to insert per batch.
        """
        log.info("Loading related works into staging table...")
        sql = "INSERT INTO stagingRelatedWorks (planId,dmpDoi,workDoi,hash,sourceType,score,scoreMax,status,doiMatch,contentMatch,authorMatches,institutionMatches,funderMatches,awardMatches) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.batch_insert(sql, rows_iterator, batch_size)

        with self.conn.cursor() as cursor:
            self.print_table(
                cursor, "stagingRelatedWorks", lambda r: f"dmpDoi={r.get('dmpDoi')}, status={r.get('status')}"
            )

    def batch_insert(self, sql: str, rows: Iterable[list[Any]], batch_size: int):
        """Execute batch insert.

        Args:
            sql: SQL insert statement.
            rows: Iterator yielding rows to insert.
            batch_size: Number of rows to insert per batch.
        """
        batch = []
        with self.conn.cursor() as cursor:
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    cursor.executemany(sql, batch)
                    batch = []
            if batch:
                cursor.executemany(sql, batch)

    def run_update_procedure(self, system_matched=True):
        """Run the stored procedure to update related works.

        Args:
            system_matched: Boolean indicating if the matches are system generated.
        """
        with self.conn.cursor() as cursor:
            for table in ("stagingWorkVersions", "stagingRelatedWorks"):
                cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table}")  # noqa: S608
                row = cursor.fetchone()
                log.info("  %s: %d rows staged", table, row["cnt"])

        log.info("Calling batch_update_related_works stored procedure...")
        with self.conn.cursor() as cursor:
            cursor.callproc("batch_update_related_works", [system_matched])
            self.print_table(cursor, "works", format_func=lambda r: f"id={r.get('id')}, doi={r.get('doi')}")
            self.print_table(cursor, "workVersions", format_func=lambda r: f"id={r.get('id')}, title={r.get('title')}")
            self.print_table(cursor, "relatedWorks", format_func=lambda r: f"id={r.get('id')}")
        log.info("Stored procedure completed.")

    def print_table(self, cursor, table_name: str, format_func: Callable, limit: int = 10):
        """Print rows from a table for debugging.

        Args:
            cursor: Database cursor.
            table_name: Name of the table to print.
            format_func: Function to format each row.
            limit: Maximum number of rows to print.

        Raises:
            ValueError: If table_name is not in the allowed list.
        """
        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"Invalid table name: {table_name!r}. Must be one of: {sorted(ALLOWED_TABLES)}")

        log.info("Table: %s", table_name)
        # table_name is checked against ALLOWED_TABLES before adding to query
        cursor.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))  # noqa: S608
        results = cursor.fetchall()
        for row in results:
            log.info(format_func(row))
