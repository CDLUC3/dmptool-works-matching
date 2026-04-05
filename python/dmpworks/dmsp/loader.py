from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import TYPE_CHECKING, Any

from opensearchpy import OpenSearch, exceptions
import pymysql
import pymysql.cursors

from dmpworks.dmsp.related_works import json_work_to_work_version
from dmpworks.dmsp.utils import serialise_json
from dmpworks.model.work_model import WorkModel
from dmpworks.transform.simdjson_transforms import extract_doi

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from dmpworks.cli_utils import MySQLConfig

log = logging.getLogger(__name__)

ALLOWED_TABLES = {"works", "workVersions", "relatedWorks", "stagingWorkVersions", "stagingRelatedWorks"}

PLAN_ID_MAPPING_SQL = """
SELECT id, dmpId FROM (
    SELECT id, dmpId,
      ROW_NUMBER() OVER (PARTITION BY dmpId ORDER BY created DESC, id DESC) AS rn
    FROM plans
    WHERE dmpId IS NOT NULL
) ranked
WHERE rn = 1
"""


def fetch_plan_id_mapping(conn) -> dict[str, int]:
    """Fetch a mapping of bare DOI to planId from the plans table.

    Keys are normalized bare DOIs (e.g. '10.xxxxx/yyyy') using extract_doi,
    matching the format used in match JSONL files and RelatedWorkReference.dmp_id.

    When multiple plans share a dmpId, picks the most recently created one
    (same dedup logic as sync_dmps).

    Args:
        conn: Database connection object.

    Returns:
        Dict mapping bare DOI to plan ID (int).
    """
    with conn.cursor() as cursor:
        cursor.execute(PLAN_ID_MAPPING_SQL)
        result = {}
        for row in cursor.fetchall():
            doi = extract_doi(row["dmpId"])
            if doi is not None:
                result[doi] = row["id"]
        return result


def make_connection(mysql_config: MySQLConfig):
    """Create a new pymysql connection from a MySQLConfig.

    Args:
        mysql_config: MySQL connection configuration.

    Returns:
        A pymysql connection.
    """
    return pymysql.connect(
        host=mysql_config.mysql_host,
        port=mysql_config.mysql_tcp_port,
        user=mysql_config.mysql_user,
        password=mysql_config.mysql_pwd,
        database=mysql_config.mysql_database,
        cursorclass=pymysql.cursors.DictCursor,
    )


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
        row["planId"],
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

    # Prefetch plan ID mapping for records that only have dmp_id
    plan_id_map = None
    if any(r.plan_id is None for r in records):
        plan_id_map = fetch_plan_id_mapping(conn)

    seen = set()
    work_versions = []
    related_works = []

    for record in records:
        # Resolve plan_id
        if record.plan_id is not None:
            plan_id = int(record.plan_id)
        elif plan_id_map is not None:
            plan_id = plan_id_map.get(extract_doi(record.dmp_id))
            if plan_id is None:
                log.warning(f"No plan found for dmp_id={record.dmp_id}, work_doi={record.work_doi}, skipping")
                continue
        else:
            log.warning(f"No plan_id and no mapping for dmp_id={record.dmp_id}, work_doi={record.work_doi}, skipping")
            continue

        work_doi = extract_doi(record.work_doi)
        work = fetch_opensearch_work(os_client, work_doi)
        if not work:
            log.warning(
                f"Skipping plan_id={plan_id}, dmp_id={record.dmp_id}, work_doi={work_doi} as work could not be found in OpenSearch"
            )
            continue

        if work_doi not in seen:
            work_dict = work.model_dump(by_alias=True)
            work_version_dict = json_work_to_work_version(work_dict)
            work_versions.append(to_sql_work_version_row(work_version_dict))
            seen.add(work_doi)

        related_work = {
            "planId": plan_id,
            "workDoi": work.doi,
            "hash": work.hash,
            "sourceType": "USER_ADDED",
            "status": "ACCEPTED",
            "score": 1,
            "scoreMax": 1,
        }
        related_works.append(to_sql_related_work_row(related_work))

    log.info(f"Loading {len(work_versions)} work versions and {len(related_works)} related works...")

    loader = RelatedWorksLoader(conn)
    try:
        loader.prepare_staging_tables()
        loader.insert_work_versions(work_versions, batch_size=batch_size)
        loader.insert_related_works(related_works, batch_size=batch_size)
        loader.run_update_procedure(system_matched=False)
        conn.commit()
        log.info("Transaction committed successfully.")
    except Exception:
        conn.rollback()
        log.exception("Transaction rolled back due to error.")
        raise
    finally:
        conn.close()


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

    def commit(self):
        """Commit the current transaction.

        A new transaction begins implicitly after this call (pymysql autocommit=False).
        """
        self.conn.commit()

    def prepare_staging_tables(self):
        """Prepare staging tables in the database."""
        log.debug("Preparing staging tables...")
        with self.conn.cursor() as cursor:
            cursor.callproc("create_related_works_staging_tables")

    def insert_work_versions(self, rows_iterator: Iterable[list[Any]], batch_size: int = 1000):
        """Insert work versions into the staging table.

        Args:
            rows_iterator: Iterator yielding rows to insert.
            batch_size: Number of rows to insert per batch.
        """
        log.debug("Loading work versions into staging table...")
        sql = "INSERT IGNORE INTO stagingWorkVersions (doi,hash,workType,publicationDate,title,abstractText,authors,institutions,funders,awards,publicationVenue,sourceName,sourceUrl) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.batch_insert(sql, rows_iterator, batch_size)

        if log.isEnabledFor(logging.DEBUG):
            with self.conn.cursor() as cursor:
                self.print_table(cursor, "stagingWorkVersions", lambda r: f"doi={r.get('doi')}")

    def insert_related_works(self, rows_iterator: Iterable[list[Any]], batch_size: int = 1000):
        """Insert related works into the staging table.

        Args:
            rows_iterator: Iterator yielding rows to insert.
            batch_size: Number of rows to insert per batch.
        """
        log.debug("Loading related works into staging table...")
        sql = "INSERT IGNORE INTO stagingRelatedWorks (planId,workDoi,hash,sourceType,score,scoreMax,status,doiMatch,contentMatch,authorMatches,institutionMatches,funderMatches,awardMatches) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.batch_insert(sql, rows_iterator, batch_size)

        if log.isEnabledFor(logging.DEBUG):
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
        if log.isEnabledFor(logging.DEBUG):
            with self.conn.cursor() as cursor:
                for table in ("stagingWorkVersions", "stagingRelatedWorks"):
                    cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table}")  # noqa: S608
                    row = cursor.fetchone()
                    log.debug(f"  {table}: {row['cnt']} rows staged")

        log.debug("Calling batch_update_related_works stored procedure...")
        with self.conn.cursor() as cursor:
            cursor.callproc("batch_update_related_works", [system_matched])

            if log.isEnabledFor(logging.DEBUG):
                self.print_table(cursor, "works", format_func=lambda r: f"id={r.get('id')}, doi={r.get('doi')}")
                self.print_table(
                    cursor, "workVersions", format_func=lambda r: f"id={r.get('id')}, title={r.get('title')}"
                )
                self.print_table(cursor, "relatedWorks", format_func=lambda r: f"id={r.get('id')}")
        log.debug("Stored procedure completed.")

    def run_cleanup_procedure(self):
        """Run the stored procedure to clean up orphaned workVersions and works."""
        log.info("Calling cleanup_orphan_works stored procedure...")
        with self.conn.cursor() as cursor:
            cursor.callproc("cleanup_orphan_works")
        log.info("Cleanup procedure completed.")

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

        log.debug(f"Table: {table_name}")
        # table_name is checked against ALLOWED_TABLES before adding to query
        cursor.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))  # noqa: S608
        results = cursor.fetchall()
        for row in results:
            log.debug(format_func(row))
