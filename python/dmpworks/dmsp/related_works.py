from collections.abc import Callable, Iterable
import csv
from dataclasses import dataclass
import json
import logging
import pathlib
import tempfile
from typing import Any

from opensearchpy import OpenSearch, exceptions
import pyarrow as pa

from dmpworks.model.work_model import WorkModel
from dmpworks.transform.simdjson_transforms import extract_doi
from dmpworks.utils import ParquetBatchWriter, read_parquet_files

log = logging.getLogger(__name__)

ALLOWED_TABLES = {"works", "workVersions", "relatedWorks", "stagingWorkVersions", "stagingRelatedWorks"}

WORK_VERSIONS_SCHEMA = pa.schema(
    [
        pa.field("doi", pa.string()),
        pa.field("hash", pa.string()),
        pa.field("workType", pa.string()),
        pa.field("publicationDate", pa.string()),
        pa.field("title", pa.string()),
        pa.field("abstractText", pa.string()),
        pa.field("authors", pa.string()),
        pa.field("institutions", pa.string()),
        pa.field("funders", pa.string()),
        pa.field("awards", pa.string()),
        pa.field("publicationVenue", pa.string()),
        pa.field("sourceName", pa.string()),
        pa.field("sourceUrl", pa.string()),
    ]
)

RELATED_WORKS_SCHEMA = pa.schema(
    [
        pa.field("planId", pa.string()),
        pa.field("dmpDoi", pa.string()),
        pa.field("workDoi", pa.string()),
        pa.field("hash", pa.string()),
        pa.field("sourceType", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("scoreMax", pa.float64()),
        pa.field("doiMatch", pa.string()),
        pa.field("contentMatch", pa.string()),
        pa.field("authorMatches", pa.string()),
        pa.field("institutionMatches", pa.string()),
        pa.field("funderMatches", pa.string()),
        pa.field("awardMatches", pa.string()),
    ]
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


def merge_related_works(matches_dir: pathlib.Path, conn, batch_size: int = 1000):
    """Merge related works data into the database.

    Args:
        matches_dir: Directory containing Parquet match data files.
        conn: Database connection object.
        batch_size: Number of records to process in a batch.
    """
    with RelatedWorksLoader(conn) as loader, tempfile.TemporaryDirectory() as tmp:
        # Create temporary directories with consolidated data
        tmp_dir = pathlib.Path(tmp)
        work_versions_dir = tmp_dir / "work_versions"
        related_works_dir = tmp_dir / "related_works"
        work_versions_dir.mkdir()
        related_works_dir.mkdir()
        create_work_versions(matches_dir, work_versions_dir)
        create_related_works(matches_dir, related_works_dir)

        # Load into MySQL and run update procedure
        loader.prepare_staging_tables()
        loader.insert_work_versions(
            (to_sql_work_version_row(row) for row in read_parquet_files([work_versions_dir])),
            batch_size=batch_size,
        )
        loader.insert_related_works(
            (to_sql_related_work_row({**row, "status": "PENDING"}) for row in read_parquet_files([related_works_dir])),
            batch_size=batch_size,
        )
        loader.run_update_procedure(system_matched=True)


def read_related_works_csv(file_path: pathlib.Path) -> list[RelatedWorkReference]:
    """Read related works from a CSV file.

    Args:
        file_path: Path to the CSV file.

    Returns:
        A list of RelatedWorkReference objects.
    """
    results = []
    with file_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=2):
            plan_id = row.get("plan_id", "").strip() or None
            dmp_id = row.get("dmp_id", "").strip() or None
            work_doi = row.get("work_doi", "").strip() or None

            if not work_doi:
                logging.warning(f"Row {i}: Skipped (Missing 'work_doi')")
                continue

            if not plan_id and not dmp_id:
                logging.warning(f"Row {i}: Skipped (Both 'plan_id' and 'dmp_id' are missing)")
                continue

            results.append(RelatedWorkReference(plan_id=plan_id, dmp_id=dmp_id, work_doi=work_doi))

    return results


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
                logging.error("Transaction rolled back due to error.")
            else:
                self.conn.commit()
                logging.info("Transaction committed successfully.")
            self.conn.close()

    def prepare_staging_tables(self):
        """Prepare staging tables in the database."""
        logging.info("Preparing staging tables...")
        with self.conn.cursor() as cursor:
            cursor.callproc("create_related_works_staging_tables")

    def insert_work_versions(self, rows_iterator: Iterable[list[Any]], batch_size: int = 1000):
        """Insert work versions into the staging table.

        Args:
            rows_iterator: Iterator yielding rows to insert.
            batch_size: Number of rows to insert per batch.
        """
        logging.info("Loading Work Versions...")
        sql = "INSERT INTO stagingWorkVersions (doi,hash,workType,publicationDate,title,abstractText,authors,institutions,funders,awards,publicationVenue,sourceName,sourceUrl) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self._batch_insert(sql, rows_iterator, batch_size)

        with self.conn.cursor() as cursor:
            self.print_table(cursor, "stagingWorkVersions", lambda r: f"doi={r.get('doi')}")

    def insert_related_works(self, rows_iterator: Iterable[list[Any]], batch_size: int = 1000):
        """Insert related works into the staging table.

        Args:
            rows_iterator: Iterator yielding rows to insert.
            batch_size: Number of rows to insert per batch.
        """
        logging.info("Loading Related Works...")
        sql = "INSERT INTO stagingRelatedWorks (planId,dmpDoi,workDoi,hash,sourceType,score,scoreMax,status,doiMatch,contentMatch,authorMatches,institutionMatches,funderMatches,awardMatches) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self._batch_insert(sql, rows_iterator, batch_size)

        with self.conn.cursor() as cursor:
            self.print_table(
                cursor, "stagingRelatedWorks", lambda r: f"dmpDoi={r.get('dmpDoi')}, status={r.get('status')}"
            )

    def _batch_insert(self, sql: str, rows: Iterable[list[Any]], batch_size: int):
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
        logging.info("Running update procedure...")
        with self.conn.cursor() as cursor:
            cursor.callproc("batch_update_related_works", [system_matched])
            self.print_table(cursor, "works", format_func=lambda r: f"id={r.get('id')}, doi={r.get('doi')}")
            self.print_table(cursor, "workVersions", format_func=lambda r: f"id={r.get('id')}, title={r.get('title')}")
            self.print_table(cursor, "relatedWorks", format_func=lambda r: f"id={r.get('id')}")

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

        logging.info(f"Table: {table_name}")
        # table_name is checked against ALLOWED_TABLES before adding to query
        cursor.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))  # noqa: S608
        results = cursor.fetchall()
        for row in results:
            logging.info(format_func(row))


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
        logging.exception(f"Work with doi='{doi}' was not found")
        return None


def fetch_migration_related_works(conn) -> list[RelatedWorkReference]:
    """Returns valid related works from migration table.

    Args:
        conn: Database connection object.

    Returns:
        A list of RelatedWorkReference objects.
    """
    sql = """
    SELECT DISTINCT p.id AS plan_id, TRIM(REPLACE(LOWER(p.dmpId), 'https://doi.org/', '')) AS dmp_id, LOWER(TRIM(rw.value)) AS work_doi
    FROM migration.related_works rw
    JOIN migration.plans p ON rw.plan_id = p.old_plan_id
    WHERE rw.is_valid = 1
    AND NOT EXISTS (
       SELECT 1 FROM dmptool.relatedWorks rw2
       JOIN dmptool.workVersions wv ON rw2.workVersionId = wv.id
       JOIN dmptool.works w ON wv.workId = w.id
       WHERE rw2.planId = p.id AND w.doi = rw.value
    )
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [
            RelatedWorkReference(plan_id=row["plan_id"], dmp_id=row["dmp_id"], work_doi=row["work_doi"])
            for row in cursor.fetchall()
        ]


def load_related_works(conn, os_client: OpenSearch, records: list[RelatedWorkReference], batch_size: int = 1000):
    """Load related works into the database.

    Args:
        conn: Database connection object.
        os_client: OpenSearch client.
        records: List of RelatedWorkReference objects to load.
        batch_size: Number of records to process in a batch.
    """
    logging.info(f"Processing {len(records)} migration records...")

    seen = set()
    work_versions = []
    related_works = []

    for record in records:
        # Extract the DOI in case of dirty data
        work_doi = extract_doi(record.work_doi)
        work = fetch_opensearch_work(os_client, work_doi)
        if not work:
            logging.warning(
                f"Skipping plan_id={record.plan_id}, dmp_id={record.dmp_id}, work_doi={work_doi} as work could not be found in OpenSearch"
            )
            continue

        # Add work version
        if work_doi not in seen:
            work_dict = work.model_dump(by_alias=True)
            work_version_dict = json_work_to_work_version(work_dict)
            work_versions.append(to_sql_work_version_row(work_version_dict))
            seen.add(work_doi)

        # Add related work
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

    logging.info(f"Loading {len(work_versions)}")
    logging.info(f"Loading {len(related_works)}")

    with RelatedWorksLoader(conn) as loader:
        loader.prepare_staging_tables()
        loader.insert_work_versions(work_versions, batch_size=batch_size)
        loader.insert_related_works(related_works, batch_size=batch_size)
        loader.run_update_procedure(system_matched=False)


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


def create_work_versions(
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    *,
    row_group_size: int = 50_000,
    row_groups_per_file: int = 4,
):
    """Extract unique work versions from input Parquet directory and write to output directory.

    Args:
        input_dir: Directory containing input Parquet match data files.
        output_dir: Directory to write work version Parquet files into.
        row_group_size: Number of rows per Parquet row group.
        row_groups_per_file: Number of row groups per output file before rotating.
    """
    seen = set()
    with ParquetBatchWriter(
        output_dir=output_dir,
        schema=WORK_VERSIONS_SCHEMA,
        row_group_size=row_group_size,
        row_groups_per_file=row_groups_per_file,
    ) as writer:
        for row in read_parquet_files([input_dir]):
            work = json.loads(row["work"])
            doi = work["doi"]
            if doi not in seen:
                writer.write_rows([json_work_to_work_version(work)])
                seen.add(doi)


def json_work_to_work_version(work: dict) -> dict:
    """Convert a JSON work dictionary to a work version dictionary.

    Args:
        work: Dictionary containing work data.

    Returns:
        A dictionary representing the work version.
    """
    return {
        "doi": work["doi"],
        "hash": work["hash"],
        "workType": work["workType"],
        "publicationDate": work["publicationDate"],
        "title": work["title"],
        "abstractText": work["abstractText"],
        "authors": serialise_json(work["authors"]),
        "institutions": serialise_json(work["institutions"]),
        "funders": serialise_json(work["funders"]),
        "awards": serialise_json(work["awards"]),
        "publicationVenue": work["publicationVenue"],
        "sourceName": work["source"]["name"],
        "sourceUrl": work["source"]["url"],
    }


def create_related_works(
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    *,
    row_group_size: int = 50_000,
    row_groups_per_file: int = 4,
):
    """Extract related works from input Parquet directory and write to output directory.

    Args:
        input_dir: Directory containing input Parquet match data files.
        output_dir: Directory to write related work Parquet files into.
        row_group_size: Number of rows per Parquet row group.
        row_groups_per_file: Number of row groups per output file before rotating.
    """
    with ParquetBatchWriter(
        output_dir=output_dir,
        schema=RELATED_WORKS_SCHEMA,
        row_group_size=row_group_size,
        row_groups_per_file=row_groups_per_file,
    ) as writer:
        for row in read_parquet_files([input_dir]):
            work = json.loads(row["work"])
            writer.write_rows(
                [
                    {
                        "planId": None,
                        "dmpDoi": row["dmpDoi"],
                        "workDoi": work["doi"],
                        "hash": work["hash"],
                        "sourceType": "SYSTEM_MATCHED",
                        "score": row["score"],
                        "scoreMax": row["scoreMax"],
                        "doiMatch": row["doiMatch"],
                        "contentMatch": row["contentMatch"],
                        "authorMatches": row["authorMatches"],
                        "institutionMatches": row["institutionMatches"],
                        "funderMatches": row["funderMatches"],
                        "awardMatches": row["awardMatches"],
                    }
                ]
            )


def serialise_json(data) -> str:
    """Serialize data to a JSON string.

    Args:
        data: The data to serialize.

    Returns:
        A JSON string representation of the data.
    """
    if isinstance(data, str):
        return data
    return json.dumps(data, sort_keys=True, separators=(",", ":"))
