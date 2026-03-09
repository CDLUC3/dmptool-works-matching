import logging
import pathlib
import tempfile

from dmpworks.dmsp.loader import RelatedWorksLoader, to_sql_related_work_row, to_sql_work_version_row
from dmpworks.dmsp.related_works import create_related_works, create_work_versions
from dmpworks.utils import read_parquet_files

log = logging.getLogger(__name__)


def merge_related_works(matches_dir: pathlib.Path, conn, batch_size: int = 1000):
    """Merge related works data into the database.

    Args:
        matches_dir: Directory containing Parquet match data files.
        conn: Database connection object.
        batch_size: Number of records to process in a batch.
    """
    with RelatedWorksLoader(conn) as loader, tempfile.TemporaryDirectory() as tmp:
        tmp_dir = pathlib.Path(tmp)
        work_versions_dir = tmp_dir / "work_versions"
        related_works_dir = tmp_dir / "related_works"
        work_versions_dir.mkdir()
        related_works_dir.mkdir()

        log.info("Step 1/4: Extracting unique work versions from matches...")
        create_work_versions(matches_dir, work_versions_dir)

        log.info("Step 2/4: Extracting related works from matches...")
        create_related_works(matches_dir, related_works_dir)

        log.info("Step 3/4: Loading into staging tables...")
        loader.prepare_staging_tables()
        loader.insert_work_versions(
            (to_sql_work_version_row(row) for row in read_parquet_files([work_versions_dir])),
            batch_size=batch_size,
        )
        loader.insert_related_works(
            (to_sql_related_work_row({**row, "status": "PENDING"}) for row in read_parquet_files([related_works_dir])),
            batch_size=batch_size,
        )

        log.info("Step 4/4: Running database update procedure...")
        loader.run_update_procedure(system_matched=True)
