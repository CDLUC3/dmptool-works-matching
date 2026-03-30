from collections import defaultdict
import json
import logging
import pathlib

import pyarrow.dataset as ds
from tqdm import tqdm

from dmpworks.dmsp.loader import RelatedWorksLoader, to_sql_related_work_row, to_sql_work_version_row
from dmpworks.dmsp.related_works import json_work_to_work_version

log = logging.getLogger(__name__)


def merge_related_works(matches_dir: pathlib.Path, conn, batch_size: int = 1000):
    """Merge related works data into the database, processing one DMP at a time.

    Reads match data Parquet files once, groups rows by DMP DOI, then processes
    each DMP's work versions and related works.

    Args:
        matches_dir: Directory containing Parquet match data files.
        conn: Database connection object.
        batch_size: Number of records to process in a batch.
    """
    dataset = ds.dataset(matches_dir, format="parquet")
    rows_by_dmp = defaultdict(list)
    for batch in dataset.to_batches():
        for row in batch.to_pylist():
            rows_by_dmp[row["dmpDoi"]].append(row)

    log.info(f"Processing related works for {len(rows_by_dmp)} DMPs...")

    with RelatedWorksLoader(conn) as loader:
        for dmp_doi, rows in tqdm(rows_by_dmp.items(), desc="Merging related works", unit="dmp"):
            log.debug(f"Processing DMP {dmp_doi} with {len(rows)} related works")

            seen_dois = set()
            work_version_rows = []
            related_work_rows = []

            for row in rows:
                work = json.loads(row["work"])
                work_doi = work["doi"]

                if work_doi not in seen_dois:
                    work_version_rows.append(to_sql_work_version_row(json_work_to_work_version(work)))
                    seen_dois.add(work_doi)

                related_work_rows.append(
                    to_sql_related_work_row(
                        {
                            "dmpDoi": row["dmpDoi"],
                            "workDoi": work_doi,
                            "hash": work["hash"],
                            "sourceType": "SYSTEM_MATCHED",
                            "status": "PENDING",
                            "score": row["score"],
                            "scoreMax": row["scoreMax"],
                            "doiMatch": row["doiMatch"],
                            "contentMatch": row["contentMatch"],
                            "authorMatches": row["authorMatches"],
                            "institutionMatches": row["institutionMatches"],
                            "funderMatches": row["funderMatches"],
                            "awardMatches": row["awardMatches"],
                        }
                    )
                )

            loader.prepare_staging_tables()
            loader.insert_work_versions(work_version_rows, batch_size=batch_size)
            loader.insert_related_works(related_work_rows, batch_size=batch_size)
            loader.run_update_procedure(system_matched=True)

        log.info("Cleaning up orphaned works...")
        loader.run_cleanup_procedure()
