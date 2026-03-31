from collections import defaultdict
import contextlib
import json
import logging
import pathlib
import statistics
import time

import pyarrow.dataset as ds
from tqdm import tqdm

from dmpworks.dmsp.loader import RelatedWorksLoader, to_sql_related_work_row, to_sql_work_version_row
from dmpworks.dmsp.related_works import json_work_to_work_version

log = logging.getLogger(__name__)

STEP_NAMES = ("stage-tables", "work-versions", "related-works", "update-proc", "cleanup")


def time_step(durations: dict[str, list[float]], name: str):
    """Context manager that records elapsed time for a named step.

    Args:
        durations: Dictionary mapping step names to lists of recorded durations.
        name: The name of the step being timed.

    Yields:
        None
    """
    @contextlib.contextmanager
    def _timer():
        t0 = time.perf_counter()
        yield
        durations[name].append(time.perf_counter() - t0)

    return _timer()


def log_timing_summary(durations: dict[str, list[float]]):
    """Log mean and standard deviation for each timed step.

    Args:
        durations: Dictionary mapping step names to lists of recorded durations.
    """
    for name in STEP_NAMES:
        times = durations[name]
        if not times:
            continue
        mean = statistics.mean(times)
        stdev = statistics.stdev(times) if len(times) > 1 else 0.0
        log.info(f"  {name}: mean={mean:.3f}s, stdev={stdev:.3f}s ({len(times)} samples)")


def merge_related_works(matches_dir: pathlib.Path, conn, batch_size: int = 1000):
    """Merge related works data into the database, processing one DMP at a time.

    Reads match data Parquet files once, groups rows by DMP DOI, then processes
    each DMP's work versions and related works. Commits after each DMP so that
    progress is preserved if the job fails partway through.

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

    durations: dict[str, list[float]] = {name: [] for name in STEP_NAMES}

    with RelatedWorksLoader(conn) as loader:
        progress = tqdm(rows_by_dmp.items(), desc="Merging related works", unit="dmp")
        for dmp_doi, rows in progress:
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

            with time_step(durations, "stage-tables"):
                loader.prepare_staging_tables()
            with time_step(durations, "work-versions"):
                loader.insert_work_versions(work_version_rows, batch_size=batch_size)
            with time_step(durations, "related-works"):
                loader.insert_related_works(related_work_rows, batch_size=batch_size)
            with time_step(durations, "update-proc"):
                loader.run_update_procedure(system_matched=True)

            loader.commit()

            postfix_steps = ("stage-tables", "work-versions", "related-works", "update-proc")
            progress.set_postfix(
                {name: f"{statistics.mean(durations[name]):.3f}s" for name in postfix_steps if durations[name]},
                refresh=False,
            )

        log.info("Cleaning up orphaned works...")
        with time_step(durations, "cleanup"):
            loader.run_cleanup_procedure()

    log.info("Step timing summary:")
    log_timing_summary(durations)
