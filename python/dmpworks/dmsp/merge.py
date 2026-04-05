from __future__ import annotations

import contextlib
import logging
import math
import pathlib  # noqa: TC003
import time
from typing import TYPE_CHECKING

import pymysql.err
from tqdm import tqdm

from dmpworks.dmsp.loader import (
    RelatedWorksLoader,
    fetch_plan_id_mapping,
    make_connection,
    to_sql_related_work_row,
    to_sql_work_version_row,
)
from dmpworks.dmsp.related_works import json_work_to_work_version
from dmpworks.utils import yield_objects_from_jsonl

if TYPE_CHECKING:
    from collections.abc import Generator

    from dmpworks.cli_utils import MySQLConfig

log = logging.getLogger(__name__)

STEP_NAMES = ("read", "stage-tables", "work-versions", "related-works", "update-proc", "cleanup")
DEADLOCK_ERRNO = 1213
DEADLOCK_MAX_RETRIES = 3


class RunningStats:
    """Online mean and standard deviation using Welford's algorithm."""

    def __init__(self):
        """Initialize with zero observations."""
        self._count = 0
        self._mean = 0.0
        self._m2 = 0.0

    def update(self, value: float):
        """Record a new value."""
        self._count += 1
        delta = value - self._mean
        self._mean += delta / self._count
        delta2 = value - self._mean
        self._m2 += delta * delta2

    @property
    def count(self) -> int:
        """Number of values recorded."""
        return self._count

    @property
    def mean(self) -> float:
        """Running mean, or NaN if no values recorded."""
        return self._mean if self._count > 0 else math.nan

    @property
    def stdev(self) -> float:
        """Sample standard deviation, or 0.0 if fewer than 2 values."""
        if self._count < 2:  # noqa: PLR2004
            return 0.0
        return math.sqrt(self._m2 / (self._count - 1))


def time_step(durations: dict[str, RunningStats], name: str):
    """Context manager that records elapsed time for a named step.

    Args:
        durations: Dictionary mapping step names to RunningStats accumulators.
        name: The name of the step being timed.

    Yields:
        None
    """

    @contextlib.contextmanager
    def _timer():
        t0 = time.perf_counter()
        yield
        durations[name].update(time.perf_counter() - t0)

    return _timer()


def log_timing_summary(durations: dict[str, RunningStats]):
    """Log mean and standard deviation for each timed step.

    Args:
        durations: Dictionary mapping step names to RunningStats accumulators.
    """
    for name in STEP_NAMES:
        stats = durations[name]
        if stats.count == 0:
            continue
        log.info(f"  {name}: mean={stats.mean:.3f}s, stdev={stats.stdev:.3f}s ({stats.count} samples)")


def read_match_records(
    matches_dir: pathlib.Path,
) -> Generator[tuple[str, list[dict]], None, None]:
    """Yield (dmp_doi, works) tuples from gzip-compressed JSONL match files.

    Uses pysimdjson for fast parsing via ``yield_objects_from_jsonl``. Each record
    is eagerly converted to a plain Python dict so the simdjson parser buffer can
    be reused for the next line.

    Args:
        matches_dir: Directory containing .jsonl.gz match data files.

    Yields:
        Tuples of (dmp_doi, works) where works is a list of publication dicts.
    """
    for path in sorted(matches_dir.glob("*.jsonl.gz")):
        for record in yield_objects_from_jsonl(path):
            d = record.as_dict()
            record = None  # noqa: PLW2901 — release simdjson reference before next parse
            yield d["dmpDoi"], d["works"]


def merge_related_works(
    matches_dir: pathlib.Path,
    *,
    mysql_config: MySQLConfig,
    insert_batch_size: int = 1000,
):
    """Merge related works data into the database, processing one DMP at a time.

    Reads match data from gzip-compressed JSONL files, then processes each DMP's
    work versions and related works sequentially on a single database connection.
    Deadlocks (errno 1213) are retried with exponential backoff.

    Args:
        matches_dir: Directory containing .jsonl.gz match data files.
        mysql_config: MySQL connection configuration.
        insert_batch_size: Number of rows per SQL INSERT batch.
    """
    durations: dict[str, RunningStats] = {name: RunningStats() for name in STEP_NAMES}

    # Prefetch plan ID mapping
    conn = make_connection(mysql_config)
    try:
        plan_id_map = fetch_plan_id_mapping(conn)
    finally:
        conn.close()
    log.info(f"Loaded {len(plan_id_map)} plan ID mappings")

    progress = tqdm(desc="Merging related works", unit="dmp")
    processed = 0
    skipped = 0
    errors = 0

    conn = make_connection(mysql_config)
    try:
        loader = RelatedWorksLoader(conn)

        t0 = time.perf_counter()
        for dmp_doi, works in read_match_records(matches_dir):
            durations["read"].update(time.perf_counter() - t0)
            plan_id = plan_id_map.get(dmp_doi)
            if plan_id is None:
                log.warning(f"No plan found for DMP DOI {dmp_doi}, skipping {len(works)} works")
                skipped += 1
                progress.update(1)
                update_postfix(progress, durations, processed=processed, skipped=skipped, errors=errors)
                continue

            log.debug(f"Processing DMP {dmp_doi} with {len(works)} related works")

            seen_dois = set()
            work_version_rows = []
            related_work_rows = []

            for pub in works:
                work = pub["work"]
                work_doi = work["doi"]

                if work_doi not in seen_dois:
                    work_version_rows.append(to_sql_work_version_row(json_work_to_work_version(work)))
                    seen_dois.add(work_doi)

                related_work_rows.append(
                    to_sql_related_work_row(
                        {
                            "planId": plan_id,
                            "workDoi": work_doi,
                            "hash": work["hash"],
                            "sourceType": "SYSTEM_MATCHED",
                            "status": "PENDING",
                            "score": pub["score"],
                            "scoreMax": pub["scoreMax"],
                            "doiMatch": pub["doiMatch"],
                            "contentMatch": pub["contentMatch"],
                            "authorMatches": pub["authorMatches"],
                            "institutionMatches": pub["institutionMatches"],
                            "funderMatches": pub["funderMatches"],
                            "awardMatches": pub["awardMatches"],
                        }
                    )
                )

            success = False
            for attempt in range(DEADLOCK_MAX_RETRIES + 1):
                try:
                    with time_step(durations, "stage-tables"):
                        loader.prepare_staging_tables()
                    with time_step(durations, "work-versions"):
                        loader.insert_work_versions(work_version_rows, batch_size=insert_batch_size)
                    with time_step(durations, "related-works"):
                        loader.insert_related_works(related_work_rows, batch_size=insert_batch_size)
                    with time_step(durations, "update-proc"):
                        loader.run_update_procedure(system_matched=True)
                    loader.commit()
                    success = True
                    break
                except pymysql.err.OperationalError as e:
                    conn.rollback()
                    if e.args[0] == DEADLOCK_ERRNO and attempt < DEADLOCK_MAX_RETRIES:
                        delay = 1.0 * (2**attempt)
                        log.warning(
                            f"Deadlock on DMP {dmp_doi}, retry {attempt + 1}/{DEADLOCK_MAX_RETRIES} after {delay:.1f}s"
                        )
                        time.sleep(delay)
                        continue
                    log.exception(f"Failed DMP {dmp_doi} after {attempt + 1} attempts")
                    break

            if success:
                processed += 1
            else:
                errors += 1

            progress.update(1)
            update_postfix(progress, durations, processed=processed, skipped=skipped, errors=errors)
            t0 = time.perf_counter()

    finally:
        conn.close()

    progress.close()

    if skipped > 0:
        log.info(f"Skipped {skipped} DMPs with no matching plan")
    if errors > 0:
        log.info(f"Failed {errors} DMPs due to errors")

    log.info("Cleaning up orphaned works...")
    conn = make_connection(mysql_config)
    try:
        loader = RelatedWorksLoader(conn)
        with time_step(durations, "cleanup"):
            loader.run_cleanup_procedure()
        conn.commit()
    finally:
        conn.close()

    log.info("Step timing summary:")
    log_timing_summary(durations)


def update_postfix(
    progress: tqdm,
    durations: dict[str, RunningStats],
    *,
    processed: int,
    skipped: int,
    errors: int,
):
    """Update tqdm postfix with counts and step timing means.

    Args:
        progress: The tqdm progress bar.
        durations: Dictionary mapping step names to RunningStats accumulators.
        processed: Number of DMPs successfully processed.
        skipped: Number of DMPs skipped.
        errors: Number of DMPs that failed.
    """
    postfix: dict[str, str | int] = {"ok": processed, "skip": skipped, "err": errors}
    postfix_steps = ("read", "stage-tables", "work-versions", "related-works", "update-proc")
    postfix.update({name: f"{durations[name].mean:.3f}s" for name in postfix_steps if durations[name].count > 0})
    progress.set_postfix(postfix, refresh=False)
