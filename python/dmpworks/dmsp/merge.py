from collections.abc import Generator
import contextlib
import gzip
import logging
import math
import pathlib
import time

from tqdm import tqdm

from dmpworks.dmsp.loader import RelatedWorksLoader, to_sql_related_work_row, to_sql_work_version_row
from dmpworks.dmsp.related_works import json_work_to_work_version
from dmpworks.utils import yield_objects_from_jsonl

log = logging.getLogger(__name__)

STEP_NAMES = ("stage-tables", "work-versions", "related-works", "update-proc", "cleanup")


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


def count_match_records(matches_dir: pathlib.Path) -> int:
    """Count total DMP records across all gzipped JSONL match files.

    Args:
        matches_dir: Directory containing .jsonl.gz match data files.

    Returns:
        Total number of non-empty lines (one per DMP) across all files.
    """
    count = 0
    for path in matches_dir.glob("*.jsonl.gz"):
        with gzip.open(path, "rb") as f:
            for line in f:
                if line.strip():
                    count += 1
    return count


def read_match_records(matches_dir: pathlib.Path) -> Generator[tuple[str, list[dict]], None, None]:
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


def merge_related_works(matches_dir: pathlib.Path, conn, batch_size: int = 1000):
    """Merge related works data into the database, processing one DMP at a time.

    Reads match data from gzip-compressed JSONL files, then processes each DMP's
    work versions and related works. Commits after each DMP so that progress is
    preserved if the job fails partway through.

    Args:
        matches_dir: Directory containing .jsonl.gz match data files.
        conn: Database connection object.
        batch_size: Number of records to process in a batch.
    """
    durations: dict[str, RunningStats] = {name: RunningStats() for name in STEP_NAMES}

    with RelatedWorksLoader(conn) as loader:
        total = count_match_records(matches_dir)
        progress = tqdm(desc="Merging related works", unit="dmp", total=total)
        for dmp_doi, works in read_match_records(matches_dir):
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
                            "dmpDoi": dmp_doi,
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

            with time_step(durations, "stage-tables"):
                loader.prepare_staging_tables()
            with time_step(durations, "work-versions"):
                loader.insert_work_versions(work_version_rows, batch_size=batch_size)
            with time_step(durations, "related-works"):
                loader.insert_related_works(related_work_rows, batch_size=batch_size)
            with time_step(durations, "update-proc"):
                loader.run_update_procedure(system_matched=True)

            loader.commit()
            progress.update(1)

            postfix_steps = ("stage-tables", "work-versions", "related-works", "update-proc")
            progress.set_postfix(
                {name: f"{durations[name].mean:.3f}s" for name in postfix_steps if durations[name].count > 0},
                refresh=False,
            )

        progress.close()
        log.info("Cleaning up orphaned works...")
        with time_step(durations, "cleanup"):
            loader.run_cleanup_procedure()

    log.info("Step timing summary:")
    log_timing_summary(durations)
