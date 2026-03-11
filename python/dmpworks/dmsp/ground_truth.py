import csv
import logging
import pathlib

from dmpworks.dmsp.loader import RelatedWorkReference

log = logging.getLogger(__name__)


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
                log.warning(f"Row {i}: Skipped (Missing 'work_doi')")
                continue

            if not plan_id and not dmp_id:
                log.warning(f"Row {i}: Skipped (Both 'plan_id' and 'dmp_id' are missing)")
                continue

            results.append(RelatedWorkReference(plan_id=plan_id, dmp_id=dmp_id, work_doi=work_doi))

    log.info("Read %d ground truth records from %s.", len(results), file_path)
    return results
