import logging

from dmpworks.dmsp.loader import RelatedWorkReference

log = logging.getLogger(__name__)


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
        rows = cursor.fetchall()

    log.info("Fetched %d migration related works.", len(rows))
    return [
        RelatedWorkReference(plan_id=row["plan_id"], dmp_id=row["dmp_id"], work_doi=row["work_doi"]) for row in rows
    ]
