from __future__ import annotations

from contextlib import closing
import logging
from typing import TYPE_CHECKING

from opensearchpy.helpers import streaming_bulk
import pendulum
from pydantic import ValidationError
import pymysql
import pymysql.cursors
from tqdm import tqdm

from dmpworks.opensearch.index import create_index
from dmpworks.opensearch.utils import OpenSearchClientConfig, make_opensearch_client
from dmpworks.transform.dmp import transform_dmp
from dmpworks.utils import timed

if TYPE_CHECKING:
    from dmpworks.cli_utils import MySQLConfig
    from dmpworks.model.common import Institution
    from dmpworks.model.dmp_model import DMPModel

log = logging.getLogger(__name__)

DMPS_QUERY_TEMPLATE = """
WITH unique_plans AS (
  SELECT id, dmpId, projectId, created, registered, modified, title, status
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY dmpId ORDER BY created DESC, id DESC) AS rn
    FROM plans
    WHERE dmpId IS NOT NULL
  ) AS ranked
  WHERE rn = 1
),

institutions AS (
  SELECT
    temp.plan_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'name', temp.name,
        'affiliation_id', temp.affiliation_id
      )
    ) AS institutions
  FROM (
    SELECT
      DISTINCT
      pl.id AS plan_id,
      af.name,
      prm.affiliationId AS affiliation_id
    FROM unique_plans pl
    INNER JOIN planMembers plm ON plm.planId = pl.id
    INNER JOIN projectMembers prm ON prm.id = plm.projectMemberId
    LEFT JOIN affiliations af ON af.uri = prm.affiliationId
  ) AS temp
  GROUP BY temp.plan_id
),

authors AS (
  SELECT
    temp.plan_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'plan_member_id', temp.plan_member_id,
        'given_name', temp.given_name,
        'surname', temp.surname,
        'orcid', temp.orcid,
        'is_primary_contact', temp.is_primary_contact,
        'created', temp.created
      )
    ) AS authors
  FROM (
    SELECT
      pl.id AS plan_id,
      plm.id AS plan_member_id,
      prm.givenName AS given_name,
      prm.surName AS surname,
      prm.orcid AS orcid,
      plm.isPrimaryContact AS is_primary_contact,
      plm.created
    FROM unique_plans pl
    INNER JOIN planMembers plm ON plm.planId = pl.id
    INNER JOIN projectMembers prm ON prm.id = plm.projectMemberId
  ) AS temp
  GROUP BY temp.plan_id
),

funding AS (
  SELECT
    temp.plan_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'project_funding_id', temp.project_funding_id,
        'funder_name', temp.funder_name,
        'funder_id', temp.funder_id,
        'funder_opportunity_id', temp.funder_opportunity_id,
        'grant_id', temp.grant_id,
        'funder_project_number', temp.funder_project_number,
        'status', temp.status,
        'created', temp.created
      )
    ) AS funding
  FROM (
    SELECT DISTINCT
      pl.id AS plan_id,
      prf.id AS project_funding_id,
      af.name AS funder_name,
      prf.affiliationId AS funder_id,
      prf.funderOpportunityNumber AS funder_opportunity_id,
      prf.grantId AS grant_id,
      prf.funderProjectNumber AS funder_project_number,
      prf.status,
      prf.created
    FROM unique_plans pl
    INNER JOIN projectFundings prf ON prf.projectId = pl.projectId
    LEFT JOIN affiliations af ON af.uri = prf.affiliationId
    WHERE COALESCE(
      af.name,
      prf.affiliationId,
      prf.funderOpportunityNumber,
      prf.grantId,
      prf.funderProjectNumber
    ) IS NOT NULL
  ) AS temp
  GROUP BY temp.plan_id
),

published_outputs AS (
  SELECT
    temp.plan_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'doi', temp.work_doi
      )
    ) AS published_outputs
  FROM (
    SELECT DISTINCT
      p.id AS plan_id,
      w.doi AS work_doi
    FROM unique_plans p
    INNER JOIN relatedWorks rw ON rw.planId = p.id
    INNER JOIN workVersions wv ON wv.id = rw.workVersionId
    INNER JOIN works w ON w.id = wv.workId
    WHERE rw.status = 'ACCEPTED'
  ) AS temp
  GROUP BY temp.plan_id
),

-- Includes REJECTED so the DMP is re-searched when a curator acts on any related work
published_outputs_modified AS (
  SELECT
    p.id AS plan_id,
    MAX(rw.modified) AS published_outputs_modified
  FROM unique_plans p
  INNER JOIN relatedWorks rw ON rw.planId = p.id
  WHERE rw.status IN ('ACCEPTED', 'REJECTED')
  GROUP BY p.id
)

SELECT
  pl.projectId AS project_id,
  pl.id AS plan_id,
  pl.dmpId AS doi,
  pl.created,
  pl.registered,
  GREATEST(pl.modified, COALESCE(pom.published_outputs_modified, pl.modified)) AS modified,
  pl.title,
  pr.abstractText AS abstract_text,
  pr.startDate AS project_start,
  pr.endDate AS project_end,
  COALESCE(au.authors, '[]') AS authors,
  COALESCE(inst.institutions, '[]') AS institutions,
  COALESCE(fn.funding, '[]') AS funding,
  COALESCE(po.published_outputs, '[]') AS published_outputs
FROM unique_plans AS pl
LEFT JOIN projects AS pr ON pr.id = pl.projectId
LEFT JOIN institutions inst ON inst.plan_id = pl.id
LEFT JOIN authors au ON au.plan_id = pl.id
LEFT JOIN funding fn ON fn.plan_id = pl.id
LEFT JOIN published_outputs po ON po.plan_id = pl.id
LEFT JOIN published_outputs_modified pom ON pom.plan_id = pl.id
WHERE pr.isTestProject = 0 AND pl.status = 'COMPLETE'
"""

DMPS_MAPPING_FILE = "dmps-mapping.json"


@timed
def sync_dmps(
    index_name: str,
    mysql_config: MySQLConfig,
    opensearch_config: OpenSearchClientConfig = None,
    chunk_size: int = 1000,
    institutions: list[Institution] | None = None,
    dois: list[str] | None = None,
):
    """Syncs the DMPs from the MySQL database to OpenSearch.

    Args:
        index_name: The name of the DMPs index.
        mysql_config: The MySQL configuration.
        opensearch_config: The OpenSearch client configuration.
        chunk_size: The number of DMPs to process per batch.
        institutions: When supplied only syncs DMPs from these institutions.
        dois: When supplied only syncs DMPs with these DOIs.
    """
    if opensearch_config is None:
        opensearch_config = OpenSearchClientConfig()

    os_client = make_opensearch_client(opensearch_config)

    # Create index if it doesn't exist
    client = make_opensearch_client(opensearch_config)
    create_index(client, index_name, DMPS_MAPPING_FILE)

    success_count = 0
    failed_count = 0
    skipped_count = 0

    with closing(
        pymysql.connect(
            host=mysql_config.mysql_host,
            port=mysql_config.mysql_tcp_port,
            user=mysql_config.mysql_user,
            password=mysql_config.mysql_pwd,
            database=mysql_config.mysql_database,
            cursorclass=pymysql.cursors.SSDictCursor,
        )
    ) as conn:

        def postfix():
            return {"Success": f"{success_count:,}", "Failed": f"{failed_count:,}", "Skipped": f"{skipped_count:,}"}

        def on_validation_error():
            nonlocal failed_count
            failed_count += 1
            pbar.update(1)
            pbar.set_postfix(postfix(), refresh=False)

        def on_skipped():
            nonlocal skipped_count
            skipped_count += 1
            pbar.update(1)
            pbar.set_postfix(postfix(), refresh=False)

        total_rows = count_dmps(conn)
        with tqdm(total=total_rows, desc="Sync DMPs with OpenSearch", unit="doc") as pbar:
            for ok, item in streaming_bulk(
                os_client,
                generate_actions(
                    conn=conn,
                    dmps_index=index_name,
                    on_error=on_validation_error,
                    on_skipped=on_skipped,
                    institutions=institutions,
                    dois=dois,
                ),
                chunk_size=chunk_size,
                raise_on_error=False,
            ):
                if ok:
                    success_count += 1
                else:
                    dmp_id = item.get("_id")
                    failed_count += 1
                    log.error(f"OpenSearch indexing failed for DMP: {dmp_id}")

                pbar.update(1)
                pbar.set_postfix(postfix(), refresh=False)


def count_dmps(conn):
    """Count the number of DMPs in the database.

    Args:
        conn: The MySQL connection.

    Returns:
        int: The number of DMPs.
    """
    query = """
        SELECT COUNT(DISTINCT pl.dmpId) AS total
        FROM plans pl
        LEFT JOIN projects pr ON pr.id = pl.projectId
        WHERE pl.dmpId IS NOT NULL AND pr.isTestProject = 0 AND pl.status = 'COMPLETE'
    """
    with conn.cursor() as count_cursor:
        count_cursor.execute(query)
        result = count_cursor.fetchone()
        return result["total"]


def validate_dmp(dmp: DMPModel) -> None:
    """Validate a DMP model, raising ValueError if invalid.

    Checks that required fields are present and that field values are within
    acceptable ranges. Currently validates the DOI and project start date.

    Args:
        dmp: The DMP object to validate.

    Raises:
        ValueError: If the DMP is missing or has an invalid DOI.
        ValueError: If the project start date is on or before 1900-01-01.
    """
    if not dmp.doi:
        raise ValueError("Missing or invalid DOI")
    if dmp.project_start is not None and dmp.project_start <= pendulum.date(1900, 1, 1):
        raise ValueError(
            f"Project start date less than 1900-01-01 for DMP: doi={dmp.doi}, project_start={dmp.project_start}"
        )


def generate_actions(
    *,
    conn,
    dmps_index: str,
    on_error: callable,
    on_skipped: callable,
    institutions: list[Institution] | None = None,
    dois: list[str] | None = None,
):
    """Generate OpenSearch bulk actions from MySQL rows.

    Args:
        conn: The MySQL connection.
        dmps_index: The name of the DMPs index.
        on_error: A callback function to call when a validation error occurs.
        on_skipped: A callback function to call when a DMP is filtered by subset.
        institutions: When supplied only syncs DMPs from these institutions (matched by ROR ID).
        dois: When supplied only syncs DMPs with these DOIs.

    Yields:
        dict: An OpenSearch bulk action.
    """
    dois_set = set(dois) if dois else None
    ror_set = {inst.ror for inst in institutions if inst.ror} if institutions else None

    def include_dmp(dmp: DMPModel) -> bool:
        if dois_set is None and ror_set is None:
            return True

        if dois_set is not None and dmp.doi in dois_set:
            return True

        return ror_set is not None and any(inst.ror in ror_set for inst in dmp.institutions if inst.ror)

    with conn.cursor() as stream_cursor:
        stream_cursor.execute(DMPS_QUERY_TEMPLATE)
        for row in stream_cursor:
            try:
                dmp = transform_dmp(row)
                validate_dmp(dmp)

                if not include_dmp(dmp):
                    on_skipped()
                    continue

                yield {
                    "_op_type": "update",
                    "_index": dmps_index,
                    "_id": dmp.doi,
                    "doc": dmp.model_dump(exclude={"external_data"}),
                    "doc_as_upsert": True,
                }
            except (ValidationError, ValueError, TypeError):
                dmp_doi = row.get("doi") or "UNKNOWN DOI"
                log.exception(f"Skipping invalid DMP: {dmp_doi}")
                on_error()
