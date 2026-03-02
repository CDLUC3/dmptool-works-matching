import logging
from contextlib import closing

import pendulum
import pymysql
import pymysql.cursors
from opensearchpy.helpers import streaming_bulk
from pydantic import ValidationError
from tqdm import tqdm

from dmpworks.cli_utils import MySQLConfig
from dmpworks.opensearch.index import create_index
from dmpworks.opensearch.utils import make_opensearch_client, OpenSearchClientConfig
from dmpworks.transform.dmp import transform_dmp
from dmpworks.utils import timed

log = logging.getLogger(__name__)

DMPS_QUERY = """
WITH institutions AS (
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
    FROM plans pl
    INNER JOIN planMembers plm ON plm.planId = pl.id
    INNER JOIN projectMembers prm ON prm.id = plm.projectMemberId
    LEFT JOIN affiliations af ON af.uri = prm.affiliationId
    WHERE pl.dmpId IS NOT NULL
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
    FROM plans pl
    INNER JOIN planMembers plm ON plm.planId = pl.id
    INNER JOIN projectMembers prm ON prm.id = plm.projectMemberId
    WHERE pl.dmpId IS NOT NULL
  ) AS temp
  GROUP BY temp.plan_id
),

funding AS (
  SELECT
    temp.plan_id,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'plan_funding_id', temp.plan_funding_id,
        'funder_name', temp.funder_name,
        'funder_id', temp.funder_id,
        'funder_opportunity_id', temp.funder_opportunity_id,
        'grant_id', temp.grant_id,
        'status', temp.status,
        'created', temp.created
      )
    ) AS funding
  FROM (
    SELECT
      pl.id AS plan_id,
      plf.id AS plan_funding_id,	  
      af.name AS funder_name,
      prf.affiliationId AS funder_id,
      prf.funderOpportunityNumber AS funder_opportunity_id,
      prf.grantId AS grant_id,
      prf.status,
      plf.created
    FROM plans pl
    INNER JOIN planFundings plf ON plf.planId = pl.id
    INNER JOIN projectFundings prf ON prf.id = plf.projectFundingId
    LEFT JOIN affiliations af ON af.uri = prf.affiliationId
    WHERE pl.dmpId IS NOT NULL 
          AND COALESCE(af.name, prf.affiliationId, prf.funderOpportunityNumber, prf.grantId) IS NOT NULL
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
    FROM plans p
    INNER JOIN relatedWorks rw ON rw.planId = p.id
    INNER JOIN workVersions wv ON wv.id = rw.workVersionId
    INNER JOIN works w ON w.id = wv.workId
    WHERE p.dmpId IS NOT NULL AND rw.status = 'ACCEPTED'
  ) AS temp
  GROUP BY temp.plan_id
)

SELECT
  pl.dmpId AS doi,
  pl.created,
  pl.registered,
  pl.modified,
  pl.title,
  pr.abstractText AS abstract_text,
  pr.startDate AS project_start,
  pr.endDate AS project_end,
  COALESCE(au.authors, '[]') AS authors, 
  COALESCE(inst.institutions, '[]') AS institutions, 
  COALESCE(fn.funding, '[]') AS funding, 
  COALESCE(po.published_outputs, '[]') AS published_outputs 
FROM plans AS pl
LEFT JOIN projects AS pr ON pr.id = pl.projectId
LEFT JOIN institutions inst ON inst.plan_id = pl.id
LEFT JOIN authors au ON au.plan_id = pl.id
LEFT JOIN funding fn ON fn.plan_id = pl.id
LEFT JOIN published_outputs po ON po.plan_id = pl.id
WHERE pl.dmpId IS NOT NULL;
"""


DMPS_MAPPING_FILE = "dmps-mapping.json"


@timed
def sync_dmps(
    index_name: str,
    mysql_config: MySQLConfig,
    opensearch_config: OpenSearchClientConfig = None,
    chunk_size: int = 1000,
):
    """Syncs the DMPs from the MySQL database to OpenSearch"""

    if opensearch_config is None:
        opensearch_config = OpenSearchClientConfig()

    os_client = make_opensearch_client(opensearch_config)

    # Create index if it doesn't exist
    client = make_opensearch_client(opensearch_config)
    create_index(client, index_name, DMPS_MAPPING_FILE)

    success_count = 0
    failed_count = 0

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

        def on_validation_error():
            nonlocal failed_count
            failed_count += 1
            pbar.update(1)
            pbar.set_postfix({"Success": f"{success_count:,}", "Failed": f"{failed_count:,}"})

        total_rows = count_dmps(conn)
        with tqdm(total=total_rows, desc="Sync DMPs with OpenSearch", unit="doc") as pbar:
            for ok, item in streaming_bulk(
                os_client,
                generate_actions(conn=conn, dmps_index=index_name, on_error=on_validation_error),
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
                pbar.set_postfix(
                    {
                        "Success": f"{success_count:,}",
                        "Failed": f"{failed_count:,}",
                    }
                )


def count_dmps(conn):
    with conn.cursor() as count_cursor:
        count_cursor.execute("SELECT COUNT(*) AS total FROM plans WHERE dmpId IS NOT NULL;")
        result = count_cursor.fetchone()
        return result["total"]


def generate_actions(*, conn, dmps_index: str, on_error: callable):
    with conn.cursor() as stream_cursor:
        stream_cursor.execute(DMPS_QUERY)
        for row in stream_cursor:
            try:
                dmp = transform_dmp(row)
                if not dmp.doi:
                    raise ValueError(f"Missing or invalid DOI")
                if dmp.project_start is not None and dmp.project_start <= pendulum.date(1900, 1, 1):
                    raise ValueError(
                        f"Project start date less than 1900-01-01 for DMP: doi={dmp.doi}, project_start={dmp.project_start}"
                    )

                yield {
                    "_op_type": "update",
                    "_index": dmps_index,
                    "_id": dmp.doi,
                    "doc": dmp.model_dump(exclude={"external_data"}),
                    "doc_as_upsert": True,
                }
            except (ValidationError, ValueError, TypeError) as e:
                dmp_doi = row.get("doi") or "UNKNOWN DOI"
                log.warning(f"Skipping invalid DMP: {dmp_doi}. Reason: {e}")
                on_error()
