/*
  opensearch.next_doi_state:

  Contains a new historic record of the DOI state from this run.
*/

MODEL (
  name opensearch.next_doi_state,
  dialect duckdb,
  kind FULL,
  enabled true
);

WITH works_index AS (
  SELECT DISTINCT doi, hash
  FROM (
    SELECT doi, hash FROM openalex_index.openalex_index
    UNION ALL
    SELECT doi, hash FROM datacite_index.datacite_index
  )
),

latest_current_state AS (
  SELECT * FROM opensearch.current_doi_state
  QUALIFY ROW_NUMBER() OVER (PARTITION BY doi ORDER BY updated_date DESC) = 1
),

upserts AS (
  SELECT
    wi.doi,
    wi.hash,
    'UPSERT' AS state,
    CAST(@VAR('process_works_run_id') AS DATE) AS updated_date
  FROM works_index wi
  LEFT JOIN latest_current_state c ON wi.doi = c.doi
  WHERE c.doi IS NULL -- DOI is completely new (NULL in current state)
        OR c.hash != wi.hash -- DOI exists, but Hash has changed
        OR c.state = 'DELETE' -- DOI exists, matches Hash, but the last state was DELETE (it is being resurrected)
),

deletes AS (
  SELECT
    c.doi,
    c.hash, -- Last known hash
    'DELETE' AS state,
    CAST(@VAR('process_works_run_id') AS DATE) AS updated_date
  FROM latest_current_state c
  LEFT JOIN works_index wi ON c.doi = wi.doi
  WHERE wi.doi IS NULL AND c.state = 'UPSERT'
),

next_doi_state AS (
  SELECT doi, hash, state, updated_date FROM opensearch.current_doi_state

  UNION ALL

  SELECT doi, hash, state, updated_date FROM upserts

  UNION ALL

  SELECT doi, hash, state, updated_date FROM deletes
)

SELECT *
FROM next_doi_state
QUALIFY ROW_NUMBER() OVER (
 PARTITION BY doi
 ORDER BY updated_date DESC
) <= CAST(@VAR('max_doi_states') AS INT64)