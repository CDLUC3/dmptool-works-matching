/*
  datacite_index.awards:

  Aggregates distinct award identifiers for DataCite works found in DataCite and
  OpenAlex, grouped by DOI.
*/

MODEL (
  name datacite_index.awards,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('datacite_index_awards_threads') AS INT64);

WITH raw_awards AS (
  -- DataCite
  SELECT doi, funder.award_number AS award_id
  FROM datacite_index.works, UNNEST(funders) AS item(funder)
  WHERE funder.award_number IS NOT NULL

  UNION ALL

  -- OpenAlex
  SELECT dw.doi, award.funder_award_id AS award_id
  FROM datacite_index.works dw
  INNER JOIN openalex.openalex_works ow ON dw.doi = ow.doi, UNNEST(ow.awards) AS item(award)
  WHERE award.funder_award_id IS NOT NULL
),

distinct_awards AS (
  SELECT DISTINCT
    doi,
    award_id
  FROM raw_awards
),

award_ids AS (
  SELECT
    doi,
    COALESCE(ARRAY_AGG(award_id ORDER BY LOWER(award_id) ASC), []) AS award_ids
  FROM distinct_awards
  GROUP BY doi
)

SELECT
  doi,
  list_transform(award_ids, x -> {'award_id': x}) AS awards
FROM award_ids;
