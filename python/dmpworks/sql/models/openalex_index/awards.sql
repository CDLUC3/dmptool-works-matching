/*
  openalex_index.awards:

  Aggregates distinct affiliation award identifiers for each OpenAlex and Crossref
  Metadata work, grouped by DOI. Grouping by DOI also handles cases where
  multiple OpenAlex records share the same DOI. DataCite works are excluded via
  openalex_index.works_metadata.
*/

MODEL (
  name openalex_index.awards,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('openalex_index_awards_threads') AS INT64);

WITH raw_awards AS (
  -- OpenAlex
  SELECT owm.id, owm.doi, award.funder_award_id AS award_id
  FROM openalex_index.works_metadata AS owm
  INNER JOIN openalex.openalex_works works ON owm.id = works.id, UNNEST(works.awards) AS item(award)
  WHERE award.funder_award_id IS NOT NULL

  UNION ALL

  -- Crossref Metadata
  SELECT owm.id, owm.doi, funder.award AS award_id
  FROM openalex_index.works_metadata owm
  INNER JOIN crossref.crossref_metadata cm ON owm.doi = cm.doi, UNNEST(cm.funders) AS item(funder)
  WHERE funder.award IS NOT NULL
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