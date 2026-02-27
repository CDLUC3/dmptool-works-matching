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

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

WITH award_ids AS (
  SELECT
    doi,
    @array_agg_distinct(award_id) AS award_ids
  FROM (
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
  )
  GROUP BY doi
)

SELECT
  award_ids.doi,
  list_transform(award_ids.award_ids, x -> {'award_id': x}) AS awards
FROM award_ids
