/*
  openalex_index.updated_dates:

  Chooses the most recent update_date for each DOI from OpenAlex and Crossref
  Metadata.
*/


MODEL (
  name openalex_index.updated_dates,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  doi,
  MAX(updated_date) AS updated_date
FROM (
  SELECT owm.doi, updated_date
  FROM openalex_index.works_metadata AS owm
  INNER JOIN openalex.works ow ON owm.id = ow.id
  WHERE updated_date IS NOT NULL

  UNION ALL

  SELECT owm.doi, cfw.updated_date
  FROM openalex_index.works_metadata AS owm
  INNER JOIN crossref_metadata.works cfw ON owm.doi = cfw.doi
  WHERE owm.doi IS NOT NULL AND cfw.updated_date IS NOT NULL
)
GROUP BY doi;