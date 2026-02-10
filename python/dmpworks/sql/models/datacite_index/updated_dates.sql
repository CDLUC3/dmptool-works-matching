/*
  datacite_index.updated_dates:

  Chooses the most recent update_date for each DOI from DataCite and OpenAlex.
*/

MODEL (
  name datacite_index.updated_dates,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true,
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Choose the most recent updated date from DataCite and OpenAlex
SELECT
  doi,
  MAX(updated_date) AS updated_date
FROM (
  SELECT doi, updated_date
  FROM datacite_index.works
  WHERE updated_date IS NOT NULL

  UNION ALL

  SELECT doi, updated_date
  FROM openalex.openalex_works
  WHERE doi IS NOT NULL AND updated_date IS NOT NULL
)
GROUP BY doi;