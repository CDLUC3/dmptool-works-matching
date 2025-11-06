/*
  openalex_index.publication_dates:

  Handles cases where multiple OpenAlex records share the same DOI, by choosing
  the most recent publication date for each DOI. DataCite works are excluded
  via openalex_index.works_metadata.
*/

MODEL (
  name openalex_index.publication_dates,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  owm.doi,
  MAX(publication_date) AS publication_date
FROM openalex_index.works_metadata AS owm
INNER JOIN openalex.works ow ON owm.id = ow.id
WHERE publication_date IS NOT NULL
GROUP BY owm.doi;