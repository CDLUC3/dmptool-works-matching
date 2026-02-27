/*
  openalex_index.titles:

  Chooses the longest title for each DOI from OpenAlex and Crossref Metadata.
*/

MODEL (
  name openalex_index.titles,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('openalex_index_titles_threads') AS INT64);

SELECT
  stats.doi,
  CASE
    WHEN cwm.title_length > stats.title_length THEN cfw.title
    WHEN cwm.title_length IS NOT NULL AND stats.title_length IS NULL THEN cfw.title
    ELSE oaw.title
  END AS title
FROM openalex_index.title_stats AS stats
LEFT JOIN openalex.openalex_works oaw ON stats.id = oaw.id
LEFT JOIN crossref_index.works_metadata cwm ON stats.doi = cwm.doi
LEFT JOIN crossref.crossref_metadata cfw ON stats.doi = cfw.doi;
