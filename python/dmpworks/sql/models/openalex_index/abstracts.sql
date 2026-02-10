/*
  openalex_index.titles:

  Chooses the longest abstract for each DOI from OpenAlex and Crossref Metadata.
*/

MODEL (
  name openalex_index.abstracts,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('openalex_index_abstracts_threads') AS INT64);

SELECT
  stats.doi,
  CASE
    WHEN cwm.abstract_length > stats.abstract_length THEN cfw.abstract
    WHEN cwm.abstract_length IS NOT NULL AND stats.abstract_length IS NULL THEN cfw.abstract
    ELSE oaw.abstract
  END AS abstract
FROM openalex_index.abstract_stats AS stats
LEFT JOIN openalex.openalex_works oaw ON stats.id = oaw.id
LEFT JOIN crossref_index.works_metadata cwm ON stats.doi = cwm.doi
LEFT JOIN crossref.crossref_metadata cfw ON stats.doi = cfw.doi;
