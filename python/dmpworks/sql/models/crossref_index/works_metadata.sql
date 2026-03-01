MODEL (
  name crossref_index.works_metadata,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('crossref_index_works_metadata_threads') AS INT64);

SELECT
  doi,
  LENGTH(title) AS title_length,
  LENGTH(abstract) AS abstract_length,
FROM crossref.crossref_metadata;