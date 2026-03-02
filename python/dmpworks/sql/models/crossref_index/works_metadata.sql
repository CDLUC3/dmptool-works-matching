MODEL (
  name crossref_index.works_metadata,
  dialect duckdb,
  kind FULL
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  doi,
  LENGTH(title) AS title_length,
  LENGTH(abstract) AS abstract_length,
FROM crossref.crossref_metadata;