MODEL (
  name crossref_index.works_metadata,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  )
);

PRAGMA threads=CAST(@VAR('crossref_index_works_metadata_threads') AS INT64);

SELECT
  doi,
  title,
  abstract,
  LENGTH(title) AS title_length,
  LENGTH(abstract) AS abstract_length,
FROM crossref.crossref_metadata
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY doi
  ORDER BY LENGTH(abstract) DESC NULLS LAST, LENGTH(title) DESC NULLS LAST
) = 1;