/*
  opensearch.current_doi_state:

  Contains a historic record of the DOI state before this run.
*/

MODEL (
  name opensearch.current_doi_state,
  dialect duckdb,
  kind VIEW
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT *
FROM read_parquet(@VAR('opensearch_path') || '/doi_state_[0-9]*.parquet');