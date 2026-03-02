MODEL (
  name crossref.crossref_metadata,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := CAST(@VAR('audit_crossref_metadata_works_threshold') AS INT64)),
    unique_values(columns := (doi), blocking := false),
    not_empty_string(column := doi, blocking := false),
    not_empty_string(column := title, blocking := false),
    not_empty_string(column := abstract, blocking := false)
  )
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT *
FROM read_parquet(@VAR('crossref_metadata_path') || '/crossref_metadata_batch_[0-9]*_part_[0-9]*.parquet');