MODEL (
  name crossref_metadata.works,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := CAST(@VAR('audit_crossref_metadata_works_threshold') AS INT64)),
    unique_values(columns := (doi), blocking := false),
    not_empty_string(column := doi, blocking := false),
    not_empty_string(column := title, blocking := false),
    not_empty_string(column := abstract, blocking := false),
    not_empty_string(column := type, blocking := false),
    not_empty_string(column := container_title, blocking := false),
    not_empty_string(column := volume, blocking := false),
    not_empty_string(column := issue, blocking := false),
    not_empty_string(column := page, blocking := false),
    not_empty_string(column := publisher, blocking := false),
    not_empty_string(column := publisher_location, blocking := false)
  )
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT *
FROM read_parquet(@VAR('crossref_metadata_path') || '/crossref_works_[0-9]*.parquet');