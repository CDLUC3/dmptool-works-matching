MODEL (
  name openalex.works,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := CAST(@VAR('audit_openalex_works_threshold') AS INT64)),
    unique_values(columns := (id), blocking := false),
    not_empty_string(column := id, blocking := false),
    not_empty_string(column := doi, blocking := false),
    not_empty_string(column := title, blocking := false),
    not_empty_string(column := abstract, blocking := false),
    accepted_values(column := type, is_in := ('article', 'book', 'book-chapter', 'dataset', 'dissertation', 'editorial',
                                              'erratum', 'grant', 'letter', 'libguides', 'other', 'paratext', 'peer-review',
                                              'preprint', 'reference-entry', 'report', 'retraction', 'review', 'standard',
                                              'supplementary-materials'), blocking := false),
    not_empty_string(column := publication_venue, blocking := false)
  )
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT *
FROM read_parquet(@VAR('openalex_works_path') || '/openalex_works_[0-9]*.parquet');