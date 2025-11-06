MODEL (
  name datacite.relations,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT *
FROM read_parquet(@VAR('datacite_path') || '/datacite_works_relations_[0-9]*.parquet');

