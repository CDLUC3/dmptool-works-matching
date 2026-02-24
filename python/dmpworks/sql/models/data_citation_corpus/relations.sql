MODEL (
  name data_citation_corpus.relations,
  dialect duckdb,
  kind VIEW,
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT *
FROM read_json_auto(@VAR('data_citation_corpus_path') || '/*.json');