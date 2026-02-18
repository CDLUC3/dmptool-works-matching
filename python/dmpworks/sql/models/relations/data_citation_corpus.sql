/*
  relations.data_citation_corpus:

  Extract Data Citation Corpus relations between DOIs.
*/

MODEL (
  name relations.data_citation_corpus,
  dialect duckdb,
  kind FULL,
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

WITH relations_with_dois AS (
  SELECT
    @extract_doi(r.publication) AS work_doi,
    @extract_doi(r.dataset) AS dataset_doi,
    source
  FROM data_citation_corpus.relations r
)

SELECT
  rd.work_doi,
  rd.dataset_doi,
  rd.source
FROM relations_with_dois rd
WHERE rd.work_doi IS NOT NULL AND rd.dataset_doi IS NOT NULL AND rd.work_doi <> rd.dataset_doi
