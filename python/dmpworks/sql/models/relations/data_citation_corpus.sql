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

PRAGMA threads=CAST(@VAR('relations_data_citation_corpus_threads') AS INT64);

WITH relations_with_dois AS (
  SELECT
    @extract_doi(r.publication) AS work_doi,
    @extract_doi(r.dataset) AS dataset_doi
  FROM data_citation_corpus.relations r
),

bidirectional AS (
  SELECT
    rd.work_doi,
    rd.dataset_doi AS related_doi
  FROM relations_with_dois rd
  WHERE rd.work_doi IS NOT NULL AND rd.dataset_doi IS NOT NULL AND rd.work_doi <> rd.dataset_doi

  UNION ALL

  SELECT
    rd.dataset_doi AS work_doi,
    rd.work_doi AS related_doi
  FROM relations_with_dois rd
  WHERE rd.work_doi IS NOT NULL AND rd.dataset_doi IS NOT NULL AND rd.work_doi <> rd.dataset_doi
)

SELECT
  work_doi AS doi,
  ARRAY_AGG(DISTINCT related_doi) AS dataset_citation_dois
FROM bidirectional
GROUP BY work_doi;