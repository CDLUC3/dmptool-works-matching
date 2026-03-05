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

unique_pairs AS (
  SELECT DISTINCT
    work_doi,
    dataset_doi
  FROM relations_with_dois
  WHERE work_doi IS NOT NULL AND dataset_doi IS NOT NULL AND work_doi <> dataset_doi
),

pair_degrees AS (
  SELECT
    work_doi,
    dataset_doi,
    -- The number of datasets this work is connected to
    COUNT(*) OVER (PARTITION BY work_doi) AS work_degree,
    -- The number of works this dataset is connected to
    COUNT(*) OVER (PARTITION BY dataset_doi) AS dataset_degree
  FROM unique_pairs
),

filtered_pairs AS (
  SELECT
    work_doi,
    dataset_doi
  FROM pair_degrees
  WHERE work_degree <= CAST(@VAR('max_relation_degrees') AS INT64) AND dataset_degree <= CAST(@VAR('max_relation_degrees') AS INT64)
),

bidirectional AS (
  SELECT
    work_doi,
    dataset_doi AS related_doi
  FROM filtered_pairs

  UNION ALL

  SELECT
    dataset_doi AS work_doi,
    work_doi AS related_doi
  FROM filtered_pairs
)

SELECT
  work_doi AS doi,
  ARRAY_AGG(DISTINCT related_doi) AS dataset_citation_dois
FROM bidirectional
GROUP BY work_doi;