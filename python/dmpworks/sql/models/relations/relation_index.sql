/*
  relations.relations_index:

  Relations from Crossref Metadata, DataCite and Data Citation Corpus merged.
*/

MODEL (
  name relations.relations_index,
  dialect duckdb,
  kind FULL,
  enabled true
);

PRAGMA threads=CAST(@VAR('relations_relations_index_threads') AS INT64);

WITH all_dois AS (
  SELECT doi FROM relations.crossref_metadata
  UNION
  SELECT doi FROM relations.datacite
  UNION
  SELECT doi FROM relations.data_citation_corpus
),

merged AS (
  SELECT
    dois.doi,

    list_distinct(
      list_concat(COALESCE(c.intra_work_dois, []), COALESCE(d.intra_work_dois, []))
    ) AS intra_work_dois,

    list_distinct(
      list_concat(COALESCE(c.possible_shared_project_dois, []), COALESCE(d.possible_shared_project_dois, []))
    ) AS possible_shared_project_dois,

    COALESCE(dcc.dataset_citation_dois, []) AS dataset_citation_dois
  FROM all_dois dois
  LEFT JOIN relations.crossref_metadata c ON dois.doi = c.doi
  LEFT JOIN relations.datacite d ON dois.doi = d.doi
  LEFT JOIN relations.data_citation_corpus dcc ON dois.doi = dcc.doi
)

SELECT
  doi,
  list_transform(list_sort(intra_work_dois), x -> {'doi': x}) AS intra_work_dois,
  list_transform(list_sort(possible_shared_project_dois), x -> {'doi': x}) AS possible_shared_project_dois,
  list_transform(list_sort(dataset_citation_dois), x -> {'doi': x}) AS dataset_citation_dois
FROM merged;