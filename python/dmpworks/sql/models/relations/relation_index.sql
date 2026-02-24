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

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

WITH relations AS (
  SELECT
    work_doi AS doi,
    COALESCE(ARRAY_AGG(DISTINCT related_doi ORDER BY related_doi) FILTER (WHERE is_intra_work), []) AS intra_work_dois,
    COALESCE(ARRAY_AGG(DISTINCT related_doi ORDER BY related_doi) FILTER (WHERE is_possible_shared_project), []) AS possible_shared_project_dois,
    COALESCE(ARRAY_AGG(DISTINCT related_doi ORDER BY related_doi) FILTER (WHERE is_dataset_relation), []) AS dataset_citation_dois,
  FROM (
    -- DataCite
    SELECT
      r.work_doi,
      r.related_doi,
      r.is_intra_work,
      r.is_possible_shared_project,
      FALSE AS is_dataset_relation
    FROM relations.datacite r

    UNION ALL

    SELECT
      r.related_doi AS work_doi,
      r.work_doi AS related_doi,
      r.is_intra_work,
      r.is_possible_shared_project,
      FALSE AS is_dataset_relation
    FROM relations.datacite r

    UNION ALL

    -- Crossref Metadata
    SELECT
      r.work_doi,
      r.related_doi,
      r.is_intra_work,
      r.is_possible_shared_project,
      FALSE AS is_dataset_relation
    FROM relations.crossref_metadata r

    UNION ALL

    SELECT
      r.related_doi AS work_doi,
      r.work_doi AS related_doi,
      r.is_intra_work,
      r.is_possible_shared_project,
      FALSE AS is_dataset_relation
    FROM relations.crossref_metadata r

    UNION ALL

    -- Data Citation Corpus
    SELECT
      r.work_doi,
      r.dataset_doi AS related_doi,
      FALSE AS is_intra_work,
      FALSE AS is_possible_shared_project,
      TRUE AS is_dataset_relation
    FROM relations.data_citation_corpus r

    UNION ALL

    SELECT
      r.dataset_doi AS work_doi,
      r.work_doi AS related_doi,
      FALSE AS is_intra_work,
      FALSE AS is_possible_shared_project,
      TRUE AS is_dataset_relation
    FROM relations.data_citation_corpus r
  )
  GROUP BY work_doi
  HAVING COUNT(*) FILTER (
    WHERE is_intra_work
       OR is_possible_shared_project
       OR is_dataset_relation
  ) > 0
)

SELECT
  r.doi,
  list_transform(r.intra_work_dois, x -> {'doi': x}) AS intra_work_dois,
  list_transform(r.possible_shared_project_dois, x -> {'doi': x}) AS possible_shared_project_dois,
  list_transform(r.dataset_citation_dois, x -> {'doi': x}) AS dataset_citation_dois
FROM relations r
