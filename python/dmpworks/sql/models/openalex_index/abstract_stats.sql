/*
  openalex_index.abstract_stats:

  Collates abstract specific metadata for each unique DOI. DataCite works are
  excluded in openalex_index.works_metadata. The first SELECT processes OpenAlex
  works with duplicate DOIs, selecting the work with the longest abstract. The
  second SELECT processes non-duplicate works. Since this is the leftmost table
  in openalex_index.abstracts, it includes works also found in Crossref Metadata,
  even if their abstracts are NULL (no length), ensuring they are available for
  the subsequent step to check Crossref Metadata for more suitable abstracts.
*/

MODEL (
  name openalex_index.abstract_stats,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (id, doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Choose the id with the longest abstract for each duplicate DOI
SELECT
  COALESCE(ARG_MAX(owm.id, owm.abstract_length), MIN(id)) AS id,
  owm.doi,
  MAX(owm.abstract_length) AS abstract_length
FROM openalex_index.works_metadata owm
LEFT JOIN crossref_index.works_metadata cwm ON owm.doi = cwm.doi
WHERE owm.is_duplicate = TRUE AND (owm.abstract_length > 0 OR cwm.abstract_length > 0)
GROUP BY owm.doi

UNION ALL

-- Select all remaining id, doi pairs
SELECT
  owm.id,
  owm.doi,
  owm.abstract_length
FROM openalex_index.works_metadata owm
LEFT JOIN crossref_index.works_metadata cwm ON owm.doi = cwm.doi
WHERE owm.is_duplicate = FALSE AND (owm.abstract_length > 0 OR cwm.abstract_length > 0)