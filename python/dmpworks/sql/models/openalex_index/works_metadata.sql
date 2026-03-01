/*
  openalex_index.works_metadata:

  Filters out OpenAlex works that are also present in DataCite, and then
  collates metadata for the remaining works — including OpenAlex ID, DOI, title
  length, abstract length, a duplicate flag (whether another OpenAlex work
  shares the same DOI), it also counts the number of unique non null ids
  for each work, which are used to determine which work to use when works
  share the same DOI (the work with the most metadata).

  This table is used by downstream queries as the leftmost table in joins, so
  that non-DataCite OpenAlex works are used in further processing.
*/

MODEL (
  name openalex_index.works_metadata,
  dialect duckdb,
  kind FULL,
  enabled true
);

PRAGMA threads=CAST(@VAR('openalex_index_works_metadata_threads') AS INT64);

-- Remove works that can be found in DataCite
-- And works without DOIs
WITH base AS (
  SELECT
    id,
    doi
  FROM openalex.openalex_works oaw
  WHERE doi IS NOT NULL AND NOT EXISTS (SELECT 1 FROM datacite.datacite WHERE oaw.doi = datacite.datacite.doi)
),

-- Count how many instances of each DOI
doi_counts AS (
  SELECT
    doi,
    COUNT(*) AS doi_count
  FROM base
  GROUP BY doi
),

metadata_counts AS (
  SELECT
    base.id,
    base.doi,
    dc.doi_count,

    ((CASE WHEN ow.ids.mag IS NOT NULL THEN 1 ELSE 0 END) +
     (CASE WHEN ow.ids.pmid IS NOT NULL THEN 1 ELSE 0 END) +
     (CASE WHEN ow.ids.pmcid IS NOT NULL THEN 1 ELSE 0 END)) AS id_count,

    COALESCE(list_unique(list_filter(list_transform(ow.authors, x -> x.orcid), x -> x IS NOT NULL)), 0) AS orcid_count,
    COALESCE(list_unique(list_filter(list_transform(ow.awards, x -> x.funder_award_id), x -> x IS NOT NULL)), 0) AS award_id_count,
    COALESCE(list_unique(list_filter(list_transform(ow.funders, x -> x.id), x -> x IS NOT NULL)), 0) AS funder_id_count,
    COALESCE(list_unique(list_filter(list_transform(ow.institutions, x -> x.ror), x -> x IS NOT NULL)), 0) AS inst_id_count,

    LENGTH(ow.title) AS title_length,
    LENGTH(ow.abstract) AS abstract_length
  FROM base
  LEFT JOIN doi_counts dc ON base.doi = dc.doi
  LEFT JOIN openalex.openalex_works ow ON base.id = ow.id
),

ranked_works AS (
  SELECT
    *,
    (id_count + orcid_count + funder_id_count + award_id_count + inst_id_count) AS total_count,
    ROW_NUMBER() OVER(
      PARTITION BY doi
      ORDER BY (id_count + orcid_count + funder_id_count + award_id_count + inst_id_count) DESC, id
    ) AS doi_rank
  FROM metadata_counts
)

SELECT
  id,
  doi,
  title_length,
  abstract_length,
  doi_count,
  id_count,
  orcid_count,
  funder_id_count,
  award_id_count,
  inst_id_count,
  total_count,
  doi_rank,
  doi_count > 1 AS is_duplicate,
  (doi_rank = 1) AS is_primary_doi
FROM ranked_works;
