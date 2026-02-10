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

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Remove works that can be found in DataCite
-- And works without DOIs
WITH base AS (
  SELECT
    id,
    doi
  FROM openalex.works oaw
  WHERE doi IS NOT NULL AND NOT EXISTS (SELECT 1 FROM datacite.works WHERE oaw.doi = datacite.works.doi)
),

-- Count how many unique ORCID IDs per work
orcid_counts AS (
  SELECT
    base.id,
    COUNT(DISTINCT author.orcid) AS orcid_count
  FROM base
  LEFT JOIN openalex.works ow ON base.id = ow.id, UNNEST(ow.authors) AS item(author)
  WHERE author.orcid IS NOT NULL
  GROUP BY base.id
),

-- Count how many unique Award IDs
award_counts AS (
  SELECT
    base.id,
    COUNT(DISTINCT award.funder_award_id) AS award_id_count
  FROM base
  LEFT JOIN openalex.works ow ON base.id = ow.id, UNNEST(ow.awards) AS item(award)
  GROUP BY base.id
),

-- Count how many unique funder IDs per work
funder_counts AS (
  SELECT
    base.id,
    COUNT(DISTINCT funder.id) AS funder_id_count
  FROM base
  LEFT JOIN openalex.works ow ON base.id = ow.id, UNNEST(ow.funders) AS item(funder)
  GROUP BY base.id
),

-- Count how many unique institution ROR IDs per work
inst_id_counts AS (
  SELECT
    base.id,
    COUNT(DISTINCT inst.ror) AS inst_id_count
  FROM base
  LEFT JOIN openalex.works ow ON base.id = ow.id, UNNEST(ow.institutions) AS item(inst)
  WHERE inst.ror IS NOT NULL
  GROUP BY base.id
),

-- Count how many instances of each DOI
doi_counts AS (
  SELECT
    doi,
    COUNT(*) AS doi_count
  FROM base
  GROUP BY doi
),

counts AS (
  SELECT
    base.id,
    base.doi,
    dc.doi_count,
    ((CASE WHEN ow.ids.mag IS NOT NULL THEN 1 ELSE 0 END) + (CASE WHEN ow.ids.pmid IS NOT NULL THEN 1 ELSE 0 END) + (CASE WHEN ow.ids.pmcid IS NOT NULL THEN 1 ELSE 0 END)) AS id_count,
    COALESCE(oc.orcid_count, 0) AS orcid_count,
    COALESCE(fc.funder_id_count, 0) AS funder_id_count,
    COALESCE(ac.award_id_count, 0) AS award_id_count,
    COALESCE(ic.inst_id_count, 0) AS inst_id_count
  FROM base
  LEFT JOIN doi_counts dc ON base.doi = dc.doi
  LEFT JOIN openalex.works ow ON base.id = ow.id
  LEFT JOIN orcid_counts AS oc ON ow.id = oc.id
  LEFT JOIN award_counts AS ac ON ow.id = ac.id
  LEFT JOIN funder_counts AS fc ON ow.id = fc.id
  LEFT JOIN inst_id_counts AS ic ON ow.id = ic.id
),

ranked_works AS (
  SELECT
    *,
    (id_count + orcid_count + funder_id_count + award_id_count + inst_id_count) AS total_count,
    ROW_NUMBER() OVER(
      PARTITION BY doi
      ORDER BY (id_count + orcid_count + funder_id_count + award_id_count + inst_id_count) DESC, id
    ) AS doi_rank
  FROM counts
)

SELECT
  base.id,
  base.doi,
  LENGTH(oaw.title) AS title_length,
  LENGTH(oaw.abstract) AS abstract_length,
  rw.doi_count,
  rw.id_count,
  rw.orcid_count,
  rw.funder_id_count,
  rw.award_id_count,
  rw.inst_id_count,
  rw.total_count,
  rw.doi_rank,
  rw.doi_count > 1 AS is_duplicate,
  (doi_rank = 1) AS is_primary_doi
FROM base
LEFT JOIN ranked_works rw ON base.id = rw.id
LEFT JOIN openalex.works oaw ON base.id = oaw.id;
