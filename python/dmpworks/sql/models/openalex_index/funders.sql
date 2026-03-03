/*
  openalex_index.funder_ids:

  Creates a list of funders for each OpenAlex DOI. Converts OpenAlex funder IDs
  to ROR IDs. The order of the funders is maintained through WITH ORDINALITY
  and sorting on pos.
*/

MODEL (
  name openalex_index.funders,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (id, doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('openalex_index_funders_threads') AS INT64);

SELECT
  owm.id,
  owm.doi,
  list_transform(
    works.funders,
    x -> {
      'name': x.display_name,
      'ror': x.ror
    }
  ) AS funders
FROM openalex_index.works_metadata AS owm
LEFT JOIN openalex.openalex_works works ON owm.id = works.id
WHERE owm.is_primary_doi = TRUE AND works.funders IS NOT NULL AND ARRAY_LENGTH(works.funders) > 0
