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

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  owm.id,
  owm.doi,
  array_agg(
    {
      'name': funder.display_name,
      'ror': funder.ror
    } ORDER BY pos
  ) AS funders
FROM openalex_index.works_metadata AS owm
LEFT JOIN openalex.works works ON owm.id = works.id, UNNEST(works.funders) WITH ORDINALITY AS item(funder, pos)
WHERE owm.is_primary_doi = TRUE
GROUP BY owm.id, owm.doi
