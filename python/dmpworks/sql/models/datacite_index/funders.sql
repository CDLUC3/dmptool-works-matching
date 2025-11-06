/*
  datacite_index.funders:

  Creates a list of funders for each DataCite DOI. DateCite contains a variety
  of funder identifiers, all funder identifiers are converted into ROR identifiers.
  The order of the funders is maintained through WITH ORDINALITY and sorting on pos.
*/

MODEL (
  name datacite_index.funders,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT
  doi,
  array_agg(
    {
      'name': funder.funder_name,
      'ror': ror.index.ror_id
    } ORDER BY pos
  ) AS funders
FROM datacite_index.works AS dw, UNNEST(dw.funders) WITH ORDINALITY AS item(funder, pos)
LEFT JOIN ror.index ON funder.funder_identifier = ror.index.identifier
GROUP BY dw.doi;
