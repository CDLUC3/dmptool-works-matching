/*
  datacite_index.institutions:

  Creates a list of institutions for each DataCite DOI. The DataCite
  affiliation_identifier field contains a variety of different identifiers,
  which are converted into ROR identifiers. The order of the institutions
  is maintained through WITH ORDINALITY and sorting on pos.
*/

MODEL (
  name datacite_index.institutions,
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
      'name': inst.name,
      'ror': ror.index.ror_id
    } ORDER BY pos
  ) AS institutions
FROM datacite_index.works AS dw, UNNEST(dw.institutions) WITH ORDINALITY AS item(inst, pos)
LEFT JOIN ror.index ON inst.affiliation_identifier = ror.index.identifier
GROUP BY dw.doi;
