/*
  relations.datacite_degrees:

  Counts the number of in and out degrees for each relation type.
*/

MODEL (
  name relations.datacite_degrees,
  dialect duckdb,
  kind FULL,
  enabled true
);

PRAGMA threads=CAST(@VAR('relations_datacite_degrees_threads') AS INT64);

WITH unnested_relations AS (
  SELECT
    dc.doi AS work_doi,
    r.related_identifier AS related_doi,
    r.relation_type
  FROM datacite.datacite dc, UNNEST(dc.relations) AS item(r)
  WHERE dc.doi IS NOT NULL AND r.related_identifier IS NOT NULL AND r.related_identifier_type = 'DOI' AND dc.doi <> r.related_identifier
  -- DOIs and related_identifier_type normalised during transformations so can be relied on here
)

SELECT
  work_doi,
  related_doi,
  relation_type,
  COUNT(*) OVER (PARTITION BY work_doi, relation_type) AS out_degree,
  COUNT(*) OVER (PARTITION BY related_doi, relation_type) AS in_degree
FROM unnested_relations;
