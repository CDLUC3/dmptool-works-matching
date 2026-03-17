/*
  relations.crossref_metadata_degrees:

  Counts the number of in and out degrees for each relation type.
*/

MODEL (
  name relations.crossref_metadata_degrees,
  dialect duckdb,
  kind FULL,
  enabled true
);

PRAGMA threads=CAST(@VAR('relations_crossref_metadata_degrees_threads') AS INT64);


WITH unnested_relations AS (
  SELECT DISTINCT
    cm.doi AS work_doi,
    r.relation_id AS related_doi,
    r.relation_type
  FROM crossref.crossref_metadata cm, UNNEST(cm.relations) AS item(r)
  WHERE cm.doi IS NOT NULL AND r.relation_id IS NOT NULL AND r.id_type = 'doi' AND cm.doi <> r.relation_id
  -- DOIs and id_type normalised during transformations so can be relied on here
)

SELECT
  work_doi,
  related_doi,
  relation_type,

  -- For one specific work_doi, how many different related_dois with a specific relation_type does it point out to?
  COUNT(*) OVER (PARTITION BY work_doi, relation_type) AS out_degree,

  -- For one specific related_doi, how many different work_dois with a specific relation_type are pointing in at it?
  COUNT(*) OVER (PARTITION BY related_doi, relation_type) AS in_degree
FROM unnested_relations;