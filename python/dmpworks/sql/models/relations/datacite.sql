/*
  relations.datacite:

  DataCite relations between DOIs grouped into higher level types.
*/

MODEL (
  name relations.datacite,
  dialect duckdb,
  kind FULL,
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);


-- https://support.datacite.org/docs/connecting-to-works
-- https://datacite-metadata-schema.readthedocs.io/en/4.5/appendices/appendix-1/relationType/
--
-- intra-work relations that imply the same work:
-- Identical: IsIdenticalTo
-- Obsoleted: IsObsoletedBy, Obsoletes
-- Part: IsPartOf, HasPart
-- Published In: IsPublishedIn
-- Translation: IsTranslationOf, HasTranslation
-- Variant: IsVariantFormOf, IsOriginalFormOf
-- Version: HasVersion, IsVersionOf, IsNewVersionOf, IsPreviousVersionOf

-- inter-work relations relations that imply a possible shared origin:
-- Compilation: Compiles, IsCompiledBy
-- Continuation: Continues, IsContinuedBy
-- Derivation: IsDerivedFrom, IsSourceOf
-- Description: Describes, IsDescribedBy
-- Documentation: Documents, IsDocumentedBy
-- Supplement: IsSupplementTo, IsSupplementedBy

WITH relations_with_dois AS (
  SELECT
    @extract_doi(dc.doi) AS work_doi,
    @extract_doi(r.related_identifier) AS related_doi,
    r.relation_type,
    CASE
      WHEN r.relation_type IN ('IsIdenticalTo', 'IsObsoletedBy', 'Obsoletes', 'IsPartOf', 'HasPart', 'IsPublishedIn',
                             'IsTranslationOf', 'HasTranslation', 'IsVariantFormOf', 'IsOriginalFormOf',
                             'HasVersion', 'IsVersionOf', 'IsNewVersionOf', 'IsPreviousVersionOf') THEN TRUE
      ELSE FALSE
    END AS is_intra_work,
    CASE
      WHEN r.relation_type IN ('Compiles', 'IsCompiledBy', 'Continues', 'IsContinuedBy', 'IsDerivedFrom', 'IsSourceOf',
                             'Describes', 'IsDescribedBy', 'Documents', 'IsDocumentedBy', 'IsSupplementTo',
                             'IsSupplementedBy') THEN TRUE
      ELSE FALSE
    END AS is_possible_shared_project
  FROM datacite.datacite dc, UNNEST(dc.relations) AS item(r)
  -- Don't filter on related_identifier_type = 'DOI', sometimes related_identifier_type is not 'DOI' but contains DOIs
  -- hence, we use extract_doi instead and check rd.related_doi IS NOT NULL at the end
)

SELECT DISTINCT
  rd.work_doi,
  rd.related_doi,
  rd.relation_type,
  rd.is_intra_work,
  rd.is_possible_shared_project
FROM relations_with_dois rd
WHERE rd.work_doi IS NOT NULL AND rd.related_doi IS NOT NULL AND rd.work_doi <> rd.related_doi
