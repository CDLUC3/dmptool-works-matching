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

PRAGMA threads=CAST(@VAR('relations_datacite_threads') AS INT64);


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
    work_doi,
    related_doi,
    CASE
      WHEN relation_type IN ('IsIdenticalTo', 'IsObsoletedBy', 'Obsoletes', 'IsPartOf', 'HasPart', 'IsPublishedIn',
                             'IsTranslationOf', 'HasTranslation', 'IsVariantFormOf', 'IsOriginalFormOf',
                             'HasVersion', 'IsVersionOf', 'IsNewVersionOf', 'IsPreviousVersionOf') THEN TRUE
      ELSE FALSE
    END AS is_intra_work,
    CASE
      WHEN relation_type IN ('Compiles', 'IsCompiledBy', 'Continues', 'IsContinuedBy', 'IsDerivedFrom', 'IsSourceOf',
                             'Describes', 'IsDescribedBy', 'Documents', 'IsDocumentedBy', 'IsSupplementTo',
                             'IsSupplementedBy') THEN TRUE
      ELSE FALSE
    END AS is_possible_shared_project
  FROM relations.datacite_degrees
  WHERE out_degree <= CAST(@VAR('max_relation_degrees') AS INT64) AND in_degree <= CAST(@VAR('max_relation_degrees') AS INT64)
),

bidirectional AS (
  SELECT
    rd.work_doi,
    rd.related_doi,
    rd.is_intra_work,
    rd.is_possible_shared_project
  FROM relations_with_dois rd
  WHERE rd.work_doi IS NOT NULL AND rd.related_doi IS NOT NULL AND rd.work_doi <> rd.related_doi

  UNION ALL

  SELECT
    rd.related_doi AS work_doi,
    rd.work_doi AS related_doi,
    rd.is_intra_work,
    rd.is_possible_shared_project
  FROM relations_with_dois rd
  WHERE rd.work_doi IS NOT NULL AND rd.related_doi IS NOT NULL AND rd.work_doi <> rd.related_doi
),

collapsed AS (
  SELECT
    work_doi,
    related_doi,
    bool_or(is_intra_work) AS is_intra_work,
    bool_or(is_possible_shared_project) AS is_possible_shared_project
  FROM bidirectional
  WHERE is_intra_work OR is_possible_shared_project
  GROUP BY work_doi, related_doi
)

SELECT
  work_doi AS doi,
  COALESCE(ARRAY_AGG(related_doi) FILTER (WHERE is_intra_work), []) AS intra_work_dois,
  COALESCE(ARRAY_AGG(related_doi) FILTER (WHERE is_possible_shared_project), []) AS possible_shared_project_dois
FROM collapsed
GROUP BY work_doi;
