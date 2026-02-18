/*
  relations.crossref_metadata:

  Crossref Metadata relations between DOIs grouped into higher level types.
*/

MODEL (
  name relations.crossref_metadata,
  dialect duckdb,
  kind FULL,
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- https://www.crossref.org/documentation/schema-library/markup-guide-metadata-segments/relationships/

-- intra-work relations that imply the same work:
-- Expression: is-expression-of, has-expression
-- Format: is-format-of, has-format
-- Identical: is-identical-to
-- Manifestation: is-manifestation-of, has-manifestation
-- Manuscript: is-manuscript-of, has-manuscript
-- Preprint: is-preprint-of, has-preprint
-- Replacement: is-replaced-by, replaces
-- Translation: is-translation-of, has-translation
-- Variant: is-variant-form-of, is-original-form-of
-- Version: is-version-of

-- inter-work relations that imply a possible shared origin
-- Basis: is-based-on, is-basis-for
-- Continuation: is-continued-by, continues
-- Derivation: is-derived-from, has-derivation
-- Documentation: is-documented-by, documents
-- Part: is-part-of, has-part
-- Related material: is-related-material, has-related-material
-- Software compilation: is-compiled-by, compiles
-- Supplement: is-supplement-to, is-supplemented

WITH relations_with_dois AS (
  SELECT
    @extract_doi(r.work_doi) AS work_doi,
    @extract_doi(r.relation_id) AS related_doi,
    r.relation_type,
    CASE
      WHEN relation_type IN ('is-expression-of', 'has-expression', 'is-format-of', 'has-format', 'is-identical-to',
                             'is-manifestation-of', 'has-manifestation', 'is-manuscript-of', 'has-manuscript',
                             'is-preprint-of', 'has-preprint', 'is-replaced-by', 'replaces', 'is-translation-of',
                             'has-translation', 'is-variant-form-of', 'is-original-form-of', 'is-version-of') THEN TRUE
      ELSE FALSE
    END AS is_intra_work,
    CASE
      WHEN relation_type IN ('is-derived-from', 'has-derivation', 'is-basis-for', 'is-based-on', 'is-supplement-to', 'is-supplemented-by',
                             'documents', 'is-documented-by', 'has-part', 'is-part-of', 'continues', 'is-continued-by',
                             'compiles', 'is-compiled-by', 'is-related-material', 'has-related-material') THEN TRUE
      ELSE FALSE
    END AS is_possible_shared_project
  FROM crossref_metadata.relations r
  -- Don't filter on id_type = 'DOI', sometimes id_type is not 'doi' but contains DOIs
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
