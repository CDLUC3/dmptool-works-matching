MODEL (
  name ror.index,
  dialect duckdb,
  kind FULL,
  audits (
    number_of_rows(threshold := 1),
  )
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Map ROR IDs to ROR IDs so that ID lookups also resolve ROR IDs
WITH ror_ids AS (
  SELECT DISTINCT
    @normalise_identifier(r.id) AS ror_id,
    'ror' AS type,
    @normalise_identifier(r.id) AS identifier
  FROM ror.ror r
),

-- Unnest external_ids.all which contains a list of all external identifiers of a given type
other_ids_intermediate AS (
  SELECT
    @normalise_identifier(r.id) AS ror_id,
    ext.type AS type,
    ident AS identifier
  FROM ror.ror r,
  UNNEST(external_ids) AS item1(ext),
  UNNEST(ext.all_ids) AS item2(ident)
),

-- Filter and transform other identifiers
other_ids AS (
  SELECT
    ror_id,
    type,
    CASE
      WHEN type = 'isni' THEN @normalise_isni(identifier)
      WHEN type = 'fundref' THEN '10.13039/' || LOWER(TRIM(identifier))
      ELSE LOWER(TRIM(identifier))
    END AS identifier
  FROM other_ids_intermediate
  WHERE type IS NOT NULL OR identifier IS NOT NULL
)

-- Combine into single table
SELECT * FROM ror_ids
UNION ALL
SELECT * FROM other_ids;