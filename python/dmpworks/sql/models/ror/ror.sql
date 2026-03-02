MODEL (
  name ror.ror,
  dialect duckdb,
  kind VIEW,
  columns (
    id TEXT,
    external_ids STRUCT(type VARCHAR, all_ids VARCHAR[])[]
  ),
  audits (
    number_of_rows(threshold := 1),
  )
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Rename "all" to "all_ids" as it is a reserved word and seems to interfere with SQLMesh
SELECT
  id,
  list_transform(
    external_ids,
    x -> {
      type: CAST(x.type AS VARCHAR),
      all_ids: CAST(x."all" AS VARCHAR[])
    }
  ) AS external_ids
FROM read_json_auto(@VAR('ror_path') || '/*.json.gz');