/*
  works_index.export:

  Exports the works index to Parquet files to the works_index_export_path specified in
  config.yaml.
*/

MODEL (
  name works_index.export,
  kind FULL,
  columns (
    export_date DATE
  ),
  depends_on (datacite_index.datacite_index, openalex_index.openalex_index, opensearch.next_doi_state), -- must manually specify these as they are not used within the query itself
  enabled true
);

PRAGMA threads=CAST(@VAR('works_index_export_threads') AS INT64);

-- Make a dummy query
SELECT CAST(@VAR('process_works_run_id') AS DATE) AS export_date;

-- Export data
@IF(
  @runtime_stage = 'creating', -- https://sqlmesh.readthedocs.io/en/stable/concepts/macros/macro_variables/#runtime-variables
  COPY (
    WITH target_upserts AS (
      SELECT doi, hash
      FROM opensearch.next_doi_state
      WHERE state = 'UPSERT' AND updated_date = CAST(@VAR('process_works_run_id') AS DATE)
    )

    SELECT
      d.*,
      tu.hash
    FROM target_upserts tu
    INNER JOIN datacite_index.datacite_index d ON tu.doi = d.doi

    UNION ALL BY NAME

    SELECT
      o.*,
      tu.hash
    FROM target_upserts tu
    INNER JOIN openalex_index.openalex_index o ON tu.doi = o.doi

  ) TO @VAR('works_index_export_path') (FORMAT PARQUET, OVERWRITE true, FILE_SIZE_BYTES '500MB', FILENAME_PATTERN 'works_index_')
)