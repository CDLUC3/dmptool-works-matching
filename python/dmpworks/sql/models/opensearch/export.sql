/*
  opensearch.export:

  Exports the opensearch doi state to Parquet files to the doi_state_export_path specified in
  config.yaml.
*/

MODEL (
  name opensearch.export,
  kind FULL,
  columns (
    export_date DATE
  ),
  depends_on (opensearch.next_doi_state), -- must manually specify as they are not used within the query itself
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Make a dummy query
SELECT CAST(@VAR('process_works_run_id') AS DATE) AS export_date;

-- Export data
@IF(
  @runtime_stage = 'creating', -- https://sqlmesh.readthedocs.io/en/stable/concepts/macros/macro_variables/#runtime-variables
  COPY (
    SELECT
      *
    FROM opensearch.next_doi_state
  ) TO @VAR('doi_state_export_path') (FORMAT PARQUET, OVERWRITE true, FILE_SIZE_BYTES '500MB', FILENAME_PATTERN 'doi_state_')
)