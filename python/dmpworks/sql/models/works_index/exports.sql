/*
  works_index.exports:

  Exports the works index to Parquet files to the export_path specified in
  config.yaml. Updates the works_index.exports with the export date.
*/

MODEL (
  name works_index.exports,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key export_date
  ),
  columns (
    export_date TIMESTAMP
  ),
  depends_on (datacite_index.datacite_index, openalex_index.openalex_index), -- must manually specify these as they are not used within the query itself
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Record export date
SELECT @end_ds AS export_date;

-- Export data
@IF(
  @runtime_stage = 'creating', -- https://sqlmesh.readthedocs.io/en/stable/concepts/macros/macro_variables/#runtime-variables
  COPY (
    SELECT
      *
    FROM datacite_index.datacite_index

    UNION ALL

    SELECT
      *
    FROM openalex_index.openalex_index
  ) TO @VAR('export_path') (FORMAT PARQUET, OVERWRITE true, FILE_SIZE_BYTES '100MB', FILENAME_PATTERN 'export_')
)