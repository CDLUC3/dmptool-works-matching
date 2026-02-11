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

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

-- Make a dummy query
SELECT CAST(@VAR('process_works_run_id') AS DATE) AS export_date;

-- Export data
@IF(
  @runtime_stage = 'creating', -- https://sqlmesh.readthedocs.io/en/stable/concepts/macros/macro_variables/#runtime-variables
  COPY (

    SELECT
      d.*
    FROM datacite_index.datacite_index d
    INNER JOIN opensearch.next_doi_state nds ON nds.doi = d.doi AND nds.hash = d.hash AND nds.state = 'UPSERT' AND nds.updated_date = CAST(@VAR('process_works_run_id') AS DATE)

    UNION ALL

    SELECT
      o.*
    FROM openalex_index.openalex_index o
    INNER JOIN opensearch.next_doi_state nds ON nds.doi = o.doi AND nds.hash = o.hash AND nds.state = 'UPSERT' AND nds.updated_date = CAST(@VAR('process_works_run_id') AS DATE)

  ) TO @VAR('works_index_export_path') (FORMAT PARQUET, OVERWRITE true, FILE_SIZE_BYTES '500MB', FILENAME_PATTERN 'works_index_')
)