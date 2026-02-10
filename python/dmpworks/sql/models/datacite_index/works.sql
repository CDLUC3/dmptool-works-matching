/*
  datacite_index.works:

  The DataCite data file contains a small number of duplicate records, so we
  pick the record with the most recent update date. QUALIFY allows us to filter
  on a window function: https://duckdb.org/docs/stable/sql/query_syntax/qualify.html.
*/

MODEL (
  name datacite_index.works,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT works.*
FROM datacite.datacite works
QUALIFY ROW_NUMBER() OVER (PARTITION BY doi ORDER BY updated_date DESC NULLS LAST) = 1;
