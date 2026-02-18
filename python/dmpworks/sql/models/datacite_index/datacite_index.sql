/*
  datacite_index.datacite_index:

  Creates the DataCite index table.
*/

MODEL (
  name datacite_index.datacite_index,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

WITH works_index AS (
  SELECT
    dw.doi,
    dw.title,
    dw.abstract AS abstract_text,
    COALESCE(datacite_index.types.type, 'OTHER') AS work_type,
    dw.publication_date,
    datacite_index.updated_dates.updated_date,
    dw.publication_venue,
    COALESCE(datacite_index.institutions.institutions, []) AS institutions,
    dw.authors,
    COALESCE(datacite_index.funders.funders, []) AS funders,
    COALESCE(datacite_index.awards.awards, []) AS awards,
    {name := 'DataCite', url := 'https://commons.datacite.org/doi.org/' || dw.doi} AS source
  FROM datacite_index.works dw
  LEFT JOIN datacite_index.types ON dw.doi = datacite_index.types.doi
  LEFT JOIN datacite_index.updated_dates ON dw.doi = datacite_index.updated_dates.doi
  LEFT JOIN datacite_index.institutions ON dw.doi = datacite_index.institutions.doi
  LEFT JOIN datacite_index.funders ON dw.doi = datacite_index.funders.doi
  LEFT JOIN datacite_index.awards ON dw.doi = datacite_index.awards.doi
)

SELECT
  *,
  -- Content Hash
  -- Exclude doi: as we pair the content hash with DOI
  -- Exclude updated_date: as updated_date could change but the data we use might not.
  -- The hash is dependent on the order of keys and json_object doesn't provide
  -- a way to sort keys, the tradeoff of using Python is speed.
  md5(
    json_object(
      'title', title,
      'abstract_text', abstract_text,
      'work_type', work_type,
      'publication_date', publication_date,
      'publication_venue', publication_venue,
      'institutions', institutions,
      'authors', authors,
      'funders', funders,
      'awards', awards,
      'source', source
  )::VARCHAR) AS hash
FROM works_index
