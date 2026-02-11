/*
  openalex_index.openalex_index:

  Creates the OpenAlex index table.
*/

MODEL (
  name openalex_index.openalex_index,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('openalex_index_openalex_index_threads') AS INT64);


WITH works_index AS (
  SELECT
    works.doi,
    openalex_index.titles.title,
    openalex_index.abstracts.abstract AS abstract_text,
    COALESCE(UPPER(REPLACE(works.type, '-', '_')), 'OTHER') AS work_type,
    works.publication_date,
    openalex_index.updated_dates.updated_date,
    works.publication_venue,
    works.institutions,
    works.authors,
    COALESCE(openalex_index.funders.funders, []) AS funders,
    COALESCE(openalex_index.awards.awards, []) AS awards,
    {name := 'OpenAlex', url := 'https://openalex.org/works/' || owm.id} AS source
  FROM openalex_index.works_metadata AS owm
  LEFT JOIN openalex.works works ON owm.id = works.id
  LEFT JOIN openalex_index.titles ON owm.doi = openalex_index.titles.doi
  LEFT JOIN openalex_index.abstracts ON owm.doi = openalex_index.abstracts.doi
  LEFT JOIN openalex_index.updated_dates ON owm.doi = openalex_index.updated_dates.doi
  LEFT JOIN openalex_index.awards ON owm.doi = openalex_index.awards.doi
  LEFT JOIN openalex_index.funders ON owm.id = openalex_index.funders.id
  WHERE owm.is_primary_doi = TRUE
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
