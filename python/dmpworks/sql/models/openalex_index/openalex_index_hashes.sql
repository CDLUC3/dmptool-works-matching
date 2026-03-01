/*
  openalex_index.openalex_index_hashes:

  Creates the OpenAlex index hashes table.
*/

MODEL (
  name openalex_index.openalex_index_hashes,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('openalex_index_openalex_index_hashes_threads') AS INT64);

SELECT
  doi,
  -- Content Hash
  -- Exclude doi: as we pair the content hash with DOI
  -- Exclude updated_date: as updated_date could change but the data we use might not.
  -- The hash is dependent on the order of keys and json_object doesn't provide
  -- a way to sort keys, the tradeoff of using Python is speed.
  md5(
    CAST({
      'title': title,
      'abstract_text': abstract_text,
      'work_type': work_type,
      'publication_date': publication_date,
      'publication_venue': publication_venue,
      'institutions': institutions,
      'authors': authors,
      'funders': funders,
      'awards': awards,
      'relations': relations,
      'source': source
    } AS VARCHAR)) AS hash
FROM openalex_index.openalex_index
