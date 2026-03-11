/*
  datacite_index.datacite_index:

  Creates the DataCite index table.
*/

MODEL (
  name datacite_index.datacite_index,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi)),
    assert_max_institutions_length(column := institutions, threshold := CAST(@VAR('audit_nested_object_limit') AS INT64), blocking := false),
    assert_max_authors_length(column := authors, threshold := CAST(@VAR('audit_nested_object_limit') AS INT64), blocking := false),
    assert_max_funders_length(column := funders, threshold := CAST(@VAR('audit_nested_object_limit') AS INT64), blocking := false),
    assert_max_awards_length(column := awards, threshold := CAST(@VAR('audit_nested_object_limit') AS INT64), blocking := false),
    assert_max_intra_work_dois_length(column := relations.intra_work_dois, threshold := CAST(@VAR('audit_nested_object_limit') AS INT64), blocking := false),
    assert_max_possible_shared_project_dois_length(column := relations.possible_shared_project_dois, threshold := CAST(@VAR('audit_nested_object_limit') AS INT64), blocking := false),
    assert_max_dataset_citation_dois_length(column := relations.dataset_citation_dois, threshold := CAST(@VAR('audit_nested_object_limit') AS INT64), blocking := false)
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('datacite_index_datacite_index_threads') AS INT64);

SELECT
  dw.doi,
  dw.title,
  dw.abstract AS abstract_text,
  COALESCE(datacite_index.work_types.work_type, 'OTHER') AS work_type,
  dw.publication_date,
  datacite_index.updated_dates.updated_date,
  dw.publication_venue,
  COALESCE(datacite_index.institutions.institutions, []) AS institutions,
  dw.authors,
  COALESCE(datacite_index.funders.funders, []) AS funders,
  COALESCE(datacite_index.awards.awards, []) AS awards,
  {
    'intra_work_dois': COALESCE(ri.intra_work_dois, []),
    'possible_shared_project_dois': COALESCE(ri.possible_shared_project_dois, []),
    'dataset_citation_dois': COALESCE(ri.dataset_citation_dois, [])
  } AS relations,
  {
    'name': 'DataCite',
    'url': 'https://commons.datacite.org/doi.org/' || dw.doi
  } AS source
FROM datacite_index.works dw
LEFT JOIN datacite_index.work_types ON dw.doi = datacite_index.work_types.doi
LEFT JOIN datacite_index.updated_dates ON dw.doi = datacite_index.updated_dates.doi
LEFT JOIN datacite_index.institutions ON dw.doi = datacite_index.institutions.doi
LEFT JOIN datacite_index.funders ON dw.doi = datacite_index.funders.doi
LEFT JOIN datacite_index.awards ON dw.doi = datacite_index.awards.doi
LEFT JOIN relations.relations_index ri ON dw.doi = ri.doi
