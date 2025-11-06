/*
  datacite_index.types:

  Consolidates DataCite types into a set of types more compatible with OpenAlex
  types.
*/

MODEL (
  name datacite_index.types,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

 -- Mapping table to normalise DataCite types
WITH type_map AS (
  SELECT * FROM (VALUES
    ('Audiovisual', 'AUDIO_VISUAL'),
    ('Award', 'OTHER'),
    ('Book', 'BOOK'),
    ('BookChapter', 'BOOK_CHAPTER'),
    ('Collection', 'COLLECTION'),
    ('ComputationalNotebook', 'SOFTWARE'),
    ('ConferencePaper', 'ARTICLE'),
    ('ConferenceProceeding', 'OTHER'),
    ('DataPaper', 'DATA_PAPER'),
    ('Dataset', 'DATASET'),
    ('Dissertation', 'DISSERTATION'),
    ('Event', 'EVENT'),
    ('Film', 'AUDIO_VISUAL'),
    ('Image', 'IMAGE'),
    ('Instrument', 'PHYSICAL_OBJECT'),
    ('InteractiveResource', 'INTERACTIVE_RESOURCE'),
    ('Journal', 'OTHER'),
    ('JournalArticle', 'ARTICLE'),
    ('List of nomenclatural and taxonomic changes for the New Zealand flora.', 'OTHER'),
    ('Model', 'MODEL'),
    ('Other', 'OTHER'),
    ('OutputManagementPlan', 'OUTPUT_MANAGEMENT_PLAN'),
    ('PeerReview', 'PEER_REVIEW'),
    ('PhysicalObject', 'PHYSICAL_OBJECT'),
    ('Preprint', 'PREPRINT'),
    ('Project', 'OTHER'),
    ('Report', 'REPORT'),
    ('Service', 'SERVICE'),
    ('Software', 'SOFTWARE'),
    ('Sound', 'SOUND'),
    ('Standard', 'STANDARD'),
    ('StudyRegistration', 'OTHER'),
    ('Text', 'TEXT'),
    ('Workflow', 'WORKFLOW')
    ) AS t(original_type, normalized_type)
)

SELECT
  doi,
  type_map.normalized_type AS type
FROM datacite_index.works dw
INNER JOIN type_map ON dw.type = type_map.original_type;