/*
  datacite_index.work_types:

  Maps raw DataCite resourceTypeGeneral values to a normalized work_type
  that is a superset of both DataCite and OpenAlex types.

  1. Merge similar DataCite types:
       ComputationalNotebook + Software: Software
       Film + Audiovisual: AudioVisual
       Instrument + PhysicalObject: PhysicalObject
       Model: Dataset (OpenAlex classifies this as dataset)

  2. Map DataCite article-like types to the OpenAlex article type:
       ConferencePaper, DataPaper, JournalArticle, Text: article.

  3. Map types that have a direct OpenAlex equivalent:
       Book, BookChapter, Dataset, Dissertation, PeerReview, Preprint, Report, Standard.

  4. For DataCite types that OpenAlex maps to "other", preserve the original
     type where it may be useful for the DMP Tool:
       Event, Image, InteractiveResource, OutputManagementPlan, Sound.

  5. Map remaining types to OTHER — either uncommon types or container types
     (e.g. Collection, Journal, ConferenceProceeding) that OpenAlex maps to other:
       Award, Collection, ConferenceProceeding, Journal, Other, Project,
       Service, StudyRegistration, Workflow.

  All work_types are then converted to upper snake case.
*/

MODEL (
  name datacite_index.work_types,
  dialect duckdb,
  kind FULL,
  audits (
    unique_values(columns := (doi))
  ),
  enabled true
);

PRAGMA threads=CAST(@VAR('datacite_index_work_types_threads') AS INT64);

 -- Mapping table to create a superset
WITH work_type_map AS (
  SELECT * FROM (VALUES
    ('Audiovisual', 'AUDIO_VISUAL'), -- OpenAlex other.
    ('Award', 'OTHER'), -- OpenAlex other.
    ('Book', 'BOOK'), -- OpenAlex book.
    ('BookChapter', 'BOOK_CHAPTER'), -- OpenAlex book-chapter.
    ('Collection', 'OTHER'), -- OpenAlex other.
    ('ComputationalNotebook', 'SOFTWARE'), -- OpenAlex other. Merged with Software.
    ('ConferencePaper', 'ARTICLE'), -- OpenAlex other. Merged with article.
    ('ConferenceProceeding', 'OTHER'), -- OpenAlex other.
    ('DataPaper', 'ARTICLE'), -- OpenAlex article. Merged with article.
    ('Dataset', 'DATASET'), -- OpenAlex dataset.
    ('Dissertation', 'DISSERTATION'), -- OpenAlex dissertation.
    ('Event', 'EVENT'), -- OpenAlex other.
    ('Film', 'AUDIO_VISUAL'), -- OpenAlex article. Merged with Audiovisual.
    ('Image', 'IMAGE'),  -- OpenAlex other.
    ('Instrument', 'PHYSICAL_OBJECT'), -- OpenAlex other. Merged with PhysicalObject.
    ('InteractiveResource', 'INTERACTIVE_RESOURCE'), -- OpenAlex other.
    ('Journal', 'OTHER'), -- OpenAlex other.
    ('JournalArticle', 'ARTICLE'), -- OpenAlex article. Merged with article.
    ('List of nomenclatural and taxonomic changes for the New Zealand flora.', 'OTHER'), -- OpenAlex other.
    ('Model', 'DATASET'), -- OpenAlex dataset. Merged with Dataset.
    ('Other', 'OTHER'), -- OpenAlex other.
    ('OutputManagementPlan', 'OUTPUT_MANAGEMENT_PLAN'), -- OpenAlex other.
    ('PeerReview', 'PEER_REVIEW'), -- OpenAlex peer-review.
    ('PhysicalObject', 'PHYSICAL_OBJECT'), -- OpenAlex other.
    ('Preprint', 'PREPRINT'), -- OpenAlex preprint.
    ('Project', 'OTHER'), -- OpenAlex other.
    ('Report', 'REPORT'), -- OpenAlex report.
    ('Service', 'OTHER'), -- OpenAlex other.
    ('Software', 'SOFTWARE'), -- OpenAlex other.
    ('Sound', 'SOUND'), -- OpenAlex other.
    ('Standard', 'STANDARD'), -- OpenAlex standard.
    ('StudyRegistration', 'OTHER'), -- OpenAlex other.
    ('Text', 'ARTICLE'), -- OpenAlex article. Merged with article.
    ('Workflow', 'OTHER') -- OpenAlex other.
    ) AS t(original_type, normalized_type)
)

SELECT
  doi,
  work_type_map.normalized_type AS work_type
FROM datacite_index.works dw
INNER JOIN work_type_map ON dw.work_type = work_type_map.original_type;