AUDIT (
  name assert_max_institutions_length
);
SELECT *
FROM @this_model
WHERE array_length(@column) > @threshold;

AUDIT (
  name assert_max_authors_length
);
SELECT *
FROM @this_model
WHERE array_length(@column) > @threshold;

AUDIT (
  name assert_max_funders_length
);
SELECT *
FROM @this_model
WHERE array_length(@column) > @threshold;

AUDIT (
  name assert_max_awards_length
);
SELECT *
FROM @this_model
WHERE array_length(@column) > @threshold;

AUDIT (
  name assert_max_intra_work_dois_length
);
SELECT *
FROM @this_model
WHERE array_length(@column) > @threshold;

AUDIT (
  name assert_max_possible_shared_project_dois_length
);
SELECT *
FROM @this_model
WHERE array_length(@column) > @threshold;

AUDIT (
  name assert_max_dataset_citation_dois_length
);
SELECT *
FROM @this_model
WHERE array_length(@column) > @threshold;