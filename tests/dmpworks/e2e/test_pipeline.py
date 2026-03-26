"""End-to-end pipeline tests: source JSONL -> parquet transform -> SQLMesh -> output verification."""

import re

import pytest
import pyarrow.parquet as pq


class TestTransformOutputs:
    """Verify that the three transforms produce valid parquet files from fixture data."""

    @pytest.mark.parametrize(
        "fixture,expected_rows",
        [
            ("openalex_parquet_dir", 10),
            ("crossref_parquet_dir", 7),
            ("datacite_parquet_dir", 47),
        ],
        ids=["openalex", "crossref", "datacite"],
    )
    def test_transform_produces_expected_rows(self, fixture, expected_rows, request):
        """Each transform should produce parquet files with the correct number of rows."""
        parquet_dir = request.getfixturevalue(fixture)
        files = list(parquet_dir.glob("*.parquet"))
        assert len(files) >= 1
        total = sum(pq.read_metadata(f).num_rows for f in files)
        assert total == expected_rows

    @pytest.mark.parametrize(
        "fixture",
        ["openalex_parquet_dir", "crossref_parquet_dir", "datacite_parquet_dir"],
        ids=["openalex", "crossref", "datacite"],
    )
    def test_dois_are_normalized(self, fixture, request):
        """Transform output DOIs should be bare, lowercase identifiers matching the DOI pattern."""
        parquet_dir = request.getfixturevalue(fixture)
        table = pq.read_table(parquet_dir, columns=["doi"])
        dois = table.column("doi").to_pylist()
        doi_re = re.compile(r"^10\.[\d.]+/[^\s]+$")
        bad = [d for d in dois if not doi_re.match(d) or d != d.lower()]
        assert not bad, f"DOIs should be lowercase and match DOI pattern: {bad[:3]}"


class TestSQLMeshOutputs:
    """Verify that SQLMesh produces expected output parquets after processing transform outputs."""

    def test_doi_state_contains_expected_dois(self, sqlmesh_outputs):
        """doi_state export should contain entries for DOIs from OpenAlex and DataCite sources."""
        table = pq.read_table(sqlmesh_outputs["doi_state_export"])
        dois = set(table.column("doi").to_pylist())
        assert "10.5281/zenodo.10381316" in dois, "Expected OpenAlex-sourced DOI in doi_state"
        assert "10.5281/zenodo.5117892" in dois, "Expected DataCite-sourced DOI in doi_state"

    def test_doi_state_all_records_are_upserts(self, sqlmesh_outputs):
        """On a fresh run with empty previous state, all DOI states should be UPSERT."""
        table = pq.read_table(sqlmesh_outputs["doi_state_export"])
        states = set(table.column("state").to_pylist())
        assert states == {"UPSERT"}

    def test_works_index_contains_expected_dois(self, sqlmesh_outputs):
        """works_index export should contain DOIs from both OpenAlex and DataCite index pipelines."""
        table = pq.read_table(sqlmesh_outputs["works_index_export"])
        dois = set(table.column("doi").to_pylist())
        assert "10.5281/zenodo.10381316" in dois, "Expected OpenAlex-sourced DOI in works_index"
        assert "10.5281/zenodo.5117892" in dois, "Expected DataCite-sourced DOI in works_index"

    def test_works_index_has_relations(self, sqlmesh_outputs):
        """Relations from Crossref, DataCite, and Data Citation Corpus should flow through to works_index."""
        table = pq.read_table(sqlmesh_outputs["works_index_export"])
        relations = table.column("relations").to_pylist()
        has_intra = any(r["intra_work_dois"] for r in relations if r)
        has_shared = any(r["possible_shared_project_dois"] for r in relations if r)
        has_dcc = any(r["dataset_citation_dois"] for r in relations if r)
        assert has_intra, "Expected at least one work with intra_work_dois (Crossref/DataCite relations)"
        assert has_shared, "Expected at least one work with possible_shared_project_dois (Crossref/DataCite relations)"
        assert has_dcc, "Expected at least one work with dataset_citation_dois (Data Citation Corpus)"
