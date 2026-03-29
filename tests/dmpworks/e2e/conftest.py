"""Session-scoped fixtures for e2e pipeline tests.

Orchestrates: plain-text fixtures -> gzip compress -> transform to parquet -> SQLMesh plan -> output parquets.
"""

import gzip
import os
import pathlib
import shutil
from unittest.mock import patch

import pytest

from tests.utils import get_fixtures_path

E2E_FIXTURES = get_fixtures_path() / "e2e" / "source"


def compress_to_gz(*, src: pathlib.Path, dst: pathlib.Path) -> None:
    """Gzip-compress a file.

    Args:
        src: Source file.
        dst: Destination .gz file path.
    """
    with open(src, "rb") as f_in, gzip.open(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


@pytest.fixture(scope="session")
def e2e_workspace(tmp_path_factory):
    """Create the root temp directory for the entire e2e test session."""
    return tmp_path_factory.mktemp("e2e")


@pytest.fixture(scope="session")
def compressed_sources(e2e_workspace):
    """Compress plain-text fixture files to the gzip formats the pipeline expects.

    Returns:
        Dict with keys: openalex, crossref, datacite, ror, dcc — each a Path to the
        directory containing the compressed file(s).
    """
    dirs = {}

    # Extension must match each transform's glob pattern (see get_file_glob in dataset_subset.py)
    oa_dir = e2e_workspace / "source" / "openalex"
    oa_dir.mkdir(parents=True)
    compress_to_gz(src=E2E_FIXTURES / "openalex" / "works.jsonl", dst=oa_dir / "works.gz")
    dirs["openalex"] = oa_dir

    cr_dir = e2e_workspace / "source" / "crossref"
    cr_dir.mkdir(parents=True)
    compress_to_gz(src=E2E_FIXTURES / "crossref" / "metadata.jsonl", dst=cr_dir / "metadata.jsonl.gz")
    dirs["crossref"] = cr_dir

    dc_dir = e2e_workspace / "source" / "datacite"
    dc_dir.mkdir(parents=True)
    compress_to_gz(src=E2E_FIXTURES / "datacite" / "datacite.jsonl", dst=dc_dir / "datacite.jsonl.gz")
    dirs["datacite"] = dc_dir

    # ROR and DCC are read directly by SQLMesh via DuckDB's read_json_auto(*.json.gz)
    ror_dir = e2e_workspace / "source" / "ror"
    ror_dir.mkdir(parents=True)
    compress_to_gz(src=E2E_FIXTURES / "ror" / "ror.json", dst=ror_dir / "ror.json.gz")
    dirs["ror"] = ror_dir

    dcc_dir = e2e_workspace / "source" / "data_citation_corpus"
    dcc_dir.mkdir(parents=True)
    compress_to_gz(src=E2E_FIXTURES / "data_citation_corpus" / "dcc.json", dst=dcc_dir / "dcc.json.gz")
    dirs["dcc"] = dcc_dir

    return dirs


@pytest.fixture(scope="session")
def openalex_parquet_dir(e2e_workspace, compressed_sources):
    """Run the OpenAlex transform on compressed fixtures, producing parquet output."""
    from dmpworks.transform.openalex_works import transform_openalex_works

    out = e2e_workspace / "parquet" / "openalex_works"
    out.mkdir(parents=True)
    transform_openalex_works(
        in_dir=compressed_sources["openalex"],
        out_dir=out,
        batch_size=1,
        row_group_size=100,
        row_groups_per_file=1,
        max_workers=1,
    )
    return out


@pytest.fixture(scope="session")
def crossref_parquet_dir(e2e_workspace, compressed_sources):
    """Run the Crossref Metadata transform on compressed fixtures, producing parquet output."""
    from dmpworks.transform.crossref_metadata import transform_crossref_metadata

    out = e2e_workspace / "parquet" / "crossref_metadata"
    out.mkdir(parents=True)
    transform_crossref_metadata(
        in_dir=compressed_sources["crossref"],
        out_dir=out,
        batch_size=1,
        row_group_size=100,
        row_groups_per_file=1,
        max_workers=1,
    )
    return out


@pytest.fixture(scope="session")
def datacite_parquet_dir(e2e_workspace, compressed_sources):
    """Run the DataCite transform on compressed fixtures, producing parquet output."""
    from dmpworks.transform.datacite import transform_datacite

    out = e2e_workspace / "parquet" / "datacite"
    out.mkdir(parents=True)
    transform_datacite(
        in_dir=compressed_sources["datacite"],
        out_dir=out,
        batch_size=1,
        row_group_size=100,
        row_groups_per_file=1,
        max_workers=1,
    )
    return out


@pytest.fixture(scope="session")
def doi_state_prev_dir(e2e_workspace):
    """Create a directory with an empty doi_state parquet to serve as the 'previous run' state.

    The file is named doi_state_0.parquet to match the glob pattern
    ``doi_state_[0-9]*.parquet`` used by the opensearch.current_doi_state model.
    """
    from dmpworks.sql.commands import init_doi_state

    d = e2e_workspace / "doi_state_prev"
    d.mkdir(parents=True)
    init_doi_state(d / "doi_state_0.parquet")
    return d


THREAD_VARS = [
    "CROSSREF_CROSSREF_METADATA_THREADS",
    "CROSSREF_INDEX_WORKS_METADATA_THREADS",
    "DATA_CITATION_CORPUS_THREADS",
    "DATACITE_DATACITE_THREADS",
    "DATACITE_INDEX_AWARDS_THREADS",
    "DATACITE_INDEX_DATACITE_INDEX_HASHES_THREADS",
    "DATACITE_INDEX_DATACITE_INDEX_THREADS",
    "DATACITE_INDEX_FUNDERS_THREADS",
    "DATACITE_INDEX_INSTITUTIONS_THREADS",
    "DATACITE_INDEX_UPDATED_DATES_THREADS",
    "DATACITE_INDEX_WORK_TYPES_THREADS",
    "DATACITE_INDEX_WORKS_THREADS",
    "OPENALEX_INDEX_ABSTRACT_STATS_THREADS",
    "OPENALEX_INDEX_ABSTRACTS_THREADS",
    "OPENALEX_INDEX_AUTHOR_NAMES_THREADS",
    "OPENALEX_INDEX_AWARDS_THREADS",
    "OPENALEX_INDEX_FUNDERS_THREADS",
    "OPENALEX_INDEX_OPENALEX_INDEX_HASHES_THREADS",
    "OPENALEX_INDEX_OPENALEX_INDEX_THREADS",
    "OPENALEX_INDEX_PUBLICATION_DATES_THREADS",
    "OPENALEX_INDEX_TITLE_STATS_THREADS",
    "OPENALEX_INDEX_TITLES_THREADS",
    "OPENALEX_INDEX_UPDATED_DATES_THREADS",
    "OPENALEX_INDEX_WORKS_METADATA_THREADS",
    "OPENALEX_OPENALEX_WORKS_THREADS",
    "OPENSEARCH_CURRENT_DOI_STATE_THREADS",
    "OPENSEARCH_EXPORT_THREADS",
    "OPENSEARCH_NEXT_DOI_STATE_THREADS",
    "RELATIONS_CROSSREF_METADATA_DEGREES_THREADS",
    "RELATIONS_CROSSREF_METADATA_THREADS",
    "RELATIONS_DATA_CITATION_CORPUS_THREADS",
    "RELATIONS_DATACITE_DEGREES_THREADS",
    "RELATIONS_DATACITE_THREADS",
    "RELATIONS_RELATIONS_INDEX_THREADS",
    "ROR_INDEX_THREADS",
    "ROR_ROR_THREADS",
    "WORKS_INDEX_EXPORT_THREADS",
]


@pytest.fixture(scope="session")
def sqlmesh_env(
    e2e_workspace,
    openalex_parquet_dir,
    crossref_parquet_dir,
    datacite_parquet_dir,
    compressed_sources,
    doi_state_prev_dir,
):
    """Build the complete dict of environment variables required by SQLMesh config.yaml.

    Returns:
        Dict mapping env var names to string values.
    """
    works_index_export = e2e_workspace / "export" / "works_index"
    works_index_export.mkdir(parents=True)
    doi_state_export = e2e_workspace / "export" / "doi_state"
    doi_state_export.mkdir(parents=True)
    duckdb_path = e2e_workspace / "e2e_test.duckdb"

    env = {
        # Data paths (parquet outputs from transforms)
        "OPENALEX_WORKS_PATH": str(openalex_parquet_dir),
        "CROSSREF_METADATA_PATH": str(crossref_parquet_dir),
        "DATACITE_PATH": str(datacite_parquet_dir),
        # JSON.GZ sources read directly by SQLMesh
        "ROR_PATH": str(compressed_sources["ror"]),
        "DATA_CITATION_CORPUS_PATH": str(compressed_sources["dcc"]),
        # Export paths
        "WORKS_INDEX_EXPORT_PATH": str(works_index_export),
        "DOI_STATE_EXPORT_PATH": str(doi_state_export),
        "DOI_STATE_EXPORT_PREV_PATH": str(doi_state_prev_dir),
        # DuckDB
        "DUCKDB_DATABASE": str(duckdb_path),
        "DUCKDB_THREADS": "1",
        "DUCKDB_MEMORY_LIMIT": "256MB",
        # Audits — threshold 0 means number_of_rows audit always passes
        "AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD": "0",
        "AUDIT_DATACITE_WORKS_THRESHOLD": "0",
        "AUDIT_OPENALEX_WORKS_THRESHOLD": "0",
        "AUDIT_NESTED_OBJECT_LIMIT": "1000",
        # Other settings
        "MAX_DOI_STATES": "10",
        "RUN_ID_SQLMESH": "20240615T060000-a1b2c3d4",
        "RELEASE_DATE_PROCESS_WORKS": "2024-06-15",
        "MAX_RELATION_DEGREES": "3",
    }

    # All per-model thread variables set to 1
    for var in THREAD_VARS:
        env[var] = "1"

    return env


@pytest.fixture(scope="session")
def sqlmesh_outputs(sqlmesh_env):
    """Set environment variables, run SQLMesh plan, and return the export directory paths.

    Returns:
        Dict with keys: works_index_export, doi_state_export — each a Path to the
        directory containing the output parquet file(s).
    """
    from dmpworks.sql.commands import run_plan

    with patch.dict(os.environ, sqlmesh_env):
        run_plan()

    return {
        "works_index_export": pathlib.Path(sqlmesh_env["WORKS_INDEX_EXPORT_PATH"]),
        "doi_state_export": pathlib.Path(sqlmesh_env["DOI_STATE_EXPORT_PATH"]),
    }
