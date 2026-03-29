import logging
import os
import pathlib

from dmpworks.batch.utils import download_files_from_s3, local_path, s3_uri, upload_files_to_s3
from dmpworks.batch_submit.job_factories import PROCESS_WORKS_SQLMESH
from dmpworks.cli_utils import RunIdentifiers, SQLMeshConfig
from dmpworks.sql.commands import run_plan

log = logging.getLogger(__name__)


def plan(
    *,
    bucket_name: str,
    run_identifiers: RunIdentifiers,
    sqlmesh_config: SQLMeshConfig,
):
    """Run the SQLMesh plan.

    Args:
        bucket_name: DMP Tool S3 bucket name.
        run_identifiers: the release dates of each dataset.
        sqlmesh_config: the SQLMesh config.
    """
    # Download Parquet files for each dataset from S3 and set env vars for datasets.
    datasets = [
        ("crossref-metadata", run_identifiers.crossref_metadata, "transform", "CROSSREF_METADATA_PATH"),
        ("data-citation-corpus", run_identifiers.data_citation_corpus, "download", "DATA_CITATION_CORPUS_PATH"),
        ("datacite", run_identifiers.datacite, "transform", "DATACITE_PATH"),
        ("openalex-works", run_identifiers.openalex_works, "transform", "OPENALEX_WORKS_PATH"),
        ("ror", run_identifiers.ror, "download", "ROR_PATH"),
    ]
    for dataset, run_id, phase, env_var_name in datasets:
        # Download
        source_uri = s3_uri(bucket_name, f"{dataset}-{phase}", run_id, "*")
        download_dir = local_path(f"{dataset}-{phase}", run_id)
        download_files_from_s3(source_uri, download_dir)

        # Set env var
        os.environ[env_var_name] = str(download_dir)

    # Download previous DOI state
    doi_state_export_prev_dir = (
        pathlib.Path("/data") / "sqlmesh" / run_identifiers.run_id_sqlmesh_prev / "doi_state_export"
    )
    target_uri = s3_uri(bucket_name, PROCESS_WORKS_SQLMESH, run_identifiers.run_id_sqlmesh_prev, "doi_state_export/*")
    download_files_from_s3(target_uri, doi_state_export_prev_dir)
    os.environ["DOI_STATE_EXPORT_PREV_PATH"] = str(doi_state_export_prev_dir)

    # Create other paths and directories
    sqlmesh_data_dir = pathlib.Path("/data") / "sqlmesh" / run_identifiers.run_id_sqlmesh
    duckdb_dir = sqlmesh_data_dir / "duckdb" / "db.db"
    works_index_export_dir = sqlmesh_data_dir / "works_index_export"
    doi_state_export_dir = sqlmesh_data_dir / "doi_state_export"
    duckdb_dir.parent.mkdir(parents=True, exist_ok=True)
    works_index_export_dir.mkdir(parents=True, exist_ok=True)
    doi_state_export_dir.mkdir(parents=True, exist_ok=True)
    os.environ["WORKS_INDEX_EXPORT_PATH"] = str(works_index_export_dir)
    os.environ["DOI_STATE_EXPORT_PATH"] = str(doi_state_export_dir)

    # Remaining SQLMesh settings
    os.environ["RUN_ID_SQLMESH"] = str(run_identifiers.run_id_sqlmesh)
    os.environ["RELEASE_DATE_PROCESS_WORKS"] = str(run_identifiers.release_date_process_works)
    os.environ["DUCKDB_DATABASE"] = str(duckdb_dir)
    os.environ["DUCKDB_THREADS"] = str(sqlmesh_config.duckdb_threads)
    os.environ["DUCKDB_MEMORY_LIMIT"] = str(sqlmesh_config.duckdb_memory_limit)
    os.environ["AUDIT_CROSSREF_METADATA_WORKS_THRESHOLD"] = str(sqlmesh_config.audit_crossref_metadata_works_threshold)
    os.environ["AUDIT_DATACITE_WORKS_THRESHOLD"] = str(sqlmesh_config.audit_datacite_works_threshold)
    os.environ["AUDIT_NESTED_OBJECT_LIMIT"] = str(sqlmesh_config.audit_nested_object_limit)
    os.environ["AUDIT_OPENALEX_WORKS_THRESHOLD"] = str(sqlmesh_config.audit_openalex_works_threshold)
    os.environ["MAX_DOI_STATES"] = str(sqlmesh_config.max_doi_states)
    os.environ["MAX_RELATION_DEGREES"] = str(sqlmesh_config.max_relation_degrees)
    os.environ["CROSSREF_CROSSREF_METADATA_THREADS"] = str(sqlmesh_config.crossref_crossref_metadata)
    os.environ["CROSSREF_INDEX_WORKS_METADATA_THREADS"] = str(sqlmesh_config.crossref_index_works_metadata)
    os.environ["DATA_CITATION_CORPUS_THREADS"] = str(sqlmesh_config.data_citation_corpus_relations)
    os.environ["DATACITE_DATACITE_THREADS"] = str(sqlmesh_config.datacite_datacite)
    os.environ["DATACITE_INDEX_AWARDS_THREADS"] = str(sqlmesh_config.datacite_index_awards)
    os.environ["DATACITE_INDEX_DATACITE_INDEX_HASHES_THREADS"] = str(
        sqlmesh_config.datacite_index_datacite_index_hashes
    )
    os.environ["DATACITE_INDEX_DATACITE_INDEX_THREADS"] = str(sqlmesh_config.datacite_index_datacite_index)
    os.environ["DATACITE_INDEX_FUNDERS_THREADS"] = str(sqlmesh_config.datacite_index_funders)
    os.environ["DATACITE_INDEX_INSTITUTIONS_THREADS"] = str(sqlmesh_config.datacite_index_institutions)
    os.environ["DATACITE_INDEX_UPDATED_DATES_THREADS"] = str(sqlmesh_config.datacite_index_updated_dates)
    os.environ["DATACITE_INDEX_WORK_TYPES_THREADS"] = str(sqlmesh_config.datacite_index_work_types)
    os.environ["DATACITE_INDEX_WORKS_THREADS"] = str(sqlmesh_config.datacite_index_works)
    os.environ["OPENALEX_INDEX_ABSTRACT_STATS_THREADS"] = str(sqlmesh_config.openalex_index_abstract_stats)
    os.environ["OPENALEX_INDEX_ABSTRACTS_THREADS"] = str(sqlmesh_config.openalex_index_abstracts)
    os.environ["OPENALEX_INDEX_AUTHOR_NAMES_THREADS"] = str(sqlmesh_config.openalex_index_author_names)
    os.environ["OPENALEX_INDEX_AWARDS_THREADS"] = str(sqlmesh_config.openalex_index_awards)
    os.environ["OPENALEX_INDEX_FUNDERS_THREADS"] = str(sqlmesh_config.openalex_index_funders)
    os.environ["OPENALEX_INDEX_OPENALEX_INDEX_HASHES_THREADS"] = str(
        sqlmesh_config.openalex_index_openalex_index_hashes
    )
    os.environ["OPENALEX_INDEX_OPENALEX_INDEX_THREADS"] = str(sqlmesh_config.openalex_index_openalex_index)
    os.environ["OPENALEX_INDEX_PUBLICATION_DATES_THREADS"] = str(sqlmesh_config.openalex_index_publication_dates)
    os.environ["OPENALEX_INDEX_TITLE_STATS_THREADS"] = str(sqlmesh_config.openalex_index_title_stats)
    os.environ["OPENALEX_INDEX_TITLES_THREADS"] = str(sqlmesh_config.openalex_index_titles)
    os.environ["OPENALEX_INDEX_UPDATED_DATES_THREADS"] = str(sqlmesh_config.openalex_index_updated_dates)
    os.environ["OPENALEX_INDEX_WORKS_METADATA_THREADS"] = str(sqlmesh_config.openalex_index_works_metadata)
    os.environ["OPENALEX_OPENALEX_WORKS_THREADS"] = str(sqlmesh_config.openalex_openalex_works)
    os.environ["OPENSEARCH_CURRENT_DOI_STATE_THREADS"] = str(sqlmesh_config.opensearch_current_doi_state)
    os.environ["OPENSEARCH_EXPORT_THREADS"] = str(sqlmesh_config.opensearch_export)
    os.environ["OPENSEARCH_NEXT_DOI_STATE_THREADS"] = str(sqlmesh_config.opensearch_next_doi_state)
    os.environ["RELATIONS_CROSSREF_METADATA_DEGREES_THREADS"] = str(sqlmesh_config.relations_crossref_metadata_degrees)
    os.environ["RELATIONS_CROSSREF_METADATA_THREADS"] = str(sqlmesh_config.relations_crossref_metadata)
    os.environ["RELATIONS_DATA_CITATION_CORPUS_THREADS"] = str(sqlmesh_config.relations_data_citation_corpus)
    os.environ["RELATIONS_DATACITE_DEGREES_THREADS"] = str(sqlmesh_config.relations_datacite_degrees)
    os.environ["RELATIONS_DATACITE_THREADS"] = str(sqlmesh_config.relations_datacite)
    os.environ["RELATIONS_RELATIONS_INDEX_THREADS"] = str(sqlmesh_config.relations_relations_index)
    os.environ["ROR_INDEX_THREADS"] = str(sqlmesh_config.ror_index)
    os.environ["ROR_ROR_THREADS"] = str(sqlmesh_config.ror_ror)
    os.environ["WORKS_INDEX_EXPORT_THREADS"] = str(sqlmesh_config.works_index_export)

    # Run SQL Mesh
    run_plan()

    # Upload exported Parquet files
    sql_mesh_s3_uri = f"s3://{bucket_name}/{PROCESS_WORKS_SQLMESH}/{run_identifiers.run_id_sqlmesh}/"
    upload_files_to_s3(sqlmesh_data_dir, sql_mesh_s3_uri, "*")
