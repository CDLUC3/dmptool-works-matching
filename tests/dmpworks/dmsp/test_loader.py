"""Integration tests for RelatedWorksLoader using a real MySQL container."""

import json

import pymysql
import pymysql.cursors
import pytest

from dmpworks.dmsp.loader import RelatedWorksLoader


def make_work_version_row(doi="10.0000/work1", work_hash="aa" * 16):
    """Build a minimal work version row list matching the staging INSERT columns."""
    return [
        doi,
        bytes.fromhex(work_hash),
        "article",
        "2023-01-01",
        "Test Title",
        "Abstract text",
        json.dumps([]),
        json.dumps([]),
        json.dumps([]),
        json.dumps([]),
        None,
        "OpenAlex",
        "https://openalex.org",
    ]


def make_related_work_row(plan_id=1, work_doi="10.0000/work1", work_hash="aa" * 16):
    """Build a minimal related work row list matching the staging INSERT columns."""
    return [
        plan_id,
        work_doi,
        bytes.fromhex(work_hash),
        "SYSTEM_MATCHED",
        0.8,
        1.0,
        "PENDING",
        json.dumps({"found": False, "score": 0.0, "sources": []}),
        json.dumps({"score": 0.0, "titleHighlight": None, "abstractHighlights": []}),
        json.dumps([]),
        json.dumps([]),
        json.dumps([]),
        json.dumps([]),
    ]


def count_rows(conn, table):
    """Return the row count for a table."""
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table}")  # noqa: S608
        return cursor.fetchone()["cnt"]


class TestRelatedWorksLoader:
    def test_insert_work_versions(self, mysql_conn):
        """Work version rows land in the staging table."""
        loader = RelatedWorksLoader(mysql_conn)
        loader.prepare_staging_tables()

        loader.insert_work_versions([make_work_version_row()], batch_size=100)

        assert count_rows(mysql_conn, "stagingWorkVersions") == 1

    def test_insert_related_works(self, mysql_conn):
        """Related work rows land in the staging table."""
        loader = RelatedWorksLoader(mysql_conn)
        loader.prepare_staging_tables()

        loader.insert_related_works([make_related_work_row()], batch_size=100)

        assert count_rows(mysql_conn, "stagingRelatedWorks") == 1

    def test_prepare_staging_tables_clears_previous_data(self, mysql_conn):
        """Calling prepare_staging_tables re-creates the temp tables, clearing old data."""
        loader = RelatedWorksLoader(mysql_conn)
        loader.prepare_staging_tables()
        loader.insert_work_versions([make_work_version_row()], batch_size=100)
        assert count_rows(mysql_conn, "stagingWorkVersions") == 1

        loader.prepare_staging_tables()

        assert count_rows(mysql_conn, "stagingWorkVersions") == 0

    def test_commit_persists_across_cycles(self, mysql_conn):
        """Data from committed cycles persists in the works table across multiple cycles."""
        loader = RelatedWorksLoader(mysql_conn)

        # Cycle 1
        loader.prepare_staging_tables()
        loader.insert_work_versions([make_work_version_row(doi="10.0000/work1")], batch_size=100)
        loader.run_update_procedure(system_matched=True)
        loader.commit()

        # Cycle 2
        loader.prepare_staging_tables()
        loader.insert_work_versions([make_work_version_row(doi="10.0000/work2", work_hash="bb" * 16)], batch_size=100)
        loader.run_update_procedure(system_matched=True)
        loader.commit()

        assert count_rows(mysql_conn, "works") == 2
        assert count_rows(mysql_conn, "workVersions") == 2

    def test_rollback_on_error(self, mysql_conn):
        """Committed data survives when a later cycle fails and is rolled back."""
        loader = RelatedWorksLoader(mysql_conn)

        # Cycle 1: commit successfully
        loader.prepare_staging_tables()
        loader.insert_work_versions([make_work_version_row(doi="10.0000/survivor")], batch_size=100)
        loader.run_update_procedure(system_matched=True)
        loader.commit()

        # Cycle 2: insert then rollback (simulating error recovery)
        loader.prepare_staging_tables()
        loader.insert_work_versions([make_work_version_row(doi="10.0000/doomed", work_hash="cc" * 16)], batch_size=100)
        loader.run_update_procedure(system_matched=True)
        mysql_conn.rollback()

        # Cycle 1 data survived, cycle 2 data did not
        assert count_rows(mysql_conn, "works") == 1
        with mysql_conn.cursor() as cursor:
            cursor.execute("SELECT doi FROM works")
            assert cursor.fetchone()["doi"] == "10.0000/survivor"
