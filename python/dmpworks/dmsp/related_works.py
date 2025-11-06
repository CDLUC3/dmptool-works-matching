import json
import logging
import pathlib
import tempfile
from typing import Annotated, Callable
import pymysql.cursors
from cyclopts import App, Parameter, validators
from jsonlines import jsonlines

app = App(name="related-works", help="DMSP related works utilities.")


@app.command(name="load")
def load_related_works_cmd(
    matches_path: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=True,
            )
        ),
    ],
    host: Annotated[
        str,
        Parameter(
            env_var="MYSQL_HOST",
            help="MySQL hostname",
        ),
    ],
    port: Annotated[
        int,
        Parameter(
            env_var="MYSQL_TCP_PORT",
            help="MySQL port",
        ),
    ],
    user: Annotated[
        str,
        Parameter(
            env_var="MYSQL_USER",
            help="MySQL user name",
        ),
    ],
    database: Annotated[
        str,
        Parameter(
            env_var="MYSQL_DATABASE",
            help="MySQL database name",
        ),
    ],
    password: Annotated[
        str,
        Parameter(
            env_var="MYSQL_PWD",
            help="MySQL password",
        ),
    ],
    batch_size: int = 1000,
):
    """Load related works into DMSP database

    Args:
        matches_path: the path to the Related Works matches generated from OpenSearch.
        host: the MYSQL hostname.
        port: the MYSQL port.
        user: the MYSQL user.
        database: the MYSQL database name.
        password: the MYSQL password.
        batch_size: the batch size for loading staging tables.
    """

    logging.basicConfig(level=logging.INFO)

    load_related_works(
        matches_path,
        host,
        port,
        user,
        database,
        password,
        batch_size=batch_size,
    )


def load_related_works(
    matches_path: pathlib.Path,
    host: str,
    port: int,
    user: str,
    database: str,
    password: str,
    batch_size: int,
):
    # Connect to database
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

    with tempfile.TemporaryDirectory() as tmp:
        # Create work versions and related works files
        tmp_dir = pathlib.Path(tmp)
        work_versions_path = tmp_dir / "work_versions.jsonl"
        related_works_path = tmp_dir / "related_works.jsonl"
        create_work_versions(matches_path, work_versions_path)
        create_related_works(matches_path, related_works_path)

        try:
            with conn.cursor() as cursor:
                # Create temp tables
                cursor.callproc("create_related_works_staging_tables")

                # Load work versions
                insert_work_versions(work_versions_path, cursor, batch_size=batch_size)
                print_table(
                    cursor,
                    "stagingWorkVersions",
                    format_func=lambda r: f"doi={r.get('doi')}, hash={r.get('hash').hex()}",
                )

                # Load related works
                insert_related_works(related_works_path, cursor, batch_size=batch_size)
                print_table(
                    cursor,
                    "stagingRelatedWorks",
                    format_func=lambda r: f"dmpDoi={r.get('dmpDoi')}, workDoi={r.get('workDoi')}, hash={r.get('hash').hex()}",
                )

                # Call stored procedure to update
                cursor.callproc("batch_update_related_works")
                print_table(cursor, "works", format_func=lambda r: f"id={r.get('id')}, doi={r.get('doi')}")
                print_table(cursor, "workVersions", format_func=lambda r: f"id={r.get('id')}, title={r.get('title')}")
                print_table(cursor, "relatedWorks", format_func=lambda r: f"id={r.get('id')}")

            conn.commit()
            logging.info("Transaction committed successfully.")

        except pymysql.MySQLError as e:
            logging.error(f"Database error occurred: {e}")
            if conn:
                conn.rollback()
                logging.error("Transaction rolled back.")

        finally:
            if conn:
                conn.close()
                logging.info("Database connection closed.")


def create_work_versions(input_path: pathlib.Path, output_path: pathlib.Path):
    seen = set()
    with jsonlines.open(input_path) as in_file:
        with jsonlines.open(output_path, mode='w') as out_file:
            for row in in_file:
                work = row["work"]
                doi = work["doi"]
                if doi not in seen:
                    out_file.write(to_work_version(work))
                    seen.add(doi)


def to_work_version(row: dict) -> dict:
    return {
        "doi": row["doi"],
        "hash": row["hash"],
        "workType": row["workType"],
        "publicationDate": row["publicationDate"],
        "title": row["title"],
        "abstractText": row["abstractText"],
        "authors": serialise_json(row["authors"]),
        "institutions": serialise_json(row["institutions"]),
        "funders": serialise_json(row["funders"]),
        "awards": serialise_json(row["awards"]),
        "publicationVenue": row["publicationVenue"],
        "sourceName": row["source"]["name"],
        "sourceUrl": row["source"]["url"],
    }


def create_related_works(input_path: pathlib.Path, output_path: pathlib.Path):
    with jsonlines.open(input_path) as in_file:
        with jsonlines.open(output_path, mode='w') as out_file:
            for row in in_file:
                out_file.write(to_related_work(row))


def to_related_work(row: dict) -> dict:
    return {
        "dmpDoi": row["dmpDoi"],
        "workDoi": row["work"]["doi"],
        "hash": row["work"]["hash"],
        "sourceType": "SYSTEM_MATCHED",
        "score": row["score"],
        "scoreMax": row["scoreMax"],
        "doiMatch": serialise_json(row["doiMatch"]),
        "contentMatch": serialise_json(row["contentMatch"]),
        "authorMatches": serialise_json(row["authorMatches"]),
        "institutionMatches": serialise_json(row["institutionMatches"]),
        "funderMatches": serialise_json(row["funderMatches"]),
        "awardMatches": serialise_json(row["awardMatches"]),
    }


def serialise_json(data) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def insert_batch_from_jsonl(
    input_path: pathlib.Path,
    cursor: pymysql.cursors.DictCursor,
    batch_insert_func: Callable,
    row_processor: Callable,
    batch_size: int = 1000,
):
    batch = []
    with jsonlines.open(input_path) as in_file:
        for row in in_file:
            processed_row = row_processor(row)
            batch.append(processed_row)

            if len(batch) >= batch_size:
                batch_insert_func(cursor, batch)
                batch = []

    if len(batch):
        batch_insert_func(cursor, batch)


def insert_work_versions(input_path: pathlib.Path, conn, batch_size: int = 1000):
    def process_row(row):
        return [
            row["doi"],
            bytes.fromhex(row["hash"]),
            row["workType"],
            row["publicationDate"],
            row["title"],
            row["abstractText"],
            row["authors"],
            row["institutions"],
            row["funders"],
            row["awards"],
            row["publicationVenue"],
            row["sourceName"],
            row["sourceUrl"],
        ]

    insert_batch_from_jsonl(
        input_path,
        conn,
        insert_work_versions_batch,
        process_row,
        batch_size,
    )


def insert_related_works(input_path: pathlib.Path, conn, batch_size: int = 1000):
    def process_row(row: dict) -> list:
        return [
            row["dmpDoi"],
            row["workDoi"],
            bytes.fromhex(row["hash"]),
            row["sourceType"],
            row["score"],
            row["scoreMax"],
            row["doiMatch"],
            row["contentMatch"],
            row["authorMatches"],
            row["institutionMatches"],
            row["funderMatches"],
            row["awardMatches"],
        ]

    insert_batch_from_jsonl(
        input_path,
        conn,
        insert_related_works_batch,
        process_row,
        batch_size,
    )


def insert_related_works_batch(cursor: pymysql.cursors.DictCursor, data: list):
    sql = "INSERT INTO stagingRelatedWorks (dmpDoi,workDoi,hash,sourceType,score,scoreMax,doiMatch,contentMatch,authorMatches,institutionMatches,funderMatches,awardMatches) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql, data)


def insert_work_versions_batch(cursor: pymysql.cursors.DictCursor, data: list):
    sql = "INSERT INTO stagingWorkVersions (doi,hash,workType,publicationDate,title,abstractText,authors,institutions,funders,awards,publicationVenue,sourceName,sourceUrl) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sql, data)


def print_table(cursor: pymysql.cursors.DictCursor, table_name: str, format_func: Callable, limit: int = 10):
    logging.info(f"Table: {table_name}")
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    results = cursor.fetchall()
    for row in results:
        logging.info(format_func(row))


if __name__ == "__main__":
    app()
