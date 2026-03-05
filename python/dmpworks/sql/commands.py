from importlib.util import find_spec
import pathlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from sqlmesh.core.console import configure_console
from sqlmesh.core.context import Context
from sqlmesh.core.plan import Plan
from sqlmesh.core.test import ModelTextTestResult
from sqlmesh.utils import Verbosity


def sqlmesh_dir(module_name: str = "dmpworks.sql") -> pathlib.Path:
    """Locate the directory of the SQLMesh module.

    Args:
        module_name: The name of the module to locate.

    Returns:
        The path to the module's directory.

    Raises:
        ModuleNotFoundError: If the module cannot be found.
    """
    spec = find_spec(module_name)
    if spec is None or not spec.origin:
        raise ModuleNotFoundError(module_name)

    return Path(spec.origin).parent


def run_plan() -> Plan | None:
    """Run the SQLMesh plan command.

    Configures the console and executes a plan in the 'prod' environment.

    Returns:
        The executed Plan object or None.
    """
    configure_console(ignore_warnings=False)
    ctx = Context(
        paths=[sqlmesh_dir()],
        load=True,
    )

    try:
        return ctx.plan(
            environment="prod",
            run=True,
            ignore_cron=True,
            auto_apply=True,
            no_prompts=True,
        )
    finally:
        # Need to close otherwise DuckDB database is not closed properly
        # and we end up with a db.db.wal and views not always written out
        ctx.close()


def run_test() -> ModelTextTestResult | None:
    """Run SQLMesh tests.

    Configures the console and runs tests with very verbose output.

    Returns:
        The result of the tests or None.
    """
    configure_console(ignore_warnings=False)
    ctx = Context(
        paths=[sqlmesh_dir()],
        load=True,
    )
    try:
        test_results: ModelTextTestResult = ctx.test(verbosity=Verbosity.VERY_VERBOSE)
        return test_results
    finally:
        # Need to close otherwise DuckDB database is not closed properly
        # and we end up with a db.db.wal and views not always written out
        ctx.close()


def init_doi_state(file_path: pathlib.Path):
    """Initialize a DOI state Parquet file with an empty table.

    Creates a Parquet file with a specific schema:
    - doi: string
    - hash: string
    - state: string
    - updated_date: date32

    Args:
        file_path: The path where the Parquet file should be created.
    """
    schema = pa.schema(
        [
            ("doi", pa.string()),
            ("hash", pa.string()),
            ("state", pa.string()),
            ("updated_date", pa.date32()),
        ]
    )
    table = pa.Table.from_batches([], schema)
    pq.write_table(table, file_path)
