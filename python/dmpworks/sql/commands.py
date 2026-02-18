import pathlib
from importlib.util import find_spec
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from sqlmesh.core.console import configure_console
from sqlmesh.core.context import Context
from sqlmesh.core.plan import Plan
from sqlmesh.core.test import ModelTextTestResult
from sqlmesh.utils import Verbosity


def sqlmesh_dir(module_name: str = "dmpworks.sql") -> pathlib.Path:
    spec = find_spec(module_name)
    if spec is None or not spec.origin:
        raise ModuleNotFoundError(module_name)

    return Path(spec.origin).parent


def run_plan() -> Plan:
    configure_console(ignore_warnings=False)
    ctx = Context(
        paths=[sqlmesh_dir()],
        load=True,
    )
    plan = ctx.plan(
        environment="prod",
        run=True,
        ignore_cron=True,
        auto_apply=True,
        no_prompts=True,
    )
    return plan


def run_test() -> ModelTextTestResult:
    configure_console(ignore_warnings=False)
    ctx = Context(
        paths=[sqlmesh_dir()],
        load=True,
    )
    test_results: ModelTextTestResult = ctx.test(verbosity=Verbosity.VERY_VERBOSE)
    return test_results


def init_doi_state(file_path: pathlib.Path):
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
