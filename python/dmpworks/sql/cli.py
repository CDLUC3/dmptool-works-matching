import pathlib
from typing import Annotated, Optional

from cyclopts import App, Parameter, validators

app = App(name="sqlmesh", help="SQLMesh utilities.")


@app.command(name="test")
def test_cmd():
    """Run SQLMesh plan."""

    # Imported here as SQLMesh prints unnecessary logs in unrelated parts of
    # system if imported globally
    from dmpworks.sql.commands import run_test

    run_test()


@app.command(name="plan")
def plan_cmd():
    """Run SQLMesh tests."""

    # Imported here as SQLMesh prints unnecessary logs in unrelated parts of
    # system if imported globally
    from dmpworks.sql.commands import run_plan

    run_plan()


@app.command(name="init-doi-state")
def init_doi_state(
    parquet_file: Annotated[
        pathlib.Path,
        Parameter(
            validator=validators.Path(
                dir_okay=False,
                file_okay=True,
                exists=False,
            )
        ),
    ],
):
    """Initialises DOI state parquet file.

    Args:
        parquet_file: the path where the parquet file should be saved.

    Returns:

    """

    # Imported here as SQLMesh prints unnecessary logs in unrelated parts of
    # system if imported globally
    from dmpworks.sql.commands import init_doi_state

    init_doi_state(parquet_file)


if __name__ == "__main__":
    app()
