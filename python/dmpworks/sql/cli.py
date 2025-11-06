from cyclopts import App


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


if __name__ == "__main__":
    app()
