from cyclopts import App
from dotenv import load_dotenv

from dmpworks.batch.cli import app as batch_app
from dmpworks.opensearch.cli import app as opensearch_app

from dmpworks.sql.cli import app as sqlmesh_app
from dmpworks.transform.cli import app as transform_app
from dmpworks.dmsp.cli import app as dmsp_app
from dmpworks.batch_submit.cli import app as batch_submit_app

cli = App(name="dmpworks", help="DMP Tool Related Works Command Line Tool.")

cli.command(opensearch_app)
cli.command(batch_app)
cli.command(sqlmesh_app)
cli.command(transform_app)
cli.command(dmsp_app)
cli.command(batch_submit_app)


def main() -> None:
    env = load_dotenv(dotenv_path='.env.batch-submit', override=True)
    cli()


if __name__ == "__main__":
    main()
