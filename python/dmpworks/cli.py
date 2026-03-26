from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from dotenv import load_dotenv

from dmpworks.batch.cli import app as batch_app
from dmpworks.batch_submit.cli import app as batch_submit_app
from dmpworks.dmsp.cli import app as dmsp_app
from dmpworks.opensearch.cli import app as opensearch_app
from dmpworks.pipeline.cli import app as pipeline_app
from dmpworks.sql.cli import app as sqlmesh_app
from dmpworks.transform.cli import app as transform_app

cli = App(name="dmpworks", help="DMP Tool Related Works Command Line Tool.")

cli.command(opensearch_app)
cli.command(batch_app)
cli.command(sqlmesh_app)
cli.command(transform_app)
cli.command(dmsp_app)
cli.command(batch_submit_app)
cli.command(pipeline_app)


@cli.meta.default
def meta(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    env_file: Annotated[
        Path,
        Parameter(
            env_var="DMPWORKS_ENV_FILE",
            help="Path to .env file to load.",
            show_default=True,
        ),
    ] = Path(".env.local"),
) -> None:
    """Load environment variables and dispatch CLI commands.

    Args:
        *tokens: Forwarded CLI tokens.
        env_file: Path to a .env file to load before executing the command.
    """
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=True)
    cli(tokens)


def main() -> None:
    """Entry point for the dmpworks CLI."""
    cli.meta()


if __name__ == "__main__":
    main()
