import os
from pathlib import Path
from typing import Annotated, Literal

from cyclopts import App, Group, Parameter
from dotenv import load_dotenv

from dmpworks.batch.cli import app as batch_app
from dmpworks.batch_submit.cli import app as batch_submit_app
from dmpworks.dmsp.cli import app as dmsp_app
from dmpworks.opensearch.cli import app as opensearch_app
from dmpworks.sql.cli import app as sqlmesh_app
from dmpworks.transform.cli import app as transform_app

DOTENV_FILES = {
    "local": ".env.local",
    "aws": ".env.aws",
}

DEFAULT_ENV = "local"
ENV_VAR_NAME = "DMPWORKS_ENV"
ENV_FILE_VAR_NAME = "DMPWORKS_ENV_FILE"

cli = App(name="dmpworks", help="DMP Tool Related Works Command Line Tool.")
cli.meta.group_parameters = Group("Runtime Options", sort_key=0)

cli.command(opensearch_app)
cli.command(batch_app)
cli.command(sqlmesh_app)
cli.command(transform_app)
cli.command(dmsp_app)
cli.command(batch_submit_app)


def load_environment(*, env: Literal["local", "aws"] | str | None, env_file: str | Path | None = None) -> None:
    """Load environment variables for the selected runtime or explicit dotenv file.

    Precedence:
        1. Explicit env_file argument (e.g. --env-file)
        2. DMPWORKS_ENV_FILE environment variable
        3. Named environment mapped via env / DMPWORKS_ENV
    """
    dotenv_path: str | Path

    if env_file is not None:
        dotenv_path = env_file
    else:
        env_file_from_var = os.getenv(ENV_FILE_VAR_NAME)
        if env_file_from_var:
            dotenv_path = env_file_from_var
        else:
            try:
                dotenv_path = DOTENV_FILES[env]
            except KeyError as e:
                valid = ", ".join(sorted(DOTENV_FILES))
                raise ValueError(f"Unknown env {env!r}. Expected one of: {valid}.") from e

    load_dotenv(dotenv_path=dotenv_path, override=True)


@cli.meta.default
def main(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    env: Annotated[
        Literal["local", "aws"] | None,
        Parameter(
            name="--env",
            help="Environment name to load a default .env file (e.g. local -> .env.local, aws -> .env.aws).",
        ),
    ] = "local",
    env_file: Annotated[
        Path | None,
        Parameter(name="--env-file", help="Path to a dotenv file to load."),
    ] = None,
) -> None:
    """Entry point for the dmpworks command line interface.

    Loads environment variables and invokes the CLI application.
    """
    resolved_env = env or os.getenv(ENV_VAR_NAME, DEFAULT_ENV)
    load_environment(env=resolved_env, env_file=env_file)
    cli(tokens)


if __name__ == "__main__":
    cli.meta()
