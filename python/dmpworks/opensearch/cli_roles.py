import logging
from typing import Annotated

from cyclopts import App, Parameter

from dmpworks.cli_utils import LogLevel, OpenSearchClientConfig
from dmpworks.constants import OPENSEARCH_DMPS_INDEX_NAME, OPENSEARCH_WORKS_INDEX_NAME

app = App(name="roles", help="Manage OpenSearch security roles and role mappings.")


@app.command(name="aws-batch")
def aws_batch_cmd(
    backend_roles: Annotated[list[str], Parameter(consume_multiple=True)],
    dmps_index: str = OPENSEARCH_DMPS_INDEX_NAME,
    works_index: str = OPENSEARCH_WORKS_INDEX_NAME,
    client_config: OpenSearchClientConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Create or replace the aws_batch role and map backend role ARNs to it.

    Sets cluster permissions (bulk write, scroll, msearch) and index permissions
    (CRUD, create_index, admin/refresh, settings) on the DMPs and works indexes.
    Replaces any existing role definition and backend role mapping.

    Args:
        backend_roles: One or more backend role ARNs to map to aws_batch (e.g. the Batch job role ARN).
        dmps_index: Name of the DMPs index.
        works_index: Name of the works index.
        client_config: OpenSearch client settings.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.roles import setup_aws_batch_role
    from dmpworks.opensearch.utils import make_opensearch_client

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)

    client = make_opensearch_client(client_config)
    setup_aws_batch_role(client=client, backend_roles=backend_roles, dmps_index=dmps_index, works_index=works_index)


@app.command(name="apollo-server")
def apollo_server_cmd(
    backend_roles: Annotated[list[str], Parameter(consume_multiple=True)],
    works_index: str = OPENSEARCH_WORKS_INDEX_NAME,
    client_config: OpenSearchClientConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Create or replace the apollo_server role and map backend role ARNs to it.

    Sets read-only index permissions on the works index.
    Replaces any existing role definition and backend role mapping.

    Args:
        backend_roles: One or more backend role ARNs to map to apollo_server (e.g. the ECS Task Role ARN).
        works_index: Name of the works index.
        client_config: OpenSearch client settings.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.roles import setup_apollo_server_role
    from dmpworks.opensearch.utils import make_opensearch_client

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)

    client = make_opensearch_client(client_config)
    setup_apollo_server_role(client=client, backend_roles=backend_roles, works_index=works_index)


@app.command(name="principal")
def principal_cmd(
    backend_roles: Annotated[list[str], Parameter(consume_multiple=True)],
    client_config: OpenSearchClientConfig | None = None,
    log_level: LogLevel = "INFO",
):
    """Map backend role ARNs to the built-in all_access and security_manager roles.

    Used on first cluster setup to grant the AWS SSO admin principal full access.
    Only updates the role mappings — does not modify the role definitions themselves.
    Replaces any existing backend role mapping.

    Args:
        backend_roles: One or more backend role ARNs to map (e.g. the AWS Reserved SSO role ARN).
        client_config: OpenSearch client settings.
        log_level: Python log level (e.g., INFO).
    """
    from dmpworks.opensearch.roles import setup_principal
    from dmpworks.opensearch.utils import make_opensearch_client

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)

    client = make_opensearch_client(client_config)
    setup_principal(client=client, backend_roles=backend_roles)


if __name__ == "__main__":
    app()
