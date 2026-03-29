import logging

from opensearchpy import OpenSearch

from dmpworks.constants import OPENSEARCH_DMPS_INDEX_NAME, OPENSEARCH_WORKS_INDEX_NAME

log = logging.getLogger(__name__)


def upsert_role(*, client: OpenSearch, role_name: str, role_body: dict) -> None:
    """Create or replace an OpenSearch security role.

    Args:
        client: The OpenSearch client.
        role_name: The name of the role to create or replace.
        role_body: The role definition body.
    """
    log.info(f"Upserting role: {role_name}")
    response = client.transport.perform_request(
        "PUT",
        f"/_plugins/_security/api/roles/{role_name}",
        body=role_body,
    )
    log.info(f"Upserted role {role_name}: {response}")


def upsert_role_mapping(*, client: OpenSearch, role_name: str, backend_roles: list[str]) -> None:
    """Create or replace an OpenSearch security role mapping.

    Replaces the entire mapping — any previously configured backend roles are dropped.

    Args:
        client: The OpenSearch client.
        role_name: The name of the role to map.
        backend_roles: The list of backend role ARNs to map to the role.
    """
    log.info(f"Upserting role mapping: {role_name} -> {backend_roles}")
    response = client.transport.perform_request(
        "PUT",
        f"/_plugins/_security/api/rolesmapping/{role_name}",
        body={"backend_roles": backend_roles},
    )
    log.info(f"Upserted role mapping {role_name}: {response}")


def setup_aws_batch_role(
    *,
    client: OpenSearch,
    backend_roles: list[str],
    dmps_index: str = OPENSEARCH_DMPS_INDEX_NAME,
    works_index: str = OPENSEARCH_WORKS_INDEX_NAME,
) -> None:
    """Create or replace the aws_batch role and its backend role mapping.

    The aws_batch role grants AWS Batch jobs permission to perform bulk writes,
    scrolls, and multi-searches, plus full CRUD and index admin on the DMPs and
    works indexes.

    Args:
        client: The OpenSearch client.
        backend_roles: Backend role ARNs to map to aws_batch (e.g. the Batch job role ARN).
        dmps_index: Name of the DMPs index.
        works_index: Name of the works index.
    """
    role_body = {
        "cluster_permissions": [
            "indices:data/write/bulk",
            "indices:data/read/scroll/clear",
            "indices:data/read/scroll",
            "indices:data/read/msearch",
        ],
        "index_permissions": [
            {
                "index_patterns": [dmps_index, works_index],
                "allowed_actions": [
                    "crud",
                    "create_index",
                    "indices:admin/refresh*",
                    "indices:admin/settings/update",
                    "indices:monitor/settings/get",
                ],
            }
        ],
    }
    upsert_role(client=client, role_name="aws_batch", role_body=role_body)
    upsert_role_mapping(client=client, role_name="aws_batch", backend_roles=backend_roles)


def setup_apollo_server_role(
    *,
    client: OpenSearch,
    backend_roles: list[str],
    works_index: str = OPENSEARCH_WORKS_INDEX_NAME,
) -> None:
    """Create or replace the apollo_server role and its backend role mapping.

    The apollo_server role grants Apollo server read-only access to the works index.

    Args:
        client: The OpenSearch client.
        backend_roles: Backend role ARNs to map to apollo_server (e.g. the ECS Task Role ARN).
        works_index: Name of the works index.
    """
    role_body = {
        "index_permissions": [
            {
                "index_patterns": [works_index],
                "allowed_actions": ["read"],
            }
        ],
    }
    upsert_role(client=client, role_name="apollo_server", role_body=role_body)
    upsert_role_mapping(client=client, role_name="apollo_server", backend_roles=backend_roles)


def setup_principal(*, client: OpenSearch, backend_roles: list[str]) -> None:
    """Map backend roles to the built-in all_access and security_manager roles.

    Used to grant the AWS SSO admin principal full access to the cluster on first setup.
    Does not create or modify the role definitions themselves — only the mappings.

    Args:
        client: The OpenSearch client.
        backend_roles: Backend role ARNs to map (e.g. the AWS Reserved SSO role ARN).
    """
    for role_name in ("all_access", "security_manager"):
        upsert_role_mapping(client=client, role_name=role_name, backend_roles=backend_roles)
