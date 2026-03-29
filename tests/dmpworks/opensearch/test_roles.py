import pytest
from unittest.mock import MagicMock, call

from dmpworks.opensearch.roles import (
    setup_apollo_server_role,
    setup_aws_batch_role,
    setup_principal,
    upsert_role,
    upsert_role_mapping,
)


BATCH_ARN = "arn:aws:iam::123456789012:role/dmpworks-dev-batch-job-role"
APOLLO_ARN = "arn:aws:iam::123456789012:role/dmpworks-dev-ecs-apollo-EcsTaskRole-abc123"
SSO_ARN = "arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_profile_abc123"


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.transport.perform_request.return_value = {"status": "OK"}
    return client


class TestUpsertRole:
    def test_puts_role_definition(self, mock_client):
        body = {"cluster_permissions": ["indices:data/write/bulk"]}
        upsert_role(client=mock_client, role_name="my_role", role_body=body)

        mock_client.transport.perform_request.assert_called_once_with(
            "PUT",
            "/_plugins/_security/api/roles/my_role",
            body=body,
        )


class TestUpsertRoleMapping:
    def test_puts_backend_roles(self, mock_client):
        upsert_role_mapping(client=mock_client, role_name="my_role", backend_roles=[BATCH_ARN])

        mock_client.transport.perform_request.assert_called_once_with(
            "PUT",
            "/_plugins/_security/api/rolesmapping/my_role",
            body={"backend_roles": [BATCH_ARN]},
        )

    def test_replaces_with_multiple_arns(self, mock_client):
        upsert_role_mapping(client=mock_client, role_name="my_role", backend_roles=[BATCH_ARN, APOLLO_ARN])

        mock_client.transport.perform_request.assert_called_once_with(
            "PUT",
            "/_plugins/_security/api/rolesmapping/my_role",
            body={"backend_roles": [BATCH_ARN, APOLLO_ARN]},
        )


class TestSetupAwsBatchRole:
    def test_upserts_role_and_mapping_with_default_indexes(self, mock_client):
        setup_aws_batch_role(client=mock_client, backend_roles=[BATCH_ARN])

        calls = mock_client.transport.perform_request.call_args_list
        assert len(calls) == 2

        role_call = calls[0]
        assert role_call.args[0] == "PUT"
        assert role_call.args[1] == "/_plugins/_security/api/roles/aws_batch"
        role_body = role_call.kwargs["body"]
        assert "indices:data/write/bulk" in role_body["cluster_permissions"]
        assert "indices:data/read/msearch" in role_body["cluster_permissions"]
        index_perm = role_body["index_permissions"][0]
        assert "works-index" in index_perm["index_patterns"]
        assert "dmps-index" in index_perm["index_patterns"]
        assert "crud" in index_perm["allowed_actions"]
        assert "create_index" in index_perm["allowed_actions"]

        mapping_call = calls[1]
        assert mapping_call.args[0] == "PUT"
        assert mapping_call.args[1] == "/_plugins/_security/api/rolesmapping/aws_batch"
        assert mapping_call.kwargs["body"] == {"backend_roles": [BATCH_ARN]}

    @pytest.mark.parametrize("dmps_index,works_index", [
        ("custom-dmps", "custom-works"),
        ("dmps-staging", "works-staging"),
    ])
    def test_uses_provided_index_names(self, mock_client, dmps_index, works_index):
        setup_aws_batch_role(
            client=mock_client,
            backend_roles=[BATCH_ARN],
            dmps_index=dmps_index,
            works_index=works_index,
        )

        role_body = mock_client.transport.perform_request.call_args_list[0].kwargs["body"]
        index_patterns = role_body["index_permissions"][0]["index_patterns"]
        assert dmps_index in index_patterns
        assert works_index in index_patterns


class TestSetupApolloServerRole:
    def test_upserts_role_and_mapping_with_default_index(self, mock_client):
        setup_apollo_server_role(client=mock_client, backend_roles=[APOLLO_ARN])

        calls = mock_client.transport.perform_request.call_args_list
        assert len(calls) == 2

        role_call = calls[0]
        assert role_call.args[1] == "/_plugins/_security/api/roles/apollo_server"
        role_body = role_call.kwargs["body"]
        index_perm = role_body["index_permissions"][0]
        assert "works-index" in index_perm["index_patterns"]
        assert index_perm["allowed_actions"] == ["read"]

        mapping_call = calls[1]
        assert mapping_call.args[1] == "/_plugins/_security/api/rolesmapping/apollo_server"
        assert mapping_call.kwargs["body"] == {"backend_roles": [APOLLO_ARN]}

    def test_uses_provided_works_index(self, mock_client):
        setup_apollo_server_role(client=mock_client, backend_roles=[APOLLO_ARN], works_index="works-staging")

        role_body = mock_client.transport.perform_request.call_args_list[0].kwargs["body"]
        assert "works-staging" in role_body["index_permissions"][0]["index_patterns"]


class TestSetupPrincipal:
    def test_maps_both_builtin_roles(self, mock_client):
        setup_principal(client=mock_client, backend_roles=[SSO_ARN])

        calls = mock_client.transport.perform_request.call_args_list
        assert len(calls) == 2

        urls = {c.args[1] for c in calls}
        assert "/_plugins/_security/api/rolesmapping/all_access" in urls
        assert "/_plugins/_security/api/rolesmapping/security_manager" in urls

        for c in calls:
            assert c.args[0] == "PUT"
            assert c.kwargs["body"] == {"backend_roles": [SSO_ARN]}

    def test_does_not_upsert_role_definitions(self, mock_client):
        setup_principal(client=mock_client, backend_roles=[SSO_ARN])

        urls = [c.args[1] for c in mock_client.transport.perform_request.call_args_list]
        assert not any("/roles/all_access" == u for u in urls)
        assert not any("/roles/security_manager" == u for u in urls)
