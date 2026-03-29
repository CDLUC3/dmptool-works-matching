"""AWS local test environment — DynamoDB Local container, table fixtures, and seeding helpers."""

from __future__ import annotations

import os

import pendulum
import pytest
from testcontainers.core.container import DockerContainer

from dmpworks.model.dataset_version_model import DatasetRelease
from dmpworks.scheduler.dynamodb_store import (
    DatasetReleaseRecord,
    TaskCheckpointRecord,
    TaskRunRecord,
    persist_discovered_release,
)
from tests.utils import wait_for_http

DYNAMODB_LOCAL_IMAGE = "amazon/dynamodb-local:2.5.4"


@pytest.fixture(scope="session")
def dynamodb_local():
    """Start DynamoDB Local container for the test session."""
    container = (
        DockerContainer(DYNAMODB_LOCAL_IMAGE).with_command("-jar DynamoDBLocal.jar -inMemory").with_exposed_ports(8000)
    )
    with container:
        port = container.get_exposed_port(8000)
        endpoint = f"http://localhost:{port}"
        wait_for_http(endpoint)
        DatasetReleaseRecord.Meta.host = endpoint
        TaskRunRecord.Meta.host = endpoint
        TaskCheckpointRecord.Meta.host = endpoint
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
        yield endpoint
        DatasetReleaseRecord.Meta.host = None
        TaskRunRecord.Meta.host = None
        TaskCheckpointRecord.Meta.host = None


@pytest.fixture
def release_table(dynamodb_local):
    """Create the dataset releases table for one test, then drop it."""
    DatasetReleaseRecord.create_table(wait=True)
    yield
    DatasetReleaseRecord.delete_table()


@pytest.fixture
def task_run_table(dynamodb_local):
    """Create the task runs table for one test, then drop it."""
    TaskRunRecord.create_table(wait=True)
    yield
    TaskRunRecord.delete_table()


@pytest.fixture
def task_checkpoint_table(dynamodb_local):
    """Create the task checkpoints table for one test, then drop it."""
    TaskCheckpointRecord.create_table(wait=True)
    yield
    TaskCheckpointRecord.delete_table()


@pytest.fixture
def tables(dynamodb_local):
    """Create all DynamoDB tables for one test, then drop them."""
    DatasetReleaseRecord.create_table(wait=True)
    TaskRunRecord.create_table(wait=True)
    TaskCheckpointRecord.create_table(wait=True)
    yield
    DatasetReleaseRecord.delete_table()
    TaskRunRecord.delete_table()
    TaskCheckpointRecord.delete_table()


def make_release_record(dataset: str, release_date: str) -> DatasetReleaseRecord:
    """Insert a minimal DatasetReleaseRecord with DISCOVERED status.

    Args:
        dataset: The dataset identifier.
        release_date: ISO date string "YYYY-MM-DD".

    Returns:
        The persisted DatasetReleaseRecord.
    """
    return persist_discovered_release(
        dataset=dataset,
        release=DatasetRelease(release_date=pendulum.parse(release_date).date()),
    )
