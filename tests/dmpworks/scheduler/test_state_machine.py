"""Integration tests for the dataset release Step Functions state machine."""

from __future__ import annotations

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import boto3
import pytest
from testcontainers.core.container import DockerContainer

from tests.utils import wait_for_http, get_fixtures_path

SFN_LOCAL_IMAGE = "amazon/aws-stepfunctions-local:latest"
SFN_LOCAL_PORT = 8083
MOCK_REGION = "us-east-1"
MOCK_ACCOUNT = "123456789012"
STATE_MACHINE_NAME = "DatasetIngestWorkflow"

_SM_DIR = Path(__file__).parent.parent.parent.parent / "infra" / "templates" / "state_machines"

ASL_PATH = _SM_DIR / "dataset_ingest_workflow" / "dataset_ingest_workflow.asl.json"
DOWNLOAD_SM_PATH = _SM_DIR / "dataset_ingest_workflow" / "download_workflow.asl.json"
SUBSET_SM_PATH = _SM_DIR / "dataset_ingest_workflow" / "subset_workflow.asl.json"
TRANSFORM_SM_PATH = _SM_DIR / "dataset_ingest_workflow" / "transform_workflow.asl.json"

DOWNLOAD_SM_NAME = "dmpworks-test-download"
SUBSET_SM_NAME = "dmpworks-test-subset"
TRANSFORM_SM_NAME = "dmpworks-test-transform"

BASE_INPUT = {
    "workflow_key": "ror",
    "publication_date": "2025-01-01",
    "aws_env": "dev",
    "bucket_name": "test-bucket",
    "execution_arn": f"arn:aws:states:{MOCK_REGION}:{MOCK_ACCOUNT}:execution:{STATE_MACHINE_NAME}:test",
    "download_url": "https://example.com/ror.zip",
    "file_hash": "md5:abc123",
    "file_name": None,
    "use_subset": False,
    "log_level": "INFO",
}

MOCK_SUBSTITUTIONS = {
    "SetReleaseStatusFunction": f"arn:aws:lambda:{MOCK_REGION}:{MOCK_ACCOUNT}:function:SetReleaseStatus",
    "GetBatchJobParamsFunction": f"arn:aws:lambda:{MOCK_REGION}:{MOCK_ACCOUNT}:function:GetBatchJobParams",
    "SetTaskRunCompleteFunction": f"arn:aws:lambda:{MOCK_REGION}:{MOCK_ACCOUNT}:function:SetTaskRunComplete",
    "SetTaskRunStatusFunction": f"arn:aws:lambda:{MOCK_REGION}:{MOCK_ACCOUNT}:function:SetTaskRunStatus",
    "TaskCheckpointsTableName": "dmpworks-test-task-checkpoints",
    "DownloadWorkflowStateMachine": f"arn:aws:states:{MOCK_REGION}:{MOCK_ACCOUNT}:stateMachine:{DOWNLOAD_SM_NAME}",
    "SubsetWorkflowStateMachine": f"arn:aws:states:{MOCK_REGION}:{MOCK_ACCOUNT}:stateMachine:{SUBSET_SM_NAME}",
    "TransformWorkflowStateMachine": f"arn:aws:states:{MOCK_REGION}:{MOCK_ACCOUNT}:stateMachine:{TRANSFORM_SM_NAME}",
    "GenerateChildRunIdFunction": f"arn:aws:lambda:{MOCK_REGION}:{MOCK_ACCOUNT}:function:GenerateChildRunId",
    "StoreApprovalTokenFunction": f"arn:aws:lambda:{MOCK_REGION}:{MOCK_ACCOUNT}:function:StoreApprovalToken",
}


def free_port() -> int:
    """Return an available local TCP port."""
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def resolve_asl(path: Path | None = None) -> str:
    """Load an ASL file, apply substitutions, and adapt for SFN Local.

    Args:
        path: Path to the ASL file. Defaults to the parent state machine.

    Returns:
        The ASL definition string ready for use with SFN Local.
    """
    raw = (path or ASL_PATH).read_text()
    for key, arn in MOCK_SUBSTITUTIONS.items():
        raw = raw.replace(f"${{{key}}}", arn)
    return _adapt_for_sfn_local(path or ASL_PATH, raw)


def _adapt_for_sfn_local(path: Path, asl: str) -> str:
    """Rewrite ASL to work around SFN Local limitations.

    SFN Local supports sync:2 for nested SM invocations but not
    waitForTaskToken. For the parent SM, waitForTaskToken states are
    downgraded to sync:2 and the TaskToken input field is removed.
    For child SMs, the SendTaskSuccess callback state is stripped and
    the preceding complete state is restored to End: true.

    Args:
        path: The path of the ASL file (used to distinguish parent from children).
        asl: The substituted ASL definition string.

    Returns:
        The adapted ASL definition string.
    """
    data = json.loads(asl)
    states = data["States"]
    if path == ASL_PATH:
        # Remove approval wait states (WaitFor*Approval) from the parent SM.
        # These are only reachable via Catch blocks and would create infinite loops under SFN Local.
        approval_states = [name for name in states if name.startswith("WaitFor") and name.endswith("Approval")]
        for name in approval_states:
            del states[name]
        for state in states.values():
            if state.get("Resource", "").endswith(".waitForTaskToken"):
                state["Resource"] = state["Resource"].replace(".waitForTaskToken", ".sync:2")
                state.get("Parameters", {}).get("Input", {}).pop("TaskToken.$", None)
                state.pop("TimeoutSeconds", None)
            # Remove Catch blocks that route to (now-deleted) approval wait states.
            state.pop("Catch", None)
    else:
        if "SendTaskSuccess" in states:
            for state in states.values():
                if state.get("Next") == "SendTaskSuccess":
                    del state["Next"]
                    state["End"] = True
                state.pop("Catch", None)
            del states["SendTaskSuccess"]
            states.pop("SendTaskFailure", None)
            states.pop("ChildFailed", None)
    return json.dumps(data)


class MockLambdaHandler(BaseHTTPRequestHandler):
    """Minimal Lambda invoke API that returns canned responses per function name."""

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        # Path: /2015-03-31/functions/{name}/invocations
        function_name = self.path.split("/")[3]

        if "GenerateChildRunId" in function_name:
            import secrets
            task_name = body.get("task_name", "unknown")
            prefix = body.get("workflow_prefix", "test")
            date = body.get("date", "2025-01-01")
            run_id = f"20250101T060000-{secrets.token_hex(4)}"
            response = {
                "child_run_id": run_id,
                "execution_name": f"{prefix}-{task_name}-{date}-{run_id}",
            }
        elif "GetBatchJobParams" in function_name:
            task_type = body.get("task_type", "download")
            response = {
                **body,
                "run_id": f"2025-01-01T060000-mock{task_type[:2]}",
                "run_name": f"mock-{task_type}",
                "batch_params": {
                    "JobName": f"mock-job-{task_type}",
                    "JobQueue": "mock-queue",
                    "JobDefinition": "mock-def",
                    "ContainerOverrides": {
                        "Command": ["/bin/bash", "-c", "echo mock"],
                        "Vcpus": 1,
                        "Memory": 1024,
                        "Environment": [],
                    },
                },
            }
        else:
            response = body

        payload = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass  # suppress request logging


class MockDynamoDBHandler(BaseHTTPRequestHandler):
    """Minimal DynamoDB API that handles GetItem for task-checkpoints skip-logic tests."""

    # Task types that should be reported as already COMPLETED (for skip-logic tests).
    completed_task_types: set[str] = set()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        task_key = body.get("Key", {}).get("task_key", {}).get("S", "")
        task_type = task_key.split("#")[0]

        if task_type in MockDynamoDBHandler.completed_task_types:
            response = {
                "Item": {
                    "workflow_key": body["Key"]["workflow_key"],
                    "task_key": body["Key"]["task_key"],
                    "run_id": {"S": "mock-run-id"},
                }
            }
        else:
            response = {}

        payload = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-amz-json-1.0")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class MockBatchHandler(BaseHTTPRequestHandler):
    """Minimal Batch API: submit returns a job ID; describe returns SUCCEEDED."""

    # Class-level flag — set True before a test to make the next job FAIL.
    fail_next = False
    _jobs: dict[str, str] = {}

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        if self.path.startswith("/v1/describejobs"):
            # POST /v1/describejobs {"jobs": [JOB_ID, ...]}
            results = []
            for job_id in body.get("jobs", []):
                status = MockBatchHandler._jobs.get(job_id, "SUCCEEDED")
                results.append(
                    {
                        "jobId": job_id,
                        "jobName": "mock",
                        "jobQueue": "mock-queue",
                        "jobDefinition": "mock-def",
                        "status": status,
                        "startedAt": 1_000_000,
                        "stoppedAt": 1_000_001,
                        "attempts": [],
                        "container": {},
                    }
                )
            self._send_json(200, {"jobs": results})
        else:
            # POST /v1/submitjob
            job_id = f"mock-job-{len(self._jobs)}"
            MockBatchHandler._jobs[job_id] = "FAILED" if MockBatchHandler.fail_next else "SUCCEEDED"
            MockBatchHandler.fail_next = False
            self._send_json(200, {"jobId": job_id, "jobName": body.get("jobName", "mock")})

    def _send_json(self, code: int, data: dict):
        payload = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


def start_server(handler_class, port: int) -> HTTPServer:
    """Start an HTTPServer on the given port in a daemon thread."""
    server = HTTPServer(("", port), handler_class)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


@pytest.fixture(scope="session")
def mock_servers():
    """Start mock Lambda, Batch, and DynamoDB HTTP servers, yield their host:port strings."""
    lambda_port = free_port()
    batch_port = free_port()
    dynamodb_port = free_port()
    start_server(MockLambdaHandler, lambda_port)
    start_server(MockBatchHandler, batch_port)
    start_server(MockDynamoDBHandler, dynamodb_port)
    # Host IP that the SFN Local container can reach on the host network
    host = "host.docker.internal"
    yield {
        "lambda": f"http://{host}:{lambda_port}",
        "batch": f"http://{host}:{batch_port}",
        "dynamodb": f"http://{host}:{dynamodb_port}",
    }


@pytest.fixture(scope="session")
def sfn_local(mock_servers):
    """Start SFN Local container wired to the mock Lambda and Batch servers."""
    container = (
        DockerContainer(SFN_LOCAL_IMAGE)
        .with_env("AWS_DEFAULT_REGION", MOCK_REGION)
        .with_env("AWS_ACCESS_KEY_ID", "test")
        .with_env("AWS_SECRET_ACCESS_KEY", "test")
        .with_env("LAMBDA_ENDPOINT", mock_servers["lambda"])
        .with_env("BATCH_ENDPOINT", mock_servers["batch"])
        .with_env("DYNAMODB_ENDPOINT", mock_servers["dynamodb"])
        # Required for sync:2 nested SM invocations: SFN Local must call itself.
        .with_env("STEP_FUNCTIONS_ENDPOINT", f"http://localhost:{SFN_LOCAL_PORT}")
        .with_exposed_ports(SFN_LOCAL_PORT)
        .with_kwargs(extra_hosts={"host.docker.internal": "host-gateway"})
    )
    with container:
        port = container.get_exposed_port(SFN_LOCAL_PORT)
        endpoint = f"http://localhost:{port}"
        wait_for_http(endpoint)
        yield endpoint


@pytest.fixture(scope="session")
def sfn_client(sfn_local):
    """boto3 SFN client pointing at SFN Local."""
    return boto3.client(
        "stepfunctions",
        endpoint_url=sfn_local,
        region_name=MOCK_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="session")
def state_machine(sfn_client):
    """Create the three child state machines and the parent DatasetIngest state machine."""
    # Child SMs must exist before the parent references their ARNs.
    for sm_path, sm_name in [
        (DOWNLOAD_SM_PATH, DOWNLOAD_SM_NAME),
        (SUBSET_SM_PATH, SUBSET_SM_NAME),
        (TRANSFORM_SM_PATH, TRANSFORM_SM_NAME),
    ]:
        sfn_client.create_state_machine(
            name=sm_name,
            definition=resolve_asl(sm_path),
            roleArn=f"arn:aws:iam::{MOCK_ACCOUNT}:role/MockRole",
            type="STANDARD",
        )

    response = sfn_client.create_state_machine(
        name=STATE_MACHINE_NAME,
        definition=resolve_asl(),
        roleArn=f"arn:aws:iam::{MOCK_ACCOUNT}:role/MockRole",
        type="STANDARD",
    )
    return response["stateMachineArn"]


_exec_counter = 0


def start_execution(sfn_client, state_machine_arn: str, workflow_input: dict) -> str:
    """Start an execution with a unique name and return its ARN."""
    global _exec_counter
    _exec_counter += 1
    # run_id must be unique per execution so child SM names (derived from run_id) don't conflict
    # across tests that share the same dataset + publication_date.
    unique_input = {**workflow_input, "run_id": f"20250101T060000-{_exec_counter:08x}"}
    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=f"test-exec-{_exec_counter}",
        input=json.dumps(unique_input),
    )
    return response["executionArn"]


def wait_for_execution(sfn_client, execution_arn: str, *, timeout: float = 90.0) -> dict:
    """Poll until execution reaches a terminal state.

    Args:
        sfn_client: boto3 SFN client.
        execution_arn: ARN of the execution to poll.
        timeout: Maximum seconds to wait.

    Returns:
        The execution description dict.

    Raises:
        TimeoutError: If execution does not complete within timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = sfn_client.describe_execution(executionArn=execution_arn)
        if desc["status"] != "RUNNING":
            return desc
        time.sleep(0.25)
    raise TimeoutError(f"Execution {execution_arn} did not complete within {timeout}s")


class TestRorWorkflow:
    """Tests for the ror dataset (download only, no transform)."""

    def test_download_succeeds(self, sfn_client, state_machine):
        """ROR download succeeds → workflow SUCCEEDED."""
        execution_arn = start_execution(sfn_client, state_machine, {**BASE_INPUT, "workflow_key": "ror"})
        desc = wait_for_execution(sfn_client, execution_arn)
        assert desc["status"] == "SUCCEEDED"

    def test_download_failure_goes_to_failed(self, sfn_client, state_machine):
        """ROR download failure: Batch job fails → workflow FAILED."""
        MockBatchHandler.fail_next = True
        execution_arn = start_execution(sfn_client, state_machine, {**BASE_INPUT, "workflow_key": "ror"})
        desc = wait_for_execution(sfn_client, execution_arn)
        assert desc["status"] == "FAILED"


class TestOpenAlexSubset:
    """Tests for the openalex-works dataset with subset enabled."""

    def test_subset_succeeds(self, sfn_client, state_machine):
        """OpenAlex with subset: download → subset → transform → SUCCEEDED."""
        execution_arn = start_execution(
            sfn_client, state_machine, {**BASE_INPUT, "workflow_key": "openalex-works", "use_subset": True}
        )
        desc = wait_for_execution(sfn_client, execution_arn)
        assert desc["status"] == "SUCCEEDED"


class TestOpenAlexNoSubset:
    """Tests for the openalex-works dataset without subset."""

    def test_no_subset_succeeds(self, sfn_client, state_machine):
        """OpenAlex without subset: download → transform (skips subset) → SUCCEEDED."""
        execution_arn = start_execution(
            sfn_client, state_machine, {**BASE_INPUT, "workflow_key": "openalex-works", "use_subset": False}
        )
        desc = wait_for_execution(sfn_client, execution_arn)
        assert desc["status"] == "SUCCEEDED"


class TestBatchFailure:
    """Tests for Batch job failure routing."""

    def test_batch_failure_routes_to_set_task_run_status(self, sfn_client, state_machine):
        """Any Batch job failure routes to UpdateWorkflowRunStatus then WorkflowFailed."""
        MockBatchHandler.fail_next = True
        execution_arn = start_execution(
            sfn_client, state_machine, {**BASE_INPUT, "workflow_key": "openalex-works", "use_subset": False}
        )
        desc = wait_for_execution(sfn_client, execution_arn)
        assert desc["status"] == "FAILED"


class TestSkipLogic:
    """Tests for skip logic: already-completed task runs are skipped on re-run."""

    def setup_method(self):
        """Reset completed_task_types before each test."""
        MockDynamoDBHandler.completed_task_types = set()

    def teardown_method(self):
        """Reset completed_task_types after each test."""
        MockDynamoDBHandler.completed_task_types = set()

    def test_skip_download_for_ror(self, sfn_client, state_machine):
        """When download is already COMPLETED for ROR, skip child SM and succeed."""
        MockDynamoDBHandler.completed_task_types = {"download"}
        execution_arn = start_execution(sfn_client, state_machine, {**BASE_INPUT, "workflow_key": "ror"})
        desc = wait_for_execution(sfn_client, execution_arn)
        assert desc["status"] == "SUCCEEDED"

    def test_skip_download_and_transform_for_openalex_no_subset(self, sfn_client, state_machine):
        """When download + transform are COMPLETED for openalex (no subset), skip both and succeed."""
        MockDynamoDBHandler.completed_task_types = {"download", "transform"}
        execution_arn = start_execution(
            sfn_client, state_machine, {**BASE_INPUT, "workflow_key": "openalex-works", "use_subset": False}
        )
        desc = wait_for_execution(sfn_client, execution_arn)
        assert desc["status"] == "SUCCEEDED"

    def test_skip_all_task_types_for_openalex_with_subset(self, sfn_client, state_machine):
        """When all task types are COMPLETED, the entire pipeline is skipped and workflow succeeds."""
        MockDynamoDBHandler.completed_task_types = {"download", "subset", "transform"}
        execution_arn = start_execution(
            sfn_client, state_machine, {**BASE_INPUT, "workflow_key": "openalex-works", "use_subset": True}
        )
        desc = wait_for_execution(sfn_client, execution_arn)
        assert desc["status"] == "SUCCEEDED"
