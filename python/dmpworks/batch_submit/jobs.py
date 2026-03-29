from collections.abc import Callable
from functools import cache
import inspect
import logging
from typing import Any, TypedDict

import boto3
import pendulum

from dmpworks.batch_submit.job_factories import (
    DOWNLOAD_QUEUE_MEMORY,  # noqa: F401
    DOWNLOAD_QUEUE_VCPUS,  # noqa: F401
    JOB_FACTORIES,
    OPENSEARCH_QUEUE_MEMORY,  # noqa: F401
    OPENSEARCH_QUEUE_VCPUS,  # noqa: F401
    PIPELINE_TASK_TYPES,  # noqa: F401
    SMALL_QUEUE_MEMORY,  # noqa: F401
    SMALL_QUEUE_VCPUS,  # noqa: F401
    SQLMESH_QUEUE_MEMORY,  # noqa: F401
    SQLMESH_QUEUE_VCPUS,  # noqa: F401
    TRANSFORM_QUEUE_MEMORY,  # noqa: F401
    TRANSFORM_QUEUE_VCPUS,  # noqa: F401
    datacite_download_job_definition,  # noqa: F401
    get_task_types_to_run,  # noqa: F401
    standard_job_queue,  # noqa: F401
)


def run_job_pipeline(
    *,
    task_definitions: dict[str, Callable],
    task_order: list[str],
    start_task_name: str,
):
    """Run a pipeline of AWS Batch jobs.

    Args:
        task_definitions: A dictionary mapping task names to callable job submission functions.
        task_order: A list of task names defining the execution order.
        start_task_name: The name of the task to start the pipeline from.

    Raises:
        ValueError: If the start task is unknown or a task definition is missing.
    """
    # Build list of tasks
    try:
        start_index = task_order.index(start_task_name)
    except ValueError as e:
        msg = f"Unknown start_task '{start_task_name}'"
        raise ValueError(msg) from e
    task_names = task_order[start_index:]

    # Execute tasks
    job_ids = []
    for task_name in task_names:
        if task_name not in task_definitions:
            msg = f"No function defined for task '{task_name}'"
            raise ValueError(msg)

        # Add any dependent jobs
        task_func = task_definitions[task_name]
        kwargs = {}
        sig = inspect.signature(task_func)
        if "depends_on" in sig.parameters and len(job_ids) > 0:
            kwargs["depends_on"] = [job_ids[-1]]

        # Call the task
        job_id = task_func(**kwargs)
        if job_id is not None:
            job_ids.append({"jobId": str(job_id)})


@cache
def get_aws_batch_client():
    """Get the AWS Batch client (cached).

    Returns:
        boto3.client: The AWS Batch client.
    """
    return boto3.client("batch")


class EnvVarDict(TypedDict):
    """Type definition for an environment variable dictionary.

    Attributes:
        name: The name of the environment variable.
        value: The value of the environment variable.
    """

    name: str
    value: str


class DependsOnDict(TypedDict):
    """Type definition for a job dependency dictionary.

    Attributes:
        jobId: The ID of the job to depend on.
    """

    jobId: str


def format_date(date: pendulum.Date):
    """Format a date as YYYY-MM-DD.

    Args:
        date: The date to format.

    Returns:
        str: The formatted date string.
    """
    return date.format("YYYY-MM-DD")


def submit_job(
    *,
    job_name: str,
    run_id: str,
    job_queue: str,
    job_definition: str,
    vcpus: int | None = None,
    memory: int | None = None,
    command: str | None = None,
    environment: list[EnvVarDict] | None = None,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submit a job to AWS Batch.

    Args:
        job_name: The name of the job.
        run_id: The unique run identifier.
        job_queue: The job queue to submit to.
        job_definition: The job definition to use.
        vcpus: The number of vCPUs to request.
        memory: The amount of memory to request (in MiB).
        command: The command to run.
        environment: A list of environment variables.
        depends_on: A list of job dependencies.

    Returns:
        str: The ID of the submitted job.
    """
    # Create container overrides
    container_overrides = {}
    if vcpus is not None:
        container_overrides["vcpus"] = vcpus
    if memory is not None:
        container_overrides["memory"] = memory
    if command is not None:
        container_overrides["command"] = ["/bin/bash", "-c", command]
    if environment is not None:
        container_overrides["environment"] = environment

    if depends_on is None:
        depends_on = []

    # Submit job
    batch_client = get_aws_batch_client()
    full_job_name = f"{job_name}-{run_id}"
    response = batch_client.submit_job(
        jobName=full_job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition,
        dependsOn=depends_on,
        containerOverrides=container_overrides,
    )
    job_id = response["jobId"]
    logging.info(f"Submitted job {full_job_name} with ID: {job_id}")

    return job_id


def make_env(env_vars: dict[str, str | None]) -> list[EnvVarDict]:
    """Create a list of environment variable dictionaries from a dictionary.

    Args:
        env_vars: A dictionary of environment variables.

    Returns:
        list[EnvVarDict]: A list of environment variable dictionaries.
    """
    return [{"name": k, "value": str(v)} for k, v in env_vars.items() if v is not None]


def submit_job_from_params(
    *,
    params: dict[str, Any],
    run_id: str,
    depends_on: list[DependsOnDict] | None = None,
) -> str:
    """Submit an AWS Batch job from a factory-produced params dict.

    Translates PascalCase SFN-compatible params (Name/Value) to boto3
    snake_case arguments (name/value). Reads run_name directly from params.

    Args:
        params: Dict as returned by a factory function (includes "run_name" key).
        run_id: The run_id passed to the factory (forwarded to submit_job).
        depends_on: Optional list of job dependency dicts.

    Returns:
        str: The submitted job ID.
    """
    overrides = params["ContainerOverrides"]
    boto3_env: list[EnvVarDict] = [{"name": e["Name"], "value": e["Value"]} for e in overrides["Environment"]]
    return submit_job(
        job_name=params["run_name"],
        run_id=run_id,
        job_queue=params["JobQueue"],
        job_definition=params["JobDefinition"],
        command=overrides["Command"][2],  # ["/bin/bash", "-c", "<cmd>"]
        vcpus=overrides["Vcpus"],
        memory=overrides["Memory"],
        environment=boto3_env,
        depends_on=depends_on,
    )


def submit_factory_job(
    *,
    factory_key: tuple[str, str],
    run_id: str,
    depends_on: list[DependsOnDict] | None = None,
    **kwargs: Any,
) -> str:
    """Look up a factory by key, build Batch params, and submit the job.

    Args:
        factory_key: (workflow_key, task_type) tuple for JOB_FACTORIES lookup.
        run_id: Unique run ID for this execution.
        depends_on: Optional list of job dependencies.
        **kwargs: Passed directly to the factory function.

    Returns:
        The job ID of the submitted AWS Batch job.
    """
    params = JOB_FACTORIES[factory_key](run_id=run_id, **kwargs)
    return submit_job_from_params(params=params, run_id=run_id, depends_on=depends_on)
