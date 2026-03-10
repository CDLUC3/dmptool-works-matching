from collections.abc import Generator, Mapping
from functools import wraps
import importlib
import logging
import os
import shlex
import subprocess
from typing import TypeVar

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

log = logging.getLogger(__name__)


def timed(func):
    """Log execution time of a function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = pendulum.now()
        try:
            return func(*args, **kwargs)
        finally:
            end = pendulum.now()
            diff = end - start
            log.info(f"Execution time: {diff.in_words()}")

    return wrapper


def run_process(
    args,
    env: Mapping[str, str] | None = None,
):
    """Run a shell script.

    Args:
        args: The command and arguments to run.
        env: Environment variables to set for the process.
    """
    log.info(f"run_process command: `{shlex.join(args)}`")

    with subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    ) as proc:
        for line in proc.stdout:
            log.info(line)

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, args)


class InstanceOf:
    """Helper class to check if an object is an instance of a class.

    Attributes:
        cls: The class to check against.
    """

    def __init__(self, cls):
        """Initialize the InstanceOf helper.

        Args:
            cls: The class to check against.
        """
        self.cls = cls

    def __eq__(self, other):
        """Check if the other object is an instance of the class.

        Args:
            other: The object to check.

        Returns:
            bool: True if the object is an instance of the class, False otherwise.
        """
        return isinstance(other, self.cls)

    def __repr__(self):
        """Return a string representation of the InstanceOf helper.

        Returns:
            str: The string representation.
        """
        return f"<any {self.cls.__name__} instance>"


def copy_dict(original_dict: dict, keys_to_remove: list) -> dict:
    """Create a copy of a dictionary with specific keys removed.

    Args:
        original_dict: The dictionary to copy.
        keys_to_remove: A list of keys to exclude from the copy.

    Returns:
        A new dictionary containing all items from the original dictionary except those with keys in keys_to_remove.
    """
    return {k: v for k, v in original_dict.items() if k not in keys_to_remove}


T = TypeVar("T")
BatchGenerator = Generator[list[T], None, None]


def to_batches(items: list[T], batch_size: int) -> BatchGenerator:
    """Yield successive batches from a list.

    Args:
        items: The list of items to batch.
        batch_size: The size of each batch.

    Yields:
        A generator yielding lists of items of size batch_size.
    """
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def retry_session(
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
    raise_on_status: bool = True,
) -> requests.Session:
    """Create a requests session with retry logic.

    Args:
        total_retries: Total number of retries to allow.
        backoff_factor: A backoff factor to apply between attempts.
        status_forcelist: A set of HTTP status codes that we should force a retry on.
        raise_on_status: Whether to raise an exception on status codes.

    Returns:
        A requests.Session object configured with the specified retry strategy.
    """
    retry_strategy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=raise_on_status,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def import_from_path(path: str):
    """Import a module or attribute from a string path.

    Args:
        path: The dotted path to the module or attribute (e.g., 'package.module.attribute').

    Returns:
        The imported module or attribute.
    """
    module_path, attr_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def fetch_datacite_aws_credentials() -> tuple[str, str, str]:
    """Fetches DataCite AWS credentials.

    Retrieves credentials from environment variables and the DataCite API.

    Returns:
        A tuple containing (access_key_id, secret_access_key, session_token).

    Raises:
        RuntimeError: If environment variables are missing or the API request fails.
    """
    account_id = os.getenv("DATACITE_ACCOUNT_ID")
    password = os.getenv("DATACITE_PASSWORD")

    if not account_id or not password:
        raise RuntimeError("DATACITE_ACCOUNT_ID and DATACITE_PASSWORD must be set in environment variables.")

    url = "https://api.datacite.org/credentials/datafile"

    try:
        response = requests.get(url, auth=(account_id, password), timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError("Failed to fetch DataCite credentials") from e

    try:
        data = response.json()
        access_key_id = data["access_key_id"]
        secret_access_key = data["secret_access_key"]
        session_token = data["session_token"]
    except (KeyError, ValueError) as e:
        raise RuntimeError("Unexpected response format from DataCite credentials endpoint.") from e

    return access_key_id, secret_access_key, session_token
