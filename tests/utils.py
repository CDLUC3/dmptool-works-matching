import gzip
from importlib.resources import as_file, files
import json
import pathlib
import time
import urllib.error
import urllib.request


def get_fixtures_path():
    """Return the resolved path to the tests/fixtures package directory."""
    resource = files("tests.fixtures")

    if not resource.is_dir():
        raise FileNotFoundError("tests.fixtures path not found")

    with as_file(resource) as path:
        pass
    return path


def wait_for_http(url: str, *, timeout: float = 30.0) -> None:
    """Poll a URL until it returns any HTTP response, including error codes.

    Args:
        url: The URL to poll.
        timeout: Maximum number of seconds to wait before raising TimeoutError.

    Raises:
        TimeoutError: If the URL does not respond within the timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(url, timeout=1)  # noqa: S310
        except urllib.error.HTTPError:
            # Any HTTP response means the server is up.
            return
        except Exception:
            time.sleep(0.25)
        else:
            return
    raise TimeoutError(f"Service at {url} did not become ready within {timeout}s")


def read_jsonl_gz(path: pathlib.Path) -> list[dict]:
    """Read all records from a .jsonl.gz file."""
    records = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            records.append(json.loads(line))
    return records


class InstanceOf:
    """Equality helper that matches any instance of a given class.

    Useful in test assertions where you need to verify a value's type
    without caring about its exact content.

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

    __hash__ = None

    def __repr__(self):
        """Return a string representation of the InstanceOf helper.

        Returns:
            str: The string representation.
        """
        return f"<any {self.cls.__name__} instance>"
