import logging
import shlex
import subprocess
from functools import wraps
from typing import Generator, TypeVar
import importlib
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

log = logging.getLogger(__name__)


def timed(func):
    """Log execution time of a function"""

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


def run_process(args):
    """Run a shell script"""

    log.info(f"run_process command: `{shlex.join(args)}`")

    with subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as proc:
        for line in proc.stdout:
            log.info(line)

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, args)


class InstanceOf:
    def __init__(self, cls):
        self.cls = cls

    def __eq__(self, other):
        return isinstance(other, self.cls)

    def __repr__(self):
        return f"<any {self.cls.__name__} instance>"


def copy_dict(original_dict: dict, keys_to_remove: list) -> dict:
    return {k: v for k, v in original_dict.items() if k not in keys_to_remove}


T = TypeVar("T")
BatchGenerator = Generator[list[T], None, None]


def to_batches(items: list[T], batch_size: int) -> BatchGenerator:
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def retry_session(
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
    raise_on_status: bool = True,
):
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
    module_path, attr_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)
