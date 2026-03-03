from collections.abc import Generator
import gzip
import logging
from multiprocessing.util import log_to_stderr
import pathlib

import simdjson

log = logging.getLogger(__name__)


def setup_multiprocessing_logging(log_level: int):
    """Setup logging for multiprocessing.

    Args:
        log_level: The logging level.
    """
    logging.basicConfig(
        level=log_level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] [%(threadName)s] %(message)s"
    )
    if log_level == logging.DEBUG:
        # Make multi-processing print logs
        log_to_stderr(logging.DEBUG)


def yield_objects_from_jsonl(file_path: pathlib.Path) -> Generator[simdjson.Object, None, None]:
    """Yields JSON objects from a plain or gzipped JSON lines file.

    Args:
        file_path: the path to the file.

    Returns: generator.

    """
    parser = simdjson.Parser()
    opener = gzip.open if file_path.suffix == ".gz" else open
    line_num = 0

    with opener(file_path, "rb") as f:
        for line in f:
            line_num += 1

            # Skip emtpy lines
            if not line.strip():
                continue

            try:
                row = parser.parse(line)
                yield row
            except ValueError:
                log.exception(f"yield_jsonl: error parsing line {line_num} in {file_path}")
                continue
            finally:
                # Clear original reference for simdjson parser
                row = None


def yield_objects_from_json(file_path: pathlib.Path) -> Generator[simdjson.Object, None, None]:
    """Yields JSON objects from a plain or gzipped JSON file.

    Args:
        file_path: the path to the file.

    Returns: generator.

    """
    parser = simdjson.Parser()

    if file_path.suffix == ".gz":
        with gzip.open(file_path, "rb") as f:
            content = f.read()
            doc = parser.parse(content)
    else:
        doc = parser.load(file_path)

    if isinstance(doc, list):
        yield from doc
    else:
        yield doc
