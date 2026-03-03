from concurrent.futures import ProcessPoolExecutor, as_completed
import gzip
import logging
from multiprocessing import current_process
import os
import pathlib
from typing import Literal

import simdjson
from tqdm import tqdm

from dmpworks.model.common import Institution
from dmpworks.transform.simdjson_transforms import (
    clean_string,
    ensure_array_of_objects,
    extract_doi,
    normalise_identifier,
    to_optional_string,
)
from dmpworks.utils import timed

log = logging.getLogger(__name__)

Dataset = Literal["crossref-metadata", "datacite", "openalex-works"]


def keep_record(
    dataset: Dataset, institution_rors: set[str], institution_names: set[str], dois: set[str], record: simdjson.Object
) -> bool:
    """Determine whether to keep a record based on filtering criteria.

    Args:
        dataset: The dataset type.
        institution_rors: Set of institution RORs to keep.
        institution_names: Set of institution names to keep.
        dois: Set of DOIs to keep.
        record: The record to check.

    Returns:
        bool: True if the record should be kept, False otherwise.
    """
    if dataset == "openalex-works":
        # Check DOI
        doi = extract_doi(to_optional_string(record.get("doi")))
        if doi in dois:
            return True

        # Check institutions
        for authorship in record.get("authorships", []):
            for inst in authorship.get("institutions", []):
                identifier = to_optional_string(inst.get("ror"))
                name = clean_string(to_optional_string(inst.get("display_name")), lower=True)
                if normalise_identifier(identifier) in institution_rors or name in institution_names:
                    return True
        return False

    if dataset == "datacite":
        # Check DOI
        doi = to_optional_string(record.get("id"))
        if doi in dois:
            return True

        # Check institutions
        for creator in record.get("attributes", {}).get("creators", []):
            for affiliation in ensure_array_of_objects(creator.get("affiliation", [])):
                identifier = to_optional_string(affiliation.get("affiliationIdentifier"))
                name = clean_string(to_optional_string(affiliation.get("name")), lower=True)
                if normalise_identifier(identifier) in institution_rors or name in institution_names:
                    return True
        return False

    if dataset == "crossref-metadata":
        # Check DOI
        doi = to_optional_string(record.get("DOI"))
        if doi in dois:
            return True

        # Check institutions
        for author in record.get("author", []):
            for affiliation in author.get("affiliation", []):
                name = clean_string(to_optional_string(affiliation.get("name")), lower=True)
                if name in institution_names:
                    return True

                for id_struct in affiliation.get("id", []):
                    identifier = to_optional_string(id_struct.get("id"))
                    if normalise_identifier(identifier) in institution_rors:
                        return True
        return False

    raise ValueError(f"keep_record: unknown dataset type {dataset}")


def get_file_glob(dataset: Dataset) -> str:
    """Get the file glob pattern for a dataset.

    Args:
        dataset: The dataset type.

    Returns:
        str: The file glob pattern.
    """
    if dataset == "openalex-works":
        return "**/*.gz"
    if dataset == "datacite":
        return "**/*jsonl.gz"
    if dataset == "crossref-metadata":
        return "*.jsonl.gz"
    raise ValueError(f"get_file_glob: unknown dataset type {dataset}")


def init_process_logs(level: int):
    """Initialize logging for a worker process.

    Args:
        level: The logging level.
    """
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")


def filter_dataset(
    dataset: Dataset,
    institution_rors: set[str],
    institution_names: set[str],
    dois: set[str],
    file_in: pathlib.Path,
    out_dir: pathlib,
):
    """Filter a dataset file and write matching records to an output file.

    Args:
        dataset: The dataset type.
        institution_rors: Set of institution RORs to keep.
        institution_names: Set of institution names to keep.
        dois: Set of DOIs to keep.
        file_in: Path to the input file.
        out_dir: Path to the output directory.

    Returns:
        int: The number of records filtered.
    """
    log.debug(f"start filtering {file_in}")

    worker_id = current_process()._identity[0]
    file_out = out_dir / f"part_{worker_id:03d}.jsonl.gz"

    parser = simdjson.Parser()
    total_filtered = 0
    # f_out: needs ab to append output when the same process processes another file
    with gzip.open(file_out, mode="ab") as f_out, gzip.open(file_in, "rb") as f_in:
        for line in f_in:
            if not line.strip():
                continue

            try:
                record = parser.parse(line)

                if keep_record(dataset, institution_rors, institution_names, dois, record):
                    f_out.write(line)
                    total_filtered += 1
            except ValueError:
                log.exception(f"Error reading record from {file_in}")
                continue
            finally:
                # Clear original reference for simdjson parser
                record = None

    log.debug(f"end filtering {file_in}")

    return total_filtered


@timed
def create_dataset_subset(
    *,
    dataset: Dataset,
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    institutions: list[Institution],
    dois: list[str],
    log_level: int = logging.INFO,
):
    """Create a subset of a dataset based on institutions and DOIs.

    Args:
        dataset: The dataset type.
        in_dir: Path to the input directory.
        out_dir: Path to the output directory.
        institutions: List of institutions to filter by.
        dois: List of DOIs to filter by.
        log_level: Logging level.
    """
    is_empty = next(out_dir.iterdir(), None) is None
    if not is_empty:
        raise Exception(f"Output directory is not empty: {out_dir}")

    file_glob = get_file_glob(dataset)
    files = list(pathlib.Path(in_dir).glob(file_glob))
    futures = []
    institution_rors = {inst.ror for inst in institutions if inst.ror is not None}
    institution_names = {val for inst in institutions if (val := clean_string(inst.name, lower=True)) is not None}
    dois_set = {extract_doi(doi) for doi in dois}

    log.info(f"institutions: {institutions}")
    log.info(f"dois: {dois_set}")
    log.info(f"institution_rors: {institution_rors}")
    log.info(f"institution_names: {institution_names}")

    try:
        with ProcessPoolExecutor(
            max_workers=os.cpu_count(), initializer=init_process_logs, initargs=(log_level,)
        ) as executor:
            for file_in in files:
                future = executor.submit(
                    filter_dataset, dataset, institution_rors, institution_names, dois_set, file_in, out_dir
                )
                futures.append(future)

            total_files = len(files)
            total_filtered = 0
            total_errors = 0
            with tqdm(
                total=total_files,
                desc=f"Filter {dataset}",
                unit="file",
            ) as pbar:
                for future in as_completed(futures):
                    try:
                        total_filtered += future.result()
                    except Exception:
                        log.exception("Error getting future")
                        total_errors += 1
                    pbar.update(1)
                    pbar.set_postfix({"Filtered": f"{total_filtered:,}", "Errors": f"{total_errors:,}"})
    except KeyboardInterrupt:
        log.info("Shutting down...")
        executor.shutdown(wait=True, cancel_futures=True)
