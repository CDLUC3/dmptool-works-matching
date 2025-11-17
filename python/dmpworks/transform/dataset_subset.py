import gzip
import logging
import os
import pathlib
import re
from concurrent.futures import as_completed, ProcessPoolExecutor
from multiprocessing import current_process
from typing import Literal, Optional

import orjson
from tqdm import tqdm

from dmpworks.utils import timed

Dataset = Literal["crossref-metadata", "datacite", "openalex-works"]


def normalise_affiliations(affiliations) -> Optional[list[dict]]:
    if isinstance(affiliations, dict):
        return [affiliations]
    elif isinstance(affiliations, list):
        return affiliations
    else:
        return []


def normalise_identifier(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return re.sub(r"(?i)^https?://[^/]+/", "", value.strip()).lower()


def normalise_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    cleaned = name.strip().lower()
    return cleaned or None


def keep_record(dataset: Dataset, ror_ids: set[str], institution_names: set[str], record: dict) -> bool:
    if dataset == "openalex-works":
        for authorship in record.get("authorships", []):
            for inst in authorship.get("institutions", []):
                identifier = inst.get("ror")
                name = normalise_name(inst.get("display_name"))
                if normalise_identifier(identifier) in ror_ids or name in institution_names:
                    return True
        return False

    elif dataset == "datacite":
        for creator in record.get("attributes", {}).get("creators", []):
            for affiliation in normalise_affiliations(creator.get("affiliation", [])):
                identifier = affiliation.get("affiliationIdentifier")
                name = normalise_name(affiliation.get("name"))
                if normalise_identifier(identifier) in ror_ids or name in institution_names:
                    return True
        return False

    elif dataset == "crossref-metadata":
        for author in record.get("author", []):
            for affiliation in author.get("affiliation", []):
                name = normalise_name(affiliation.get("name"))
                if name in institution_names:
                    return True

                for id_struct in affiliation.get("id", []):
                    identifier = id_struct.get("id")
                    if normalise_identifier(identifier) in ror_ids:
                        return True
        return False

    else:
        raise ValueError(f"keep_record: unknown dataset type {dataset}")


def get_file_glob(dataset: Dataset) -> str:
    if dataset == "openalex-works":
        return "**/*.gz"
    elif dataset == "datacite":
        return "**/*jsonl.gz"
    elif dataset == "crossref-metadata":
        return "*.jsonl.gz"
    else:
        raise ValueError(f"get_file_glob: unknown dataset type {dataset}")


def init_process_logs(level: int):
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")


def filter_dataset(
    dataset: Dataset, ror_ids: set[str], institution_names: set[str], file_in: pathlib.Path, out_dir: pathlib
):
    logging.debug(f"start filtering {file_in}")

    worker_id = current_process()._identity[0]
    file_out = out_dir / f"part_{worker_id:03d}.jsonl.gz"

    total_filtered = 0
    with gzip.open(file_out, mode="ab") as f_out:
        with gzip.open(file_in, "rt", encoding="utf-8") as f_in:
            for line in f_in:
                if line.strip():
                    record = orjson.loads(line)
                    if keep_record(dataset, ror_ids, institution_names, record):
                        f_out.write(line.encode("utf-8"))  # line already ends with newline
                        total_filtered += 1

    logging.debug(f"end filtering {file_in}")

    return total_filtered


@timed
def create_dataset_subset(
    *,
    dataset: Dataset,
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    ror_ids: set[str],
    institution_names: set[str],
    log_level: int,
):
    is_empty = next(out_dir.iterdir(), None) is None
    if not is_empty:
        raise Exception(f"Output directory is not empty: {out_dir}")

    file_glob = get_file_glob(dataset)
    files = list(pathlib.Path(in_dir).glob(file_glob))
    futures = []
    institution_names = set([val for name in institution_names if (val := normalise_name(name)) is not None])

    try:
        with ProcessPoolExecutor(
            max_workers=os.cpu_count(), initializer=init_process_logs, initargs=(log_level,)
        ) as executor:
            for file_in in files:
                future = executor.submit(filter_dataset, dataset, ror_ids, institution_names, file_in, out_dir)
                futures.append(future)

            total_files = len(files)
            total_filtered = 0
            total_errors = 0
            with tqdm(
                total=total_files,
                desc=f"Filter {dataset}",
                unit="file",
            ) as pbar:
                for i, future in enumerate(as_completed(futures)):
                    try:
                        total_filtered += future.result()
                    except Exception as exc:
                        logging.error(exc)
                        total_errors += 1
                    pbar.update(1)
                    pbar.set_postfix({"Filtered": f"{total_filtered:,}", "Errors": f"{total_errors:,}"})
    except KeyboardInterrupt:
        logging.info(f"Shutting down...")
        executor.shutdown(wait=True, cancel_futures=True)
