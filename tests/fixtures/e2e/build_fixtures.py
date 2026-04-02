#!/usr/bin/env python3
"""Build e2e test fixture data by extracting records matching a set of DOIs from upstream data dumps.

Pass 1: Scan OpenAlex, Crossref, DataCite for DOI matches (parallel per-file).
        Collect matched lines and extract ROR IDs from matched records.
Pass 2: Scan Data Citation Corpus for DOI matches (parallel per-file).
Pass 3: Filter ROR by the ROR IDs collected in pass 1 (single file, single-threaded).

Reads upstream data paths from environment variables defined in the provided .env file.

Usage:
    python tests/fixtures/e2e/build_fixtures.py --env-file .env.local --doi-file dois.txt
"""

import gzip
import json
import logging
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Annotated

import simdjson
from cyclopts import App, Parameter, validators
from dotenv import load_dotenv
from tqdm import tqdm

from dmpworks.transform.dataset_subset import Dataset, get_file_glob
from dmpworks.transform.simdjson_transforms import (
    ensure_array_of_objects,
    extract_doi,
    extract_ror,
    normalise_identifier,
    to_optional_string,
)

log = logging.getLogger(__name__)

FIXTURES_DIR = pathlib.Path(__file__).resolve().parent / "source"


def get_doi(dataset: Dataset, record: simdjson.Object) -> str | None:
    """Extract the DOI from a record, using the same field path as keep_record.

    Args:
        dataset: Dataset identifier.
        record: A parsed simdjson record.

    Returns:
        Normalised DOI string or None.
    """
    if dataset == "openalex-works":
        return extract_doi(to_optional_string(record.get("doi")))
    if dataset == "datacite":
        return extract_doi(to_optional_string(record.get("id")))
    if dataset == "crossref-metadata":
        return extract_doi(to_optional_string(record.get("DOI")))
    return None


def get_ror_ids(dataset: Dataset, record: simdjson.Object) -> set[str]:
    """Extract ROR IDs from a record, using the same field paths as keep_record.

    Args:
        dataset: Dataset identifier.
        record: A parsed simdjson record.

    Returns:
        Set of extracted ROR ID strings.
    """
    ror_ids = set()

    if dataset == "openalex-works":
        for authorship in record.get("authorships", []):
            for inst in authorship.get("institutions", []):
                ror_id = extract_ror(to_optional_string(inst.get("ror")))
                if ror_id:
                    ror_ids.add(ror_id)

    elif dataset == "datacite":
        for creator in record.get("attributes", {}).get("creators", []):
            for affiliation in ensure_array_of_objects(creator.get("affiliation", [])):
                ror_id = extract_ror(to_optional_string(affiliation.get("affiliationIdentifier")))
                if ror_id:
                    ror_ids.add(ror_id)

    elif dataset == "crossref-metadata":
        for author in record.get("author", []):
            for affiliation in author.get("affiliation", []):
                for id_struct in affiliation.get("id", []):
                    ror_id = extract_ror(to_optional_string(id_struct.get("id")))
                    if ror_id:
                        ror_ids.add(ror_id)

    return ror_ids


def scan_jsonl_file(dataset: Dataset, dois: set[str], file_path: str) -> tuple[list[bytes], set[str]]:
    """Scan a single gzipped JSONL file for records matching the target DOIs.

    Args:
        dataset: Dataset identifier ("openalex-works", "crossref-metadata", or "datacite").
        dois: Set of normalised DOIs to match.
        file_path: Path to the gzipped JSONL file.

    Returns:
        Tuple of (matched raw lines, extracted ROR IDs from matched records).
    """
    parser = simdjson.Parser()
    matched_lines = []
    ror_ids = set()

    with gzip.open(file_path, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = parser.parse(line)
                doi = get_doi(dataset, record)
                if doi in dois:
                    matched_lines.append(line.rstrip(b"\n"))
                    ror_ids |= get_ror_ids(dataset, record)
            except ValueError:
                continue
            finally:
                record = None

    return matched_lines, ror_ids


def scan_jsonl_dataset(*, dataset: Dataset, in_dir: pathlib.Path, dois: set[str]) -> tuple[list[bytes], set[str]]:
    """Scan all files in a JSONL dataset directory for DOI matches.

    Args:
        dataset: Dataset identifier.
        in_dir: Directory containing the gzipped JSONL files.
        dois: Set of normalised DOIs to match.

    Returns:
        Tuple of (all matched raw lines, all extracted ROR IDs).
    """
    files = sorted(in_dir.glob(get_file_glob(dataset)))
    all_lines = []
    all_ror_ids = set()

    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(scan_jsonl_file, dataset, dois, str(f)): f for f in files}
        with tqdm(total=len(futures), desc=dataset, unit="file") as pbar:
            for future in as_completed(futures):
                lines, ror_ids = future.result()
                all_lines.extend(lines)
                all_ror_ids |= ror_ids
                pbar.update(1)
                pbar.set_postfix({"matched": len(all_lines)}, refresh=False)

    return all_lines, all_ror_ids


def scan_dcc_file(dois: set[str], file_path: str) -> list[dict]:
    """Scan a single DCC gzipped JSON file for records matching the target DOIs.

    Args:
        dois: Set of normalised DOIs to match.
        file_path: Path to the gzipped JSON file.

    Returns:
        List of matched records as dicts.
    """
    matched = []
    with gzip.open(file_path, "rt") as f:
        data = json.load(f)
    for record in data:
        pub_doi = extract_doi(record.get("publication"))
        ds_doi = extract_doi(record.get("dataset"))
        if pub_doi in dois or ds_doi in dois:
            matched.append(record)
    return matched


def scan_dcc_dataset(*, in_dir: pathlib.Path, dois: set[str]) -> list[dict]:
    """Scan all Data Citation Corpus files for DOI matches.

    Args:
        in_dir: Directory containing the gzipped JSON files.
        dois: Set of normalised DOIs to match.

    Returns:
        List of all matched records.
    """
    files = sorted(in_dir.glob("*.json.gz"))
    all_matched = []

    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(scan_dcc_file, dois, str(f)): f for f in files}
        with tqdm(total=len(futures), desc="data-citation-corpus", unit="file") as pbar:
            for future in as_completed(futures):
                all_matched.extend(future.result())
                pbar.update(1)
                pbar.set_postfix({"matched": len(all_matched)}, refresh=False)

    return all_matched


def filter_ror(*, in_dir: pathlib.Path, ror_ids: set[str]) -> list[dict]:
    """Filter ROR records by a set of ROR IDs.

    Args:
        in_dir: Directory containing the ROR gzipped JSON file.
        ror_ids: Set of normalised ROR IDs to match.

    Returns:
        List of matched ROR records.
    """
    files = sorted(in_dir.glob("*.json.gz"))
    if not files:
        log.warning(f"No ROR files found in {in_dir}")
        return []

    matched = []
    with gzip.open(files[0], "rt") as f:
        data = json.load(f)

    for record in tqdm(data, desc="ror", unit="record"):
        record_ror = normalise_identifier(record.get("id"))
        if record_ror in ror_ids:
            matched.append(record)

    return matched


app = App(name="build-fixtures", help="Build e2e test fixtures from upstream data dumps.")


@dataclass
class UpstreamPaths:
    """Paths to upstream data dump directories, read from environment variables."""

    openalex_works: Annotated[
        pathlib.Path,
        Parameter(env_var="UPSTREAM_OPENALEX_WORKS", help="OpenAlex works dump directory."),
    ]
    crossref_metadata: Annotated[
        pathlib.Path,
        Parameter(env_var="UPSTREAM_CROSSREF_METADATA", help="Crossref metadata dump directory."),
    ]
    datacite: Annotated[
        pathlib.Path,
        Parameter(env_var="UPSTREAM_DATACITE", help="DataCite dump directory."),
    ]
    data_citation_corpus: Annotated[
        pathlib.Path,
        Parameter(env_var="UPSTREAM_DATA_CITATION_CORPUS", help="Data Citation Corpus dump directory."),
    ]
    ror: Annotated[
        pathlib.Path,
        Parameter(env_var="UPSTREAM_ROR", help="ROR dump directory."),
    ]


@app.default
def build(
    *,
    upstream: UpstreamPaths,
    doi_file: Annotated[
        pathlib.Path,
        Parameter(help="Text file with one DOI per line.", validator=validators.Path(exists=True)),
    ],
    out_dir: Annotated[
        pathlib.Path,
        Parameter(help="Output directory for fixture files."),
    ] = FIXTURES_DIR,
) -> None:
    """Extract records matching DOIs from upstream data dumps into fixture files."""
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")

    dois = set()
    for line in doi_file.read_text().splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        doi = extract_doi(raw)
        if doi:
            dois.add(doi)
        else:
            log.warning(f"Could not extract DOI from: {raw}")
    if not dois:
        raise SystemExit("No valid DOIs found in file")

    log.info(f"Target DOIs ({len(dois)}): {sorted(dois)}")

    all_ror_ids = set()

    jsonl_datasets: list[tuple[Dataset, pathlib.Path, str, str]] = [
        ("openalex-works", upstream.openalex_works, "openalex", "works.jsonl"),
        ("crossref-metadata", upstream.crossref_metadata, "crossref", "metadata.jsonl"),
        ("datacite", upstream.datacite, "datacite", "datacite.jsonl"),
    ]

    for dataset, in_dir, subdir, filename in jsonl_datasets:
        lines, ror_ids = scan_jsonl_dataset(dataset=dataset, in_dir=in_dir, dois=dois)
        all_ror_ids |= ror_ids

        dest = out_dir / subdir / filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for line in lines:
                f.write(line + b"\n")
        log.info(f"{dataset}: {len(lines)} records → {dest}")

    dcc_records = scan_dcc_dataset(in_dir=upstream.data_citation_corpus, dois=dois)
    dest = out_dir / "data_citation_corpus" / "dcc.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        json.dump(dcc_records, f, indent=2)
    log.info(f"data-citation-corpus: {len(dcc_records)} records → {dest}")

    log.info(f"ROR IDs collected from matched records ({len(all_ror_ids)}): {sorted(all_ror_ids)}")
    ror_records = filter_ror(in_dir=upstream.ror, ror_ids=all_ror_ids)
    dest = out_dir / "ror" / "ror.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        json.dump(ror_records, f, indent=2)
    log.info(f"ror: {len(ror_records)} records → {dest}")
    log.info(f"Done. Fixture files written to: {out_dir}")


@app.meta.default
def meta(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    env_file: Annotated[
        pathlib.Path,
        Parameter(help="Path to .env file containing UPSTREAM_* variables."),
    ],
) -> None:
    """Load environment variables from .env file, then run the build command.

    Args:
        *tokens: Forwarded CLI tokens.
        env_file: Path to a .env file to load before executing the command.
    """
    load_dotenv(dotenv_path=env_file, override=True)
    app(tokens)


if __name__ == "__main__":
    app.meta()
