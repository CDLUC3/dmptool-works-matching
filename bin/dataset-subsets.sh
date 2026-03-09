#!/usr/bin/env bash

# Generates a subset of Crossref Metadata, DataCite and OpenAlex Works,
# by copying records associated with specific ROR IDs, institution names,
# or DOIs.
#
# Required environment variables:
#   DATA_DIR: Working data directory. Subsets are written to ${DATA_DIR}/sources/
#     using the standardised dataset layout.
#   UPSTREAM_CROSSREF_METADATA: Path to the upstream Crossref Metadata dataset.
#   UPSTREAM_DATACITE: Path to the upstream DataCite dataset.
#   UPSTREAM_OPENALEX_WORKS: Path to the upstream OpenAlex Works dataset.
#   UPSTREAM_ROR: Path to the upstream ROR dataset.
#   UPSTREAM_DATA_CITATION_CORPUS: Path to the upstream Data Citation Corpus.
#   DATASET_SUBSET_INSTITUTIONS_PATH: Path to a JSON file containing a list of
#     ROR IDs and institution names, e.g.
#     [{"name": "University of California, San Diego", "ror": "0168r3w48"}]
#   DATASET_SUBSET_DOIS_PATH: Path to a JSON file with a list of Work DOIs to
#     include in the subset, e.g. ["10.0000/abc", "10.0000/123"]

set -euo pipefail

if [ -f .env.local ]; then
  # shellcheck source=../.env.local
  source .env.local
fi

REQUIRED_VARS=(
  DATA_DIR
  UPSTREAM_CROSSREF_METADATA
  UPSTREAM_DATACITE
  UPSTREAM_OPENALEX_WORKS
  UPSTREAM_ROR
  UPSTREAM_DATA_CITATION_CORPUS
  DATASET_SUBSET_INSTITUTIONS_PATH
  DATASET_SUBSET_DOIS_PATH
)

for var in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!var:-}" ]; then
    echo "Error: environment variable $var is not set" >&2
    exit 1
  fi
done

SOURCES_DIR="${DATA_DIR}/sources"

if [ -d "${SOURCES_DIR}" ]; then
  SYMLINK_COUNT=$(find "${SOURCES_DIR}" -maxdepth 1 -type l | wc -l)
  if [ "${SYMLINK_COUNT}" -gt 0 ]; then
    echo "Sources directory contains symlinks (likely created by link-upstream.sh):"
    find "${SOURCES_DIR}" -maxdepth 1 -type l -printf "  %f -> %l\n"
    read -p "Remove symlinks and replace with subset copies? [y/N] " confirm
  else
    read -p "Delete contents of '${SOURCES_DIR}' and regenerate subsets? [y/N] " confirm
  fi

  if [[ "$confirm" == [yY] ]]; then
    find "${SOURCES_DIR}" -maxdepth 1 -type l -delete
    find "${SOURCES_DIR}" -mindepth 1 -maxdepth 1 ! -type l -exec rm -rf {} +
    echo "Cleaned ${SOURCES_DIR}"
  else
    echo "Aborted."
    exit 0
  fi
fi

mkdir -p "${DATA_DIR}/duckdb"
mkdir -p "${SOURCES_DIR}"/{crossref_metadata,datacite,openalex_works,ror,data_citation_corpus}

echo "Copying ROR"
cp -r "${UPSTREAM_ROR}/." "${SOURCES_DIR}/ror/"

echo "Copying Data Citation Corpus"
cp -r "${UPSTREAM_DATA_CITATION_CORPUS}/." "${SOURCES_DIR}/data_citation_corpus/"

echo "Subsetting Crossref Metadata"
dmpworks transform dataset-subset crossref-metadata "${UPSTREAM_CROSSREF_METADATA}" "${SOURCES_DIR}/crossref_metadata"

echo "Subsetting DataCite"
dmpworks transform dataset-subset datacite "${UPSTREAM_DATACITE}" "${SOURCES_DIR}/datacite"

echo "Subsetting OpenAlex Works"
dmpworks transform dataset-subset openalex-works "${UPSTREAM_OPENALEX_WORKS}" "${SOURCES_DIR}/openalex_works"

echo "Done. Subsets written to ${SOURCES_DIR}"