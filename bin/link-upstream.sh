#!/usr/bin/env bash

# Symlinks upstream datasets into the standardised ${DATA_DIR}/sources/ layout.
# Use this when you want to run transform-datasets.sh directly against the
# upstream data, skipping the subsetting step.
#
# Required environment variables:
#   DATA_DIR: Working data directory. Symlinks are created in ${DATA_DIR}/sources/.
#   UPSTREAM_CROSSREF_METADATA: Path to the upstream Crossref Metadata dataset.
#   UPSTREAM_DATACITE: Path to the upstream DataCite dataset.
#   UPSTREAM_OPENALEX_WORKS: Path to the upstream OpenAlex Works dataset.
#   UPSTREAM_ROR: Path to the upstream ROR dataset.
#   UPSTREAM_DATA_CITATION_CORPUS: Path to the upstream Data Citation Corpus.

set -euo pipefail

# shellcheck source=load-env.sh
source "$(dirname "$0")/load-env.sh"

REQUIRED_VARS=(
  DATA_DIR
  UPSTREAM_CROSSREF_METADATA
  UPSTREAM_DATACITE
  UPSTREAM_OPENALEX_WORKS
  UPSTREAM_ROR
  UPSTREAM_DATA_CITATION_CORPUS
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
    echo "Sources directory contains symlinks:"
    find "${SOURCES_DIR}" -maxdepth 1 -type l -printf "  %f -> %l\n"
    read -p "Remove and re-create symlinks? [y/N] " confirm
  else
    read -p "Delete contents of '${SOURCES_DIR}' and create symlinks? [y/N] " confirm
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

mkdir -p "${SOURCES_DIR}"

echo "Linking upstream datasets into ${SOURCES_DIR}"

ln -s "${UPSTREAM_CROSSREF_METADATA}" "${SOURCES_DIR}/crossref_metadata"
ln -s "${UPSTREAM_DATACITE}" "${SOURCES_DIR}/datacite"
ln -s "${UPSTREAM_OPENALEX_WORKS}" "${SOURCES_DIR}/openalex_works"
ln -s "${UPSTREAM_ROR}" "${SOURCES_DIR}/ror"
ln -s "${UPSTREAM_DATA_CITATION_CORPUS}" "${SOURCES_DIR}/data_citation_corpus"

echo "Done. Symlinks:"
ls -la "${SOURCES_DIR}"/
