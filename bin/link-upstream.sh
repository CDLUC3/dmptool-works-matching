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
mkdir -p "${SOURCES_DIR}"

echo "Linking upstream datasets into ${SOURCES_DIR}"

ln -sfn "${UPSTREAM_CROSSREF_METADATA}" "${SOURCES_DIR}/crossref_metadata"
ln -sfn "${UPSTREAM_DATACITE}" "${SOURCES_DIR}/datacite"
ln -sfn "${UPSTREAM_OPENALEX_WORKS}" "${SOURCES_DIR}/openalex_works"
ln -sfn "${UPSTREAM_ROR}" "${SOURCES_DIR}/ror"
ln -sfn "${UPSTREAM_DATA_CITATION_CORPUS}" "${SOURCES_DIR}/data_citation_corpus"

echo "Done. Symlinks:"
ls -la "${SOURCES_DIR}"/
