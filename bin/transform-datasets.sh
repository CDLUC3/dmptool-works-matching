#!/usr/bin/env bash

# Transforms source datasets into parquet files for the DMP Tool
# Related Works matching pipeline.
#
# Reads from ${DATA_DIR}/sources/ (populated by either dataset-subsets.sh
# or link-upstream.sh) and writes to ${DATA_DIR}/transform/.
#
# Required environment variables:
#   DATA_DIR: Working data directory.

set -euo pipefail

if [ -z "${DATA_DIR:-}" ]; then
  echo "Error: environment variable DATA_DIR is not set" >&2
  exit 1
fi

SOURCES_DIR="${DATA_DIR}/sources"
TRANSFORM_DIR="${DATA_DIR}/transform"

if [ ! -d "${SOURCES_DIR}" ]; then
  echo "Error: sources directory '${SOURCES_DIR}' does not exist." >&2
  echo "Run bin/dataset-subsets.sh or bin/link-upstream.sh first." >&2
  exit 1
fi

if [ -d "${TRANSFORM_DIR}" ]; then
  read -p "Delete contents of '${TRANSFORM_DIR}' and regenerate transforms? [y/N] " confirm
  if [[ "$confirm" == [yY] ]]; then
    find "${TRANSFORM_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    echo "Cleaned ${TRANSFORM_DIR}"
  else
    echo "Aborted."
    exit 0
  fi
fi

mkdir -p "${TRANSFORM_DIR}"/{crossref_metadata,datacite,openalex_works,ror,opensearch,data_citation_corpus}

echo "Initialising DOI state"
dmpworks sqlmesh init-doi-state "${TRANSFORM_DIR}/opensearch/doi_state_00000.parquet"

echo "Copying ROR"
cp -r "${SOURCES_DIR}/ror/." "${TRANSFORM_DIR}/ror/"

echo "Copying Data Citation Corpus"
cp -r "${SOURCES_DIR}/data_citation_corpus/." "${TRANSFORM_DIR}/data_citation_corpus/"

echo "Transforming Crossref Metadata"
dmpworks transform crossref-metadata "${SOURCES_DIR}/crossref_metadata" "${TRANSFORM_DIR}/crossref_metadata"

echo "Transforming OpenAlex Works"
dmpworks transform openalex-works "${SOURCES_DIR}/openalex_works" "${TRANSFORM_DIR}/openalex_works"

echo "Transforming DataCite"
dmpworks transform datacite "${SOURCES_DIR}/datacite" "${TRANSFORM_DIR}/datacite"

echo "Done. Transforms written to ${TRANSFORM_DIR}"