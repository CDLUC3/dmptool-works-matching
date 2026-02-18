#!/usr/bin/env bash

# Makes the parquet files for the DMP Tool Related Works matching.
#
# Required environment variables:
#   DATA_DIR: path to the data folder.

for var in DATA_DIR; do
  if [ -z "${!var}" ]; then
    echo "Environment variable $var is not set"
    exit 1
  fi
done

SOURCES_DIR="${DATA_DIR}/sources"
TRANSFORM_DIR="${DATA_DIR}/transform"

# Clean transform directory
echo "Clean transform directory..."
read -p "Are you sure you want to delete the transform directory '$TRANSFORM_DIR'? [y/N] " confirm
if [[ "$confirm" == [yY] ]]; then
  rm -rf "${TRANSFORM_DIR}"
  echo "Deleted ${TRANSFORM_DIR}"
else
  echo "Aborted."
fi

mkdir -p "${TRANSFORM_DIR}"/{dmps,datacite,openalex_works,crossref_metadata,openalex_funders,ror,opensearch/parquets}
dmpworks sqlmesh init-doi-state "${TRANSFORM_DIR}/opensearch/parquets/doi_state_00000.parquet"
dmpworks transform dmps "${SOURCES_DIR}/dmps" "${TRANSFORM_DIR}/dmps"
dmpworks transform crossref-metadata "${SOURCES_DIR}/crossref_metadata" "${TRANSFORM_DIR}/crossref_metadata"
dmpworks transform openalex-works "${SOURCES_DIR}/openalex_works" "${TRANSFORM_DIR}/openalex_works" --batch-size=2 --max-file-processes=2
dmpworks transform datacite "${SOURCES_DIR}/datacite" "${TRANSFORM_DIR}/datacite"
dmpworks transform ror "${SOURCES_DIR}/ror/v1.63-2025-04-03-ror-data_schema_v2.json" "${TRANSFORM_DIR}/ror"
