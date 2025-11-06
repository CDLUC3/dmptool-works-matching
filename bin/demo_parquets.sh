#!/usr/bin/env bash

# Makes the parquet files for the DMP Tool Related Works matching.
#
# Required environment variables:
#   DEMO_SOURCES_DIR: path to the demo sources.
#   DEMO_TRANSFORM_DIR: path where the transformed parquet files will be saved.

for var in DEMO_SOURCES_DIR DEMO_TRANSFORM_DIR; do
  if [ -z "${!var}" ]; then
    echo "Environment variable $var is not set"
    exit 1
  fi
done

mkdir -p "${DEMO_TRANSFORM_DIR}"/{datacite,openalex_works,crossref_metadata,openalex_funders,ror}
dmpworks transform crossref-metadata "${DEMO_SOURCES_DIR}/crossref_metadata" "${DEMO_TRANSFORM_DIR}/crossref_metadata"
dmpworks transform openalex-works "${DEMO_SOURCES_DIR}/openalex_works" "${DEMO_TRANSFORM_DIR}/openalex_works"
dmpworks transform openalex-funders "${DEMO_SOURCES_DIR}/openalex_funders" "${DEMO_TRANSFORM_DIR}/openalex_funders"
dmpworks transform datacite "${DEMO_SOURCES_DIR}/datacite" "${DEMO_TRANSFORM_DIR}/datacite"
dmpworks transform ror "${DEMO_SOURCES_DIR}/ror/v1.63-2025-04-03-ror-data_schema_v2.json" "${DEMO_TRANSFORM_DIR}/ror"