#!/usr/bin/env bash

# Generates a subset of Crossref Metadata, DataCite and OpenAlex Works,
# by copying records associated with a specific ROR ID or institution
# name.
#
# Required environment variables:
#   SOURCE_DIR: path to the source data directory with raw datasets.
#   DEMO_DIR: path to the demo directory, the dataset subset will be saved inside the 'sources' folder within this directory.
#   ROR_ID: the institution's ROR ID, e.g. "01an7q238".
#   INSTITUTION_NAME: the institution's name, e.g. "University of California, Berkeley".

for var in SOURCE_DIR DEMO_DIR ROR_ID INSTITUTION_NAME; do
  if [ -z "${!var}" ]; then
    echo "Environment variable $var is not set"
    exit 1
  fi
done

# Clean demo sources directory
DEMO_SOURCES_DIR="${DEMO_DIR}/sources"
echo "Clean demo sources directory..."
read -p "Are you sure you want to delete the demo sources directory '$DEMO_SOURCES_DIR'? [y/N] " confirm
if [[ "$confirm" == [yY] ]]; then
  rm -rf "${DEMO_SOURCES_DIR}"
  echo "Deleted ${DEMO_SOURCES_DIR}"
else
  echo "Aborted."
fi

mkdir -p "${DEMO_SOURCES_DIR}"/{datacite,openalex_works,crossref_metadata,openalex_funders,ror}

echo "Copying OpenAlex Funders"
cp -r "${SOURCE_DIR}/openalex/openalex-snapshot/data/funders/." "${DEMO_DIR}/sources/openalex_funders/"

echo "Copying ROR"
cp "${SOURCE_DIR}/ror/v1.63-2025-04-03-ror-data/v1.63-2025-04-03-ror-data_schema_v2.json" "${DEMO_DIR}/sources/ror/v1.63-2025-04-03-ror-data_schema_v2.json"

dmpworks transform dataset-subset crossref-metadata "${ROR_ID}" "${SOURCE_DIR}/crossref_metadata/March 2025 Public Data File from Crossref" "${DEMO_DIR}/sources/crossref_metadata" --institution-name="${INSTITUTION_NAME}"
dmpworks transform dataset-subset datacite "${ROR_ID}" "${SOURCE_DIR}/datacite/DataCite_Public_Data_File_2024/dois" "${DEMO_DIR}/sources/datacite" --institution-name="${INSTITUTION_NAME}"
dmpworks transform dataset-subset openalex-works "${ROR_ID}" "${SOURCE_DIR}/openalex/openalex-snapshot/data/works" "${DEMO_DIR}/sources/openalex_works"


