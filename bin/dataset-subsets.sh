#!/usr/bin/env bash

# Generates a subset of Crossref Metadata, DataCite and OpenAlex Works,
# by copying records associated with a specific ROR ID or institution
# name.
#
# Required environment variables:
#   SOURCE_DIR: path to the source data directory with raw datasets.
#   DATA_DIR: path to the data directory, the dataset subset will be saved inside the 'sources' folder within this directory.
#   DATASET_SUBSET_INSTITUTIONS_PATH: Path to a JSON file containing a list of ROR IDs and institution names, e.g. [{"name": "University of California, San Diego", "ror": "0168r3w48"}]. Works authored by researchers from these institutions will be included.
#   DATASET_SUBSET_DOIS_PATH: Path to a JSON file with specific list of Work DOIs to include in the subset, e.g. ["10.0000/abc", "10.0000/123"].

for var in SOURCE_DIR DATA_DIR DATASET_SUBSET_INSTITUTIONS_PATH DATASET_SUBSET_DOIS_PATH; do
  if [ -z "${!var}" ]; then
    echo "Environment variable $var is not set"
    exit 1
  fi
done

# Clean demo sources directory
DEMO_SOURCES_DIR="${DATA_DIR}/sources"
echo "Clean demo sources directory..."
read -p "Are you sure you want to delete the demo sources directory '$DEMO_SOURCES_DIR'? [y/N] " confirm
if [[ "$confirm" == [yY] ]]; then
  rm -rf "${DEMO_SOURCES_DIR}"
  echo "Deleted ${DEMO_SOURCES_DIR}"
else
  echo "Aborted."
fi

mkdir -p "${DEMO_SOURCES_DIR}"/{dmps,datacite,openalex_works,crossref_metadata,openalex_funders,ror}

echo "Copying DMPs"
cp -r "${SOURCE_DIR}/dmps/." "${DATA_DIR}/sources/dmps/"

echo "Copying OpenAlex Funders"
cp -r "${SOURCE_DIR}/openalex/openalex-snapshot/data/funders/." "${DATA_DIR}/sources/openalex_funders/"

echo "Copying ROR"
cp "${SOURCE_DIR}/ror/v1.63-2025-04-03-ror-data/v1.63-2025-04-03-ror-data_schema_v2.json" "${DATA_DIR}/sources/ror/v1.63-2025-04-03-ror-data_schema_v2.json"

dmpworks transform dataset-subset crossref-metadata "${SOURCE_DIR}/crossref_metadata/March 2025 Public Data File from Crossref" "${DATA_DIR}/sources/crossref_metadata"
dmpworks transform dataset-subset datacite "${SOURCE_DIR}/datacite/DataCite_Public_Data_File_2024/dois" "${DATA_DIR}/sources/datacite"
dmpworks transform dataset-subset openalex-works "${SOURCE_DIR}/openalex/openalex-snapshot/data/works" "${DATA_DIR}/sources/openalex_works"
