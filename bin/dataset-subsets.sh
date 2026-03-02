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

mkdir -p "${DATA_DIR}/duckdb"

mkdir -p "${DEMO_SOURCES_DIR}"/{datacite,openalex_works,crossref_metadata,ror,data_citation_corpus}

echo "Copying ROR"
cp -r "${SOURCE_DIR}/ror/." "${DATA_DIR}/sources/ror/"

echo "Copying Data Citation Corpus"
cp -r "${SOURCE_DIR}/data_citation_corpus/." "${DATA_DIR}/sources/data_citation_corpus/"

dmpworks transform dataset-subset crossref-metadata "${SOURCE_DIR}/crossref_metadata" "${DATA_DIR}/sources/crossref_metadata"
dmpworks transform dataset-subset datacite "${SOURCE_DIR}/datacite/dois" "${DATA_DIR}/sources/datacite"
dmpworks transform dataset-subset openalex-works "${SOURCE_DIR}/openalex/openalex-snapshot/data/works" "${DATA_DIR}/sources/openalex_works"