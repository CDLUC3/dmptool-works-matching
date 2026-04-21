#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <ground_truth_file> <output_dir>" >&2
  echo "" >&2
  echo "  <ground_truth_file>  CSV of (dmp_doi,work_doi,status) judgements" >&2
  echo "  <output_dir>         Directory for metrics CSVs. Subdirectories" >&2
  echo "                       no_awards/, awards/, relations/ are created." >&2
  exit 1
fi

GROUND_TRUTH_FILE="$1"
OUTPUT_DIR="$2"

if [ ! -f "${GROUND_TRUTH_FILE}" ]; then
  echo "Error: ground truth file '${GROUND_TRUTH_FILE}' does not exist" >&2
  exit 1
fi

export OPENSEARCH_PORT=8080
TRUE_POSITIVE_OUTPUTS="${OUTPUT_DIR}/awards/tp_outputs.csv"

mkdir -p \
  "${OUTPUT_DIR}/no_awards" \
  "${OUTPUT_DIR}/awards" \
  "${OUTPUT_DIR}/relations"

# 1. No awards / no identifiers / no relations.
#    On: authors, institutions, funders, content.
dmpworks opensearch rank-metrics \
  "${GROUND_TRUTH_FILE}" \
  dmps-index \
  works-index \
  "${OUTPUT_DIR}/no_awards/metrics.csv" \
  --max-results=100 \
  --ks=10 20 100 \
  --disable-features relations awards funded_dois

# 2. Add funded_dois + awards back in; still no relations.
#    On: funded_dois, authors, institutions, funders, awards, content.
#    Also writes the true-positive outputs that the relations run will inject.
dmpworks opensearch rank-metrics \
  "${GROUND_TRUTH_FILE}" \
  dmps-index \
  works-index \
  "${OUTPUT_DIR}/awards/metrics.csv" \
  --max-results=100 \
  --ks=10 20 100 \
  --disable-features relations \
  --true-positive-published-outputs-file="${TRUE_POSITIVE_OUTPUTS}"

# 3. Relations test: everything on, relations driven by the injected TP set from run 2.
dmpworks opensearch rank-metrics \
  "${GROUND_TRUTH_FILE}" \
  dmps-index \
  works-index \
  "${OUTPUT_DIR}/relations/metrics.csv" \
  --max-results=100 \
  --ks=10 20 100 \
  --inject-published-outputs-file="${TRUE_POSITIVE_OUTPUTS}"
