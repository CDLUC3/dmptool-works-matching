#!/usr/bin/env bash

# Source this script to load environment variables from a .env file.
# Resolves the env file path in priority order:
#   1. --env-file /path/to/.env  (command-line argument)
#   2. DMPWORKS_ENV_FILE          (environment variable)
#   3. .env.local                 (default)
#
# Usage (from another script):
#   source "$(dirname "$0")/load-env.sh"

ENV_FILE="${DMPWORKS_ENV_FILE:-.env.local}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --env-file=*) ENV_FILE="${1#--env-file=}"; shift ;;
    *) break ;;
  esac
done

if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

# Export so that dmpworks subprocesses load the same env file via python-dotenv
export DMPWORKS_ENV_FILE="${ENV_FILE}"
