#!/bin/bash

set -e

# Load deployment settings from .env beside this script, not the working directory.
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi


: "${OCI_METADATA_BASE_URL:?Set OCI_METADATA_BASE_URL in ocikit/.env}"

curl --noproxy '*' -fsS --max-time 10 \
  -H 'Authorization: Bearer Oracle' \
  "${OCI_METADATA_BASE_URL%/}/instance/" |
  jq '{id, displayName, compartmentId, region}'
