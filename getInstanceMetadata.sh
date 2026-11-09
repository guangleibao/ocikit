#!/bin/bash
# Print the complete IMDSv2 /instance/ object as JSON, including nested metadata.
set -euo pipefail

OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi

for dependency in curl jq; do
    if ! command -v "$dependency" >/dev/null 2>&1; then
        echo "Error: $dependency is required; run the preparation script for your OS." >&2
        exit 127
    fi
done

OCIKIT_METADATA_BASE_URL=${OCI_METADATA_BASE_URL:-http://169.254.169.254/opc/v2}
OCIKIT_INSTANCE_METADATA=$(curl --noproxy '*' -fsS --connect-timeout 3 --max-time 10 \
    -H 'Authorization: Bearer Oracle' \
    "${OCIKIT_METADATA_BASE_URL%/}/instance/")

printf '%s\n' "$OCIKIT_INSTANCE_METADATA" |
    jq -e 'if type == "object" then . else error("Expected an instance metadata JSON object") end'
