#!/bin/bash

set -e

# Load deployment settings from .env beside this script, not the working directory.
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi


COMPARTMENT_ID=${OCI_COMPARTMENT_ID:-}
if [ -z "$COMPARTMENT_ID" ]; then
  : "${OCI_METADATA_BASE_URL:?Set OCI_METADATA_BASE_URL or OCI_COMPARTMENT_ID in ocikit/.env}"
  COMPARTMENT_ID=$(curl --noproxy '*' -fsS --max-time 10 \
    -H 'Authorization: Bearer Oracle' \
    "${OCI_METADATA_BASE_URL%/}/instance/compartmentId")
fi
: "${OCI_AUTH:?Set OCI_AUTH in ocikit/.env}"
set -- oci os bucket list --all --compartment-id "$COMPARTMENT_ID" --auth "$OCI_AUTH"
if [ -n "${OCI_PROFILE:-}" ]; then
  set -- "$@" --profile "$OCI_PROFILE"
fi
if [ -n "${OCI_REGION:-}" ]; then
  set -- "$@" --region "$OCI_REGION"
fi
"$@"
