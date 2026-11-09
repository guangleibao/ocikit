#!/bin/bash
# Compatibility entry point for the original misspelled filename.
set -eu
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "$OCIKIT_DIR/getInstanceMetadata.sh" "$@"
