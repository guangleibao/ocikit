#!/bin/bash

set -e

# Load deployment settings from .env beside this script, not the working directory.
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi


sudo apt update -y
sudo apt upgrade -y
sudo apt install -y curl ca-certificates python3 python3-venv htop sysstat
curl -fsSL \
  https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh \
  -o /tmp/install-oci-cli.sh

bash /tmp/install-oci-cli.sh --accept-all-defaults
export PATH="$HOME/bin:$PATH"

