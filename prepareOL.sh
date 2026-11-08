#!/bin/bash

set -e

# Load deployment settings from .env beside this script, not the working directory.
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi


# Install oci-cli
sudo dnf update -y
sudo dnf install -y oraclelinux-developer-release-el9
sudo dnf install -y python39-oci-cli
sudo dnf install -y oracle-epel-release-el9
sudo dnf --enablerepo=ol9_developer_EPEL install -y htop sysstat

