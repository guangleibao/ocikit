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

# Provision the stream runtime with uv (independent of the OCI CLI's Python).
export PATH="$HOME/.local/bin:$PATH"
if ! command -v uv >/dev/null 2>&1; then
    OCIKIT_UV_INSTALLER=$(mktemp)
    trap 'rm -f -- "$OCIKIT_UV_INSTALLER"' EXIT
    curl -fsSL https://astral.sh/uv/install.sh -o "$OCIKIT_UV_INSTALLER"
    UV_INSTALL_DIR="$HOME/.local/bin" UV_NO_MODIFY_PATH=1 sh "$OCIKIT_UV_INSTALLER"
    rm -f -- "$OCIKIT_UV_INSTALLER"
    trap - EXIT
fi
uv python install 3.11
uv venv --python 3.11 --managed-python --allow-existing "$OCIKIT_DIR/.venv"
# Kafka wheels include librdkafka; no local Kafka broker is needed.
uv pip install --python "$OCIKIT_DIR/.venv/bin/python" 'oci>=2.189.0,<3' 'confluent-kafka>=2.12,<3'

cat <<'KAFKA_SETUP'
Python 3.11 and Kafka client provisioned with uv in .venv beside these scripts.
Configure these entries in the adjacent .env (quote shell-special characters):
  OCI_KAFKA_USERNAME='tenancyName/domain/username/streamPoolId'
  OCI_KAFKA_AUTH_TOKEN='your OCI user auth token, not your console password'
Create an auth token in OCI Console > user details > Auth tokens.
Grant the token's user access to the stream; instance principals still discover
the stream/pool and need read streams and read stream-pools permissions.
Bootstrap servers are discovered from the pool. Optional override:
  OCI_KAFKA_BOOTSTRAP_SERVERS='host:9092'
Allow TLS traffic to the pool Kafka endpoint on port 9092 (private DNS/network
access is required for private pools). No local broker or Java is required.
Example: ./runStreamConsumerDemo.sh STREAM --group-name kafka-group --endpoint private --kafka
KAFKA_SETUP

