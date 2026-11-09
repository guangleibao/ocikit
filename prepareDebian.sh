#!/bin/bash

set -e

# Load deployment settings from .env beside this script, not the working directory.
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi

rm -rf /home/debian/lib/oracle-cli
sudo apt update -y
sudo apt upgrade -y
sudo apt install -y curl ca-certificates jq python3 python3-venv htop sysstat
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

# Rebuild the native Java Kafka consumer on every preparation run.
sudo apt install -y openjdk-17-jdk maven
echo "Rebuilding Java Kafka consumer (including OCI instance-principal support)..."
mvn -B -f "$OCIKIT_DIR/kafka-java/pom.xml" clean package
echo "Java Kafka consumer ready: $OCIKIT_DIR/kafka-java/target/stream-consumer.jar"

cat <<'PREPARATION_SUMMARY'
Prerequisite preparation complete:
  OCI CLI and system utilities installed.
  Python 3.11, OCI SDK, and Python Kafka client provisioned with uv in .venv.
  JDK 17 and Maven installed; Java Kafka client rebuilt.
Configure deployment settings in the adjacent .env as needed.
Ensure the required IAM permissions, network connectivity, and DNS are available.
PREPARATION_SUMMARY
