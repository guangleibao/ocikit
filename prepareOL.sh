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
sudo dnf --enablerepo=ol9_developer_EPEL install -y htop sysstat git

# Provision the stream runtime with uv (independent of the OCI CLI's Python).
sudo dnf install -y curl ca-certificates jq
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
sudo dnf install -y java-17-openjdk-devel maven
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
