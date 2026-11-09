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
sudo dnf install -y curl ca-certificates
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

# Build the native Java Kafka consumer used by --api kafka-java.
sudo dnf install -y java-17-openjdk-devel maven
mvn -B -f "$OCIKIT_DIR/kafka-java/pom.xml" package

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
access is required for private pools). No local broker is required; JDK 17 and the Java Kafka client are installed.
Examples:
  ./runStreamConsumerDemo.sh STREAM --group-name my-group --api oci
  ./runStreamConsumerDemo.sh STREAM --group-name kafka-group --endpoint private --api kafka-python
  ./runStreamConsumerDemo.sh STREAM --group-name kafka-group --endpoint private --api kafka-java
KAFKA_SETUP

