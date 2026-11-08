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

# Python Kafka client: wheels include librdkafka; no local Kafka broker is needed.
sudo dnf install -y ca-certificates python3.11 python3.11-pip
python3.11 -m venv "$OCIKIT_DIR/.venv"
"$OCIKIT_DIR/.venv/bin/python" -m pip install --upgrade pip
"$OCIKIT_DIR/.venv/bin/python" -m pip install 'oci>=2.189.0,<3' 'confluent-kafka>=2.12,<3'

cat <<'KAFKA_SETUP'
Kafka client installed in .venv beside these scripts.
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
Example: ./runStreamConsumer.sh STREAM --group-name kafka-group --endpoint private --kafka
KAFKA_SETUP

