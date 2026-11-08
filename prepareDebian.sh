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

# Python Kafka client: wheels include librdkafka; no local Kafka broker is needed.
python3 -c 'import sys; sys.exit("Python 3.10+ is required" if sys.version_info < (3, 10) else 0)'
python3 -m venv "$OCIKIT_DIR/.venv"
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

