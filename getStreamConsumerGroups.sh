#!/bin/sh
# Standalone OCI SDK consumer-group inspector. Prefer the caller's .venv or active virtualenv.
# Without a virtualenv, uv supplies the embedded Python SDK dependency.
set -eu
# Load deployment settings from .env beside this script, not the working directory.
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi

# Select the interpreter directly: activation scripts can depend on Bash variables
# such as OSTYPE and are unnecessary when launching an environment's Python.
OCIKIT_PYTHON=
if [ -d .venv ]; then
    OCIKIT_PYTHON="$(pwd)/.venv/bin/python"
elif [ -n "${VIRTUAL_ENV:-}" ]; then
    OCIKIT_PYTHON="$VIRTUAL_ENV/bin/python"
elif [ -d "$OCIKIT_DIR/.venv" ]; then
    OCIKIT_PYTHON="$OCIKIT_DIR/.venv/bin/python"
fi
if [ -n "$OCIKIT_PYTHON" ]; then
    if [ ! -x "$OCIKIT_PYTHON" ]; then
        echo "Error: virtual environment has no executable Python: $OCIKIT_PYTHON" >&2
        exit 127
    fi
    set -- "$OCIKIT_PYTHON" - "$@"
else
    if ! command -v uv >/dev/null 2>&1; then
        echo "Error: activate a Python environment with OCI SDK installed, or make uv available on PATH." >&2
        exit 127
    fi
    set -- uv run --no-project --script - "$@"
fi
exec "$@" <<'OCI_STREAM_GROUPS_PY'
# /// script
# requires-python = ">=3.10"
# dependencies = ["oci>=2.189.0,<3"]
# ///
"""Show OCI stream consumer group details as JSON on stdout.

Usage: ./getStreamConsumerGroups.sh STREAM_NAME [--endpoint private|public]

Probe exactly cp1 through cp9 with GetGroup. Existing groups include the full
partition reservation/committed offset details returned by OCI. No groups are
created, no messages are read, and no offsets are changed.

Run on an OCI instance using instance principals. Load OCI_COMPARTMENT_ID and
optional OCI_REGION/OCI_STREAM_ENDPOINT from .env beside this script. Stream
lookup uses the exact compartment, excluding child compartments. If the endpoint
setting is private/public, match that pool type; --endpoint overrides .env.
Otherwise discover the messages
endpoint (or use the configured HTTPS URL). A private endpoint needs private DNS
and network access. Permissions: read streams, use stream-pull, and read
stream-pools when filtering by endpoint type.

HTTP 404 means absent OR inaccessible: these probes are listed under
unavailable_groups. Other probe failures appear under errors and exit with 1.
Calls are paced at 2 requests/second; temporary service failures retry up to
three times with backoff. Diagnostics go to stderr.

Example:
  ./getStreamConsumerGroups.sh demo-stream-2p | jq '.groups'
  ./getStreamConsumerGroups.sh demo-stream-2p --endpoint private
  ./getStreamConsumerGroups.sh demo-stream-2p --endpoint public
"""
import argparse
import json
import os
import sys
import time
from urllib.parse import urlsplit


class Requests:
    def __init__(self, oci):
        self.oci = oci
        self.next_call = 0.0

    def call(self, method, *args, **kwargs):
        for attempt in range(4):
            time.sleep(max(0, self.next_call - time.monotonic()))
            self.next_call = time.monotonic() + .5
            try:
                return method(*args, retry_strategy=self.oci.retry.NoneRetryStrategy(), **kwargs)
            except (self.oci.exceptions.ServiceError, self.oci.exceptions.RequestException, OSError) as error:
                status = getattr(error, 'status', None)
                if isinstance(error, self.oci.exceptions.ServiceError) and not (
                        status in (408, 429) or 500 <= status < 600 and status != 501):
                    raise
                if attempt == 3:
                    raise
                print(f'Retrying OCI request after {type(error).__name__} (status={status}); '
                      f'waiting {2 ** attempt}s...', file=sys.stderr)
                time.sleep(2 ** attempt)


def resolve_stream(admin, requests, name, compartment_id, endpoint):
    streams = []
    page = None
    while True:
        parameters = dict(compartment_id=compartment_id, name=name, lifecycle_state='ACTIVE')
        if page:
            parameters['page'] = page
        response = requests.call(admin.list_streams, **parameters)
        streams.extend(stream for stream in response.data if stream.name == name and stream.lifecycle_state == 'ACTIVE')
        page = response.headers.get('opc-next-page')
        if not page:
            break
    if endpoint in ('private', 'public'):
        pools, selected = {}, []
        for stream in streams:
            if stream.stream_pool_id not in pools:
                pools[stream.stream_pool_id] = requests.call(admin.get_stream_pool, stream.stream_pool_id).data.is_private
            if pools[stream.stream_pool_id] is (endpoint == 'private'):
                selected.append(stream)
        streams = selected
    if not streams:
        raise RuntimeError(f'No matching ACTIVE stream named {name!r} in compartment {compartment_id!r}. '
                           'Check region, exact compartment, stream name, endpoint setting and read permissions.')
    if len(streams) != 1:
        raise RuntimeError(f'Multiple ACTIVE streams named {name!r}; use a unique name or '
                           'OCI_STREAM_ENDPOINT=private/public to narrow the lookup.')
    stream = requests.call(admin.get_stream, streams[0].id).data
    if stream.lifecycle_state != 'ACTIVE':
        raise RuntimeError(f'Stream is {stream.lifecycle_state}; expected ACTIVE.')
    return stream


def probe_groups(oci, client, requests, stream_id):
    groups, unavailable, errors = [], [], []
    for number in range(1, 10):
        name = f'cp{number}'
        try:
            groups.append(oci.util.to_dict(requests.call(client.get_group, stream_id, name).data))
        except oci.exceptions.ServiceError as error:
            detail = dict(group_name=name, status=error.status, code=error.code, message=error.message)
            if error.status == 404:
                detail['reason'] = 'not_found_or_not_authorized'
                unavailable.append(detail)
            else:
                errors.append(detail)
        except (oci.exceptions.RequestException, OSError) as error:
            errors.append(dict(group_name=name, error=type(error).__name__, message=str(error)))
    return groups, unavailable, errors


def main(argv=None):
    parser = argparse.ArgumentParser(prog='getStreamConsumerGroups.sh', description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('stream_name', help='exact stream name; probes cp1 through cp9')
    parser.add_argument('--endpoint', choices=('private', 'public'),
                        help='select stream pool endpoint type (overrides OCI_STREAM_ENDPOINT in .env)')
    args = parser.parse_args(argv)
    compartment_id = (os.getenv('OCI_COMPARTMENT_ID') or '').strip()
    if not args.stream_name.strip():
        parser.error('stream name must not be empty')
    if not compartment_id:
        parser.error('set OCI_COMPARTMENT_ID in .env beside this script')
    endpoint = args.endpoint or (os.getenv('OCI_STREAM_ENDPOINT') or '').strip() or None
    if endpoint and endpoint not in ('private', 'public'):
        parsed = urlsplit(endpoint)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or
                parsed.query or parsed.fragment or parsed.path not in ('', '/')):
            parser.error('OCI_STREAM_ENDPOINT must be private, public, or an HTTPS messages endpoint URL')
        endpoint = endpoint.rstrip('/')
    try:
        import oci
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner(
            retry_strategy=oci.retry.NoneRetryStrategy(),
            federation_client_retry_strategy=oci.retry.NoneRetryStrategy())
        region = os.getenv('OCI_REGION') or signer.region
        config = {'region': region}
        common = dict(signer=signer, timeout=(5, 30), retry_strategy=oci.retry.NoneRetryStrategy())
        requests = Requests(oci)
        print(f'Resolving {args.stream_name!r}; region={region}; compartment={compartment_id}...', file=sys.stderr)
        admin = oci.streaming.StreamAdminClient(config, **common)
        try:
            stream = resolve_stream(admin, requests, args.stream_name, compartment_id, endpoint)
        finally:
            admin.base_client.session.close()
        if endpoint in (None, 'private', 'public'):
            endpoint = stream.messages_endpoint
        if not endpoint:
            raise RuntimeError('Stream has no messages endpoint.')
        client = oci.streaming.StreamClient(config, endpoint, **common)
        try:
            groups, unavailable, errors = probe_groups(oci, client, requests, stream.id)
        finally:
            client.base_client.session.close()
        result = dict(stream_name=stream.name, stream_id=stream.id, region=region,
                      compartment_id=compartment_id, messages_endpoint=endpoint,
                      groups=groups, unavailable_groups=unavailable, errors=errors)
        print(json.dumps(result, indent=2))
        print(f'Probed cp1-cp9: {len(groups)} found, {len(unavailable)} absent or inaccessible, '
              f'{len(errors)} failed.', file=sys.stderr)
        return int(bool(errors))
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        print(f'Error: {type(error).__name__}: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
OCI_STREAM_GROUPS_PY
