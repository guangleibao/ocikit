#!/bin/sh
# Standalone OCI SDK producer. Prefer the caller's .venv or active virtualenv.
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
exec "$@" <<'OCI_STREAM_PRODUCER_PY'
# /// script
# requires-python = ">=3.10"
# dependencies = ["oci>=2.189.0,<3"]
# ///
"""Stress-test an existing OCI stream using instance principals and the Python SDK.

No OCI CLI, OCI credentials config, SSO profile or API key is used. Run on an OCI instance.
The VM's dynamic group needs permission to use stream-push. Endpoint discovery
also needs read streams for name lookup and endpoint discovery, and read
stream-pools when selecting a private or public endpoint. The default
lookup scope is OCI_COMPARTMENT_ID, required in the adjacent .env file.
Stream name, endpoint and region can also default from that file.
A private endpoint requires a private network path and DNS resolution from this instance.

--endpoint private|public selects a stream whose pool has that endpoint type;
it does not change the pool's network configuration. With no endpoint directive,
the stream's configured messages endpoint is discovered automatically.

Payloads contain random bytes, reused within a worker. Unique keys spread load
across partitions; --key deliberately puts the load on one partition. A batch
stays below 1,000,000 decoded key+value bytes. No automatic publish retries or
resends are performed. Timeouts/5xx are uncertain delivery, not proven rejection.
By default, keep producing until Ctrl-C. An explicit --duration limits new
requests; in-flight requests drain up to their timeouts. --max-messages also
stops the run when the requested number of messages has been attempted.
Exit: 0 all acknowledged, 1 rejected/uncertain/fatal errors, 130 interrupted.
"""
import argparse
import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import math
import os
from pathlib import Path
import signal
import sys
import threading
import time
from urllib.parse import urlsplit
import uuid

MAX_BATCH_BYTES = 1_000_000
KEY_BUDGET = 96
LATENCY_BOUNDS = (.001, .002, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5, 10, 30, 60, float('inf'))
USAGE_EXAMPLES = """Examples:
  ./runStreamProducerDemo.sh <stream-name> --endpoint private
  ./runStreamProducerDemo.sh <stream-name> --endpoint public
  ./runStreamProducerDemo.sh <stream-name> --duration 60 --workers 4
  ./runStreamProducerDemo.sh <stream-name> --rate 2000 --max-messages 100000
  ./runStreamProducerDemo.sh <stream-name> --endpoint https://<messages-host> --dry-run
  ./runStreamProducerDemo.sh --endpoint private --dry-run  # stream name from .env
"""


class Allocator:
    """Bound total messages and globally pace batch completion slots."""
    def __init__(self, deadline, batch_size, rate, maximum, started):
        self.lock = threading.Lock()
        self.deadline, self.batch_size, self.rate, self.maximum = deadline, batch_size, rate, maximum
        self.next_message = 0
        self.next_slot = started

    def claim(self):
        with self.lock:
            now = time.monotonic()
            if now >= self.deadline:
                return None
            count = self.batch_size
            if self.maximum is not None:
                count = min(count, self.maximum - self.next_message)
            if count <= 0:
                return None
            due = now
            if self.rate:
                due = max(now, self.next_slot) + count / self.rate
                if due >= self.deadline:
                    return None
                self.next_slot = due
            first = self.next_message
            self.next_message += count
            return first, count, due


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = Counter()
        self.errors = Counter()
        self.partitions = Counter()
        self.histogram = [0] * len(LATENCY_BOUNDS)
        self.latency_total = 0.0
        self.latency_max = 0.0
        self.fatal = None

    def record(self, attempted, acknowledged, rejected, uncertain, throttled,
               latency, message_size, errors=None, partitions=None, fatal=None):
        with self.lock:
            self.counts.update(requests=1, attempted=attempted, acknowledged=acknowledged,
                rejected=rejected, uncertain=uncertain, throttled=throttled,
                payload_bytes=acknowledged * message_size,
                failed_requests=int(bool(rejected or uncertain)))
            self.errors.update(errors or {})
            self.partitions.update(partitions or {})
            self.latency_total += latency
            self.latency_max = max(self.latency_max, latency)
            for i, bound in enumerate(LATENCY_BOUNDS):
                if latency <= bound:
                    self.histogram[i] += 1
                    break
            if fatal and self.fatal is None:
                self.fatal = fatal

    def snapshot(self):
        with self.lock:
            counts = dict(self.counts)
            requests = counts.get('requests', 0)
            def percentile(fraction):
                goal = math.ceil(requests * fraction)
                if not goal:
                    return 0
                total = 0
                for count, bound in zip(self.histogram, LATENCY_BOUNDS):
                    total += count
                    if total >= goal:
                        return min(bound, self.latency_max) * 1000
                return 0
            return {**{k: counts.get(k, 0) for k in ('requests', 'attempted', 'acknowledged',
                'rejected', 'uncertain', 'throttled', 'payload_bytes', 'failed_requests')},
                'errors': dict(self.errors), 'partitions': dict(self.partitions),
                'latency_avg_ms': self.latency_total / requests * 1000 if requests else 0,
                'latency_p50_upper_ms': percentile(.50), 'latency_p95_upper_ms': percentile(.95),
                'latency_p99_upper_ms': percentile(.99), 'latency_max_ms': self.latency_max * 1000,
                'fatal': self.fatal}


def analyze_result(data, count):
    entries = getattr(data, 'entries', None)
    if entries is None or len(entries) != count:
        return 0, 0, count, 0, {'MalformedPutMessagesResult': count}, {}
    acknowledged = rejected = throttled = 0
    errors, partitions = Counter(), Counter()
    for entry in entries:
        error = getattr(entry, 'error', None)
        if error:
            rejected += 1
            errors[str(error)] += 1
            if 'throttl' in str(error).lower() or str(error) == '429':
                throttled += 1
        else:
            acknowledged += 1
            partitions[str(entry.partition)] += 1
    return acknowledged, rejected, 0, throttled, errors, partitions


def worker(oci, client, args, allocator, metrics, stop, run_id, worker_id):
    payload = base64.b64encode(os.urandom(args.message_size)).decode('ascii')
    while not stop.is_set():
        allocation = allocator.claim()
        if allocation is None:
            return
        first, count, due = allocation
        if stop.wait(max(0, due - time.monotonic())):
            return
        if time.monotonic() >= allocator.deadline:
            return
        messages = []
        for number in range(first, first + count):
            key = args.key.encode('utf-8') if args.key is not None else f'{run_id}:{worker_id}:{number}'.encode()
            messages.append(oci.streaming.models.PutMessagesDetailsEntry(
                key=base64.b64encode(key).decode('ascii'), value=payload))
        started = time.monotonic()
        fatal = None
        errors, partitions = Counter(), Counter()
        acknowledged = rejected = uncertain = throttled = 0
        try:
            response = client.put_messages(args.stream_id,
                oci.streaming.models.PutMessagesDetails(messages=messages),
                retry_strategy=oci.retry.NoneRetryStrategy())
            acknowledged, rejected, uncertain, throttled, errors, partitions = analyze_result(response.data, count)
            if uncertain:
                fatal = 'PutMessages returned an unexpected entry count; delivery is uncertain.'
        except oci.exceptions.ServiceError as error:
            status = int(error.status or 0)
            label = f'HTTP {status} {error.code}'
            errors[label] = count
            if 400 <= status < 500 and status not in (408,):
                rejected = count
            else:
                uncertain = count
            if status == 429:
                throttled = count
            if status in (400, 401, 403, 404, 413):
                fatal = f'{label}: {error.message}'
        except (oci.exceptions.RequestException, OSError) as error:
            uncertain = count
            errors[type(error).__name__] = count
        except Exception as error:
            uncertain = count
            errors[type(error).__name__] = count
            fatal = f'{type(error).__name__}: {error}'
        latency = time.monotonic() - started
        metrics.record(count, acknowledged, rejected, uncertain, throttled, latency,
                       args.message_size, errors, partitions, fatal)
        if fatal:
            stop.set()
        if rejected or uncertain:
            stop.wait(args.error_backoff)


def report(snapshot, elapsed, previous=None, interval=None, final=False):
    delta = snapshot['acknowledged'] - (previous['acknowledged'] if previous else 0)
    span = interval if interval is not None else elapsed
    rate = delta / span if span > 0 else 0
    print(f"{'FINAL' if final else 'RUN'} {elapsed:7.1f}s | "
          f"ack {snapshot['acknowledged']:,} | {rate:,.0f} msg/s | "
          f"{(snapshot['payload_bytes'] - (previous['payload_bytes'] if previous else 0)) / max(span, .000001) / 1048576:.2f} payload MiB/s | "
          f"rejected {snapshot['rejected']:,} | uncertain {snapshot['uncertain']:,} | "
          f"throttled {snapshot['throttled']:,} | p95 <= {snapshot['latency_p95_upper_ms']:.1f} ms", flush=True)


def stress(oci, clients, args, batch_size, run_id):
    stop, interrupted = threading.Event(), threading.Event()
    metrics = Metrics()
    old_signals = {}
    def interrupt(signum, frame):
        interrupted.set()
        stop.set()
    for signum in (signal.SIGINT, signal.SIGTERM):
        old_signals[signum] = signal.signal(signum, interrupt)
    started = time.monotonic()
    deadline = started + args.duration if args.duration is not None else float('inf')
    allocator = Allocator(deadline, batch_size, args.rate, args.max_messages, started)
    previous, previous_time = metrics.snapshot(), started
    try:
        with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix='stream-producer') as pool:
            tasks = [pool.submit(worker, oci, client, args, allocator, metrics, stop, run_id, i)
                     for i, client in enumerate(clients)]
            next_report = started + args.report_interval
            while not all(task.done() for task in tasks):
                now = time.monotonic()
                if now >= allocator.deadline:
                    stop.set()
                if now >= next_report:
                    snapshot = metrics.snapshot()
                    report(snapshot, now - started, previous, now - previous_time)
                    previous, previous_time = snapshot, now
                    next_report = now + args.report_interval
                # Continue draining/reporting after stop; Event.wait would busy-loop.
                time.sleep(.1)
            for task in tasks:
                task.result()
    finally:
        stop.set()
        for signum, handler in old_signals.items():
            signal.signal(signum, handler)
    elapsed = time.monotonic() - started
    snapshot = metrics.snapshot()
    report(snapshot, elapsed, final=True)
    print(f"Requests: {snapshot['requests']:,}; attempted messages: {snapshot['attempted']:,}; "
          f"failed/partial requests: {snapshot['failed_requests']:,}")
    print(f"Request latency: average {snapshot['latency_avg_ms']:.1f} ms; "
          f"p50 <= {snapshot['latency_p50_upper_ms']:.1f} ms; "
          f"p99 <= {snapshot['latency_p99_upper_ms']:.1f} ms; maximum {snapshot['latency_max_ms']:.1f} ms")
    print('Acknowledged messages by partition: ' + json.dumps(snapshot['partitions'], sort_keys=True))
    if snapshot['errors']:
        print('Message error counts: ' + json.dumps(snapshot['errors'], sort_keys=True))
    if snapshot['fatal']:
        print('Stopped: ' + snapshot['fatal'], file=sys.stderr)
    summary = {'run_id': run_id, 'stream_id': args.stream_id, 'stream_name': args.stream_name, 'elapsed_seconds': elapsed,
               'workers': args.workers, 'message_size_bytes': args.message_size,
               'batch_size': batch_size, 'target_messages_per_second': args.rate,
               'interrupted': interrupted.is_set(), **snapshot}
    if args.json_report:
        args.json_report.expanduser().write_text(json.dumps(summary, indent=2) + '\n')
        print('JSON report: ' + str(args.json_report.expanduser()))
    if interrupted.is_set():
        return 130
    return int(bool(snapshot['rejected'] or snapshot['uncertain'] or snapshot['fatal']))


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return number


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError('must be finite and positive')
    return number


def resolve_stream(oci, admin, args):
    print(f'Resolving stream name {args.stream_name!r} in the configured compartment...', flush=True)
    streams = oci.pagination.list_call_get_all_results(admin.list_streams,
        compartment_id=args.compartment_id, name=args.stream_name, lifecycle_state='ACTIVE',
        retry_strategy=oci.retry.NoneRetryStrategy()).data
    matches = [stream for stream in streams if stream.name == args.stream_name and stream.lifecycle_state == 'ACTIVE']
    if not matches:
        raise RuntimeError(f'No ACTIVE stream named {args.stream_name!r} found in the selected scope.')
    if args.endpoint in ('private', 'public'):
        pools = {}
        selected = []
        for stream in matches:
            if stream.stream_pool_id not in pools:
                pool = admin.get_stream_pool(stream.stream_pool_id,
                    retry_strategy=oci.retry.NoneRetryStrategy()).data
                if pool.is_private is not True and pool.is_private is not False:
                    raise RuntimeError(f'Cannot determine endpoint type for stream pool {stream.stream_pool_id}.')
                pools[stream.stream_pool_id] = pool.is_private
            if pools[stream.stream_pool_id] == (args.endpoint == 'private'):
                selected.append(stream)
        matches = selected
        if not matches:
            raise RuntimeError(f'No ACTIVE stream named {args.stream_name!r} with a {args.endpoint} endpoint '
                               'found in the configured compartment. Endpoint type is configured on the stream pool.')
    if len(matches) > 1:
        qualifier = f' with a {args.endpoint} endpoint' if args.endpoint in ('private', 'public') else ''
        raise RuntimeError(f'Multiple ACTIVE streams named {args.stream_name!r}{qualifier} in the configured compartment; use a unique stream name.')
    return matches[0]


def main(argv=None):
    parser = argparse.ArgumentParser(prog='runStreamProducerDemo.sh', description=__doc__,
                                     epilog=USAGE_EXAMPLES,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('stream_name', nargs='?', default=os.getenv('OCI_STREAM_NAME') or None,
                        help='exact stream name (default: OCI_STREAM_NAME in .env)')
    parser.add_argument('--endpoint', default=os.getenv('OCI_STREAM_ENDPOINT') or None,
                        metavar='private|public|HTTPS_URL',
                        help='discover a private/public messages endpoint, or use an HTTPS URL '
                             '(default: OCI_STREAM_ENDPOINT in .env, otherwise discover configured endpoint)')
    parser.add_argument('--region', default=os.getenv('OCI_REGION') or None,
                        help='stream region (default: OCI_REGION in .env, otherwise instance region)')
    parser.add_argument('--duration', type=positive_float,
                        help='seconds to start new requests (default: run continuously until Ctrl-C)')
    parser.add_argument('--workers', type=positive_int, default=4, help='concurrent SDK clients (default: 4)')
    parser.add_argument('--batch-size', type=positive_int, default=100, help='maximum messages/request; shrinks to fit 1 MB (default: 100)')
    parser.add_argument('--message-size', type=positive_int, default=1024, help='decoded payload bytes/message (default: 1024)')
    parser.add_argument('--rate', type=float, default=0, help='global attempted messages/sec; 0 = unlimited (default)')
    parser.add_argument('--max-messages', type=positive_int, help='maximum attempted messages across all workers')
    parser.add_argument('--key', help='fixed UTF-8 key to stress one partition (default: unique keys)')
    parser.add_argument('--report-interval', type=positive_float, default=5)
    parser.add_argument('--connect-timeout', type=positive_float, default=5)
    parser.add_argument('--request-timeout', type=positive_float, default=30)
    parser.add_argument('--error-backoff', type=positive_float, default=.1, help='seconds to pause after rejected/uncertain batches')
    parser.add_argument('--json-report', type=Path, help='save final metrics as JSON to this local file')
    parser.add_argument('--debug', action='store_true', help='enable SDK debug logging')
    parser.add_argument('--dry-run', action='store_true', help='show plan without importing OCI, authenticating or sending requests')
    args = parser.parse_args(argv)
    duration_plan = f'{args.duration:g}s' if args.duration is not None else 'unlimited (until Ctrl-C)'
    if not args.stream_name or not args.stream_name.strip():
        parser.error('set OCI_STREAM_NAME in .env or provide a stream name')
    args.compartment_id = (os.getenv('OCI_COMPARTMENT_ID') or '').strip()
    if not args.compartment_id:
        parser.error('set OCI_COMPARTMENT_ID in .env beside this script')
    if not math.isfinite(args.rate) or args.rate < 0:
        parser.error('--rate must be finite and nonnegative')
    if args.workers > 128:
        parser.error('--workers must not exceed 128')
    key_budget = KEY_BUDGET if args.key is None else len(args.key.encode('utf-8'))
    if args.key == '':
        parser.error('--key must not be empty')
    per_message = key_budget + args.message_size
    if per_message > MAX_BATCH_BYTES:
        parser.error('message plus key exceeds the 1 MB decoded request budget')
    batch_size = min(args.batch_size, MAX_BATCH_BYTES // per_message)
    if args.rate:
        batch_size = min(batch_size, max(1, int(args.rate / 10)))
    if args.endpoint and args.endpoint not in ('private', 'public'):
        endpoint = urlsplit(args.endpoint)
        if (endpoint.scheme != 'https' or not endpoint.hostname or endpoint.username or endpoint.password or
                endpoint.query or endpoint.fragment or endpoint.path not in ('', '/')):
            parser.error('--endpoint must be private, public, or an HTTPS messages endpoint URL without path, credentials or query')
        args.endpoint = args.endpoint.rstrip('/')
    if args.dry_run:
        print(f'Dry run: instance-principal authentication; stream name={args.stream_name}')
        print('Lookup scope: configured compartment (.env)')
        endpoint_plan = args.endpoint or 'discover with StreamAdminClient.get_stream'
        if args.endpoint in ('private', 'public'):
            endpoint_plan = f'discover {args.endpoint} endpoint using stream pool type and StreamAdminClient.get_stream'
        print(f'Endpoint: {endpoint_plan}')
        print(f'Duration: {duration_plan}; workers: {args.workers}; effective batch size: {batch_size}; payload: {args.message_size} bytes')
        print(f'Rate: {args.rate:g} messages/sec (0=unlimited); maximum attempted: {args.max_messages or "unlimited"}')
        print(f'Decoded batch budget: <= {batch_size * per_message:,} bytes; keys: {"fixed/single partition" if args.key is not None else "unique/distributed"}')
        print('No SDK import, instance metadata requests, authentication, publication or report writes.')
        return 0
    try:
        import oci
        if args.debug:
            logging.basicConfig(level=logging.DEBUG)
        print('Authenticating with OCI instance principals...', flush=True)
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner(
            retry_strategy=oci.retry.NoneRetryStrategy(),
            federation_client_retry_strategy=oci.retry.NoneRetryStrategy())
        region = args.region or signer.region
        config = {'region': region}
        common = {'signer': signer, 'timeout': (args.connect_timeout, args.request_timeout),
                  'retry_strategy': oci.retry.NoneRetryStrategy()}
        endpoint = None if args.endpoint in ('private', 'public') else args.endpoint
        admin = oci.streaming.StreamAdminClient(config, **common)
        try:
            stream = resolve_stream(oci, admin, args)
            args.stream_id = stream.id
            print(f'Stream: {stream.name}; ID: {stream.id}', flush=True)
            if not endpoint:
                stream = admin.get_stream(args.stream_id, retry_strategy=oci.retry.NoneRetryStrategy()).data
                if stream.lifecycle_state != 'ACTIVE':
                    raise RuntimeError(f'Stream is {stream.lifecycle_state}; expected ACTIVE.')
                endpoint = stream.messages_endpoint
                print(f'Partitions: {stream.partitions}', flush=True)
        finally:
            admin.base_client.session.close()
        if not endpoint:
            raise RuntimeError('Stream has no messages endpoint; specify --endpoint.')
        print(f'Messages endpoint: {endpoint}\nWorkers: {args.workers}; batch: {batch_size}; payload: {args.message_size} bytes; '
              f'duration: {duration_plan}; rate: {args.rate:g} msg/s (0=unlimited)', flush=True)
        clients = []
        try:
            for _ in range(args.workers):
                clients.append(oci.streaming.StreamClient(config, endpoint, **common))
            return stress(oci, clients, args, batch_size, uuid.uuid4().hex)
        finally:
            for client in clients:
                client.base_client.session.close()
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        print(f'Error: {type(error).__name__}: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
OCI_STREAM_PRODUCER_PY
