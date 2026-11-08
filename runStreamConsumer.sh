#!/bin/sh
# Standalone OCI SDK consumer. Prefer the caller's .venv or active virtualenv.
# Without a virtualenv, uv supplies the embedded Python SDK dependency.
set -eu
# Load deployment settings from .env beside this script, not the working directory.
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$OCIKIT_DIR/.env" ]; then
    set -a
    . "$OCIKIT_DIR/.env"
    set +a
fi

if [ -f .venv/bin/activate ]; then
    . ./.venv/bin/activate
elif [ -z "${VIRTUAL_ENV:-}" ] && [ -f "$OCIKIT_DIR/.venv/bin/activate" ]; then
    . "$OCIKIT_DIR/.venv/bin/activate"
fi
if [ -n "${VIRTUAL_ENV:-}" ]; then
    if [ ! -x "$VIRTUAL_ENV/bin/python" ]; then
        echo "Error: virtual environment has no executable Python: $VIRTUAL_ENV/bin/python" >&2
        exit 127
    fi
    set -- "$VIRTUAL_ENV/bin/python" - "$@"
else
    if ! command -v uv >/dev/null 2>&1; then
        echo "Error: activate a Python environment with OCI SDK installed, or make uv available on PATH." >&2
        exit 127
    fi
    set -- uv run --no-project --script - "$@"
fi
exec "$@" <<'OCI_STREAM_CONSUMER_PY'
# /// script
# requires-python = ">=3.10"
# dependencies = ["oci>=2.189.0,<3", "confluent-kafka>=2.12,<3"]
# ///
"""Consume an OCI stream using a consumer group and instance principals.

Run on an OCI instance with permission to use stream-pull, read streams, and
read stream-pools when selecting --endpoint private|public. Load deployment
settings from .env beside this script. No OCI CLI or API key is used.
A private endpoint requires private network connectivity and DNS resolution.

All workers join the same group with unique instance names. OCI assigns the
partitions; workers exceeding the partition count may remain idle. New groups
start at TRIM_HORIZON by default. Existing groups resume committed offsets;
--cursor-type and --time apply only when the group is first created.

Each batch is processed (counted and printed by default) before an explicit offset
commit. Delivery is at least once: a failure can cause messages to be read again.
SDK calls are not retried automatically; errors stop the run with exit code 1.
A duration limits new reads; in-flight reads and commits drain to their timeouts.
Ctrl-C/SIGTERM drains fetched batches and exits 130. Metrics are per this process,
not the entire consumer group. Messages are printed as JSON lines to stdout;
Keys remain base64; values show the first 10 base64 characters followed by "...".
Runtime status and metrics go to stderr.
Use --no-print-messages to consume without printing messages.

--kafka uses OCI's Kafka compatibility endpoint over SASL_SSL/PLAIN. Discovery
still uses instance principals and requires read stream-pools. Set
OCI_KAFKA_USERNAME to tenancyName/[domain/]username/streamPoolId and
OCI_KAFKA_AUTH_TOKEN to the OCI user's auth token in .env. Bootstrap servers
are discovered from the stream pool; OCI_KAFKA_BOOTSTRAP_SERVERS overrides them.
Kafka supports TRIM_HORIZON (earliest) and LATEST here; AT_TIME uses REST only.
Kafka client retries and group heartbeats are managed by librdkafka.

Conservative defaults: two workers, 1000 messages per batch, and two application
calls/second shared by all workers (--request-rate). REST cursor creation,
reads and commits share this cap; Kafka consume calls and commits share it.
Kafka background network requests are managed separately by librdkafka; small
prefetch queues and fetch backoff reduce bursts. Other processes share OCI's
service limits, so these settings cannot guarantee that throttling never occurs.
"""
import argparse
import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import logging
import math
import os
from pathlib import Path
import signal
import sys
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit
import uuid

USAGE_EXAMPLES = """Examples:
  ./runStreamConsumer.sh <stream-name> --group-name my-group --endpoint private
  ./runStreamConsumer.sh <stream-name> --group-name my-group --endpoint public
  ./runStreamConsumer.sh <stream-name> --group-name my-group --duration 60 --workers 2 --request-rate 2
  ./runStreamConsumer.sh <stream-name> --group-name new-group --cursor-type LATEST
  ./runStreamConsumer.sh <stream-name> --group-name new-group --cursor-type AT_TIME --time 2026-11-09T00:00:00Z
  ./runStreamConsumer.sh <stream-name> --group-name my-group --max-messages 1000 --print-messages
  ./runStreamConsumer.sh --group-name my-group --endpoint private --dry-run
  ./runStreamConsumer.sh <stream-name> --group-name kafka-group --endpoint private --kafka
  ./runStreamConsumer.sh <stream-name> --group-name kafka-group --kafka --dry-run
"""


class RateLimiter:
    """Space actual call starts across workers without accumulating burst credits."""
    def __init__(self, rate):
        self.lock = threading.Lock()
        self.interval = 1 / rate
        self.next_call = 0.0

    def wait(self, stop=None, deadline=None):
        while True:
            with self.lock:
                now = time.monotonic()
                if (stop is not None and stop.is_set()) or (deadline is not None and now >= deadline):
                    return False
                delay = self.next_call - now
                if delay <= 0:
                    self.next_call = now + self.interval
                    return True
            # Never hold the lock while waiting; check stop/deadline promptly.
            delay = min(delay, .1)
            if stop is None:
                time.sleep(delay)
            else:
                stop.wait(delay)


class State:
    """Reserve read capacity so concurrent workers honor the global message cap."""
    def __init__(self, maximum, request_rate=2):
        self.lock = threading.Lock()
        self.pacer = RateLimiter(request_rate)
        self.maximum = maximum
        self.reserved = 0
        self.consumed = self.committed = self.payload_bytes = self.requests = 0
        self.partitions = Counter()
        self.errors = []

    def claim(self, batch_size):
        with self.lock:
            if self.maximum is None:
                count = batch_size
            else:
                if self.consumed >= self.maximum:
                    return None
                count = min(batch_size, self.maximum - self.consumed - self.reserved)
            self.reserved += count
            return count

    def release(self, count):
        with self.lock:
            self.reserved -= count

    def record(self, messages, payload_bytes):
        with self.lock:
            self.requests += 1
            self.consumed += len(messages)
            self.payload_bytes += payload_bytes
            self.partitions.update(str(message.partition) for message in messages)

    def snapshot(self):
        with self.lock:
            return dict(consumed=self.consumed, committed=self.committed,
                        payload_bytes=self.payload_bytes, requests=self.requests,
                        partitions=dict(self.partitions), errors=list(self.errors))


def require_cursor(cursor):
    if not isinstance(cursor, str) or not cursor:
        raise RuntimeError('Streaming returned no usable cursor; stopping without another read.')
    return cursor


def worker(oci, client, args, state, stop, deadline, instance_name, output_lock):
    try:
        details = dict(group_name=args.group_name, instance_name=instance_name,
                       type=args.cursor_type, commit_on_get=False)
        if args.time is not None:
            details['time'] = args.time
        retry = dict(retry_strategy=oci.retry.NoneRetryStrategy())
        if not state.pacer.wait(stop, deadline):
            return
        cursor = require_cursor(client.create_group_cursor(args.stream_id,
            oci.streaming.models.CreateGroupCursorDetails(**details), **retry).data.value)
        while not stop.is_set() and time.monotonic() < deadline:
            limit = state.claim(args.batch_size)
            if limit is None:
                return
            if not limit:
                stop.wait(.1)
                continue
            try:
                if not state.pacer.wait(stop, deadline):
                    return
                response = client.get_messages(args.stream_id, cursor, limit=limit, **retry)
                messages = response.data
                if messages is None or len(messages) > limit:
                    raise RuntimeError('Unexpected GetMessages batch; refusing to commit offsets.')
                next_cursor = require_cursor(response.headers.get('opc-next-cursor'))
                payload_bytes = sum(len(base64.b64decode(message.value or '', validate=True))
                                    for message in messages)
                if args.print_messages:
                    with output_lock:
                        for message in messages:
                            print(json.dumps(dict(partition=message.partition, offset=message.offset,
                                                  key=message.key, value=(message.value or '')[:10] + '...')), flush=True)
                state.record(messages, payload_bytes)
                # Commit the returned cursor, including the last batch before stopping.
                if messages:
                    state.pacer.wait()  # Drain processed batches, including on interrupt/deadline.
                    cursor = require_cursor(client.consumer_commit(args.stream_id, next_cursor, **retry).data.value)
                    with state.lock:
                        state.committed += len(messages)
                else:
                    cursor = next_cursor
            finally:
                state.release(limit)
            if not messages:
                stop.wait(min(args.poll_interval, max(0, deadline - time.monotonic())))
    except Exception as error:
        with state.lock:
            state.errors.append(f'{instance_name}: {type(error).__name__}: {error}')
        stop.set()


def report(snapshot, elapsed, final=False):
    print(f"{'FINAL' if final else 'RUN'} {elapsed:7.1f}s | "
          f"consumed {snapshot['consumed']:,} | committed {snapshot['committed']:,} | "
          f"{snapshot['consumed'] / max(elapsed, .000001):,.0f} msg/s average | "
          f"{snapshot['payload_bytes'] / max(elapsed, .000001) / 1048576:.2f} payload MiB/s | "
          f"errors {len(snapshot['errors'])}", file=sys.stderr, flush=True)


def kafka_worker(kafka, client, args, state, stop, deadline, instance_name, output_lock):
    try:
        client.subscribe([args.stream_name])
        while not stop.is_set() and time.monotonic() < deadline:
            limit = state.claim(args.batch_size)
            if limit is None:
                return
            if not limit:
                stop.wait(.1)
                continue
            try:
                if not state.pacer.wait(stop, deadline):
                    return
                records = client.consume(num_messages=limit,
                    timeout=min(args.poll_interval, max(0, deadline - time.monotonic())))
                messages, offsets = [], {}
                payload_bytes = 0
                for record in records:
                    if record.error():
                        raise kafka.KafkaException(record.error())
                    value, key = record.value(), record.key()
                    payload_bytes += len(value or b'')
                    messages.append(SimpleNamespace(partition=record.partition(), offset=record.offset(),
                        key=base64.b64encode(key).decode('ascii') if key is not None else None,
                        value=base64.b64encode(value).decode('ascii') if value is not None else None))
                    partition = (record.topic(), record.partition())
                    offsets[partition] = max(offsets.get(partition, 0), record.offset() + 1)
                if args.print_messages:
                    with output_lock:
                        for message in messages:
                            print(json.dumps(dict(partition=message.partition, offset=message.offset,
                                key=message.key, value=(message.value or '')[:10] + '...')), flush=True)
                state.record(messages, payload_bytes)
                if messages:
                    state.pacer.wait()  # Final commits also share the process-wide rate cap.
                    result = client.commit(offsets=[kafka.TopicPartition(topic, partition, offset)
                        for (topic, partition), offset in offsets.items()], asynchronous=False)
                    for partition in result or []:
                        if partition.error:
                            raise kafka.KafkaException(partition.error)
                    with state.lock:
                        state.committed += len(messages)
            finally:
                state.release(limit)
    except Exception as error:
        with state.lock:
            state.errors.append(f'{instance_name}: {type(error).__name__}: {error}')
        stop.set()


def kafka_config(args, bootstrap_servers, instance_name):
    return {'bootstrap.servers': bootstrap_servers, 'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN', 'sasl.username': args.kafka_username,
            'sasl.password': os.environ['OCI_KAFKA_AUTH_TOKEN'],
            'group.id': args.group_name, 'client.id': instance_name,
            'group.protocol': 'classic', 'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'enable.auto.commit': False, 'enable.auto.offset.store': False,
            'auto.offset.reset': 'earliest' if args.cursor_type == 'TRIM_HORIZON' else 'latest',
            'allow.auto.create.topics': False, 'max.partition.fetch.bytes': 1024 * 1024,
            'fetch.max.bytes': 1024 * 1024, 'queued.min.messages': 1,
            'queued.max.messages.kbytes': 1024, 'fetch.queue.backoff.ms': 1000,
            'fetch.error.backoff.ms': 1000, 'retry.backoff.ms': 1000,
            'retry.backoff.max.ms': 10000,
            'socket.connection.setup.timeout.ms': max(1000, int(args.connect_timeout * 1000)),
            'socket.timeout.ms': max(1000, int(args.request_timeout * 1000))}


def consume(oci, clients, args, run_id):
    stop = threading.Event()
    interrupted = threading.Event()
    state = State(args.max_messages, args.request_rate)
    output_lock = threading.Lock()
    old_signals = {}
    def interrupt(signum, frame):
        interrupted.set()
        stop.set()
    for signum in (signal.SIGINT, signal.SIGTERM):
        old_signals[signum] = signal.signal(signum, interrupt)
    started = time.monotonic()
    deadline = started + args.duration
    prefix = args.instance_name or run_id
    try:
        with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix='stream-consumer') as pool:
            target = kafka_worker if args.kafka else worker
            tasks = [pool.submit(target, oci, client, args, state, stop, deadline,
                                 f'{prefix}-{index}', output_lock)
                     for index, client in enumerate(clients)]
            next_report = started + args.report_interval
            while not all(task.done() for task in tasks):
                now = time.monotonic()
                if now >= deadline:
                    stop.set()
                if now >= next_report:
                    with output_lock:
                        report(state.snapshot(), now - started)
                    next_report = now + args.report_interval
                time.sleep(.1)
            for task in tasks:
                task.result()
    finally:
        stop.set()
        for signum, handler in old_signals.items():
            signal.signal(signum, handler)
    elapsed = time.monotonic() - started
    snapshot = state.snapshot()
    report(snapshot, elapsed, final=True)
    print('Consumed messages by partition: ' + json.dumps(snapshot['partitions'], sort_keys=True), file=sys.stderr)
    for error in snapshot['errors']:
        print('Stopped: ' + error, file=sys.stderr)
    if args.json_report:
        summary = dict(run_id=run_id, stream_id=args.stream_id, stream_name=args.stream_name,
                       group_name=args.group_name, instance_name_prefix=prefix,
                       workers=args.workers, batch_size=args.batch_size, elapsed_seconds=elapsed,
                       request_rate=args.request_rate,
                       protocol='kafka' if args.kafka else 'rest',
                       interrupted=interrupted.is_set(), **snapshot)
        args.json_report.expanduser().write_text(json.dumps(summary, indent=2) + '\n')
        print('JSON report: ' + str(args.json_report.expanduser()), file=sys.stderr)
    return 130 if interrupted.is_set() else int(bool(snapshot['errors']))


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
    print(f'Resolving stream name {args.stream_name!r} in the configured compartment...', file=sys.stderr, flush=True)
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


def timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError('timezone required')
        return result
    except ValueError as error:
        raise argparse.ArgumentTypeError('use an ISO 8601 timestamp with timezone, e.g. 2026-11-09T00:00:00Z') from error


def main(argv=None):
    parser = argparse.ArgumentParser(prog='runStreamConsumer.sh', description=__doc__,
        epilog=USAGE_EXAMPLES, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('stream_name', nargs='?', default=os.getenv('OCI_STREAM_NAME') or None,
                        help='exact stream name (default: OCI_STREAM_NAME in .env)')
    parser.add_argument('--group-name', default=os.getenv('OCI_STREAM_GROUP_NAME') or None,
                        help='consumer group name (required; default: OCI_STREAM_GROUP_NAME in .env)')
    parser.add_argument('--instance-name', help='unique instance name prefix; worker index is appended (default: random UUID)')
    parser.add_argument('--kafka', action='store_true', help='consume over OCI Kafka compatibility using SASL_SSL/PLAIN')
    parser.add_argument('--kafka-username', default=os.getenv('OCI_KAFKA_USERNAME') or None,
                        help='tenancyName/[domain/]username/streamPoolId (default: OCI_KAFKA_USERNAME)')
    parser.add_argument('--kafka-bootstrap-servers', default=os.getenv('OCI_KAFKA_BOOTSTRAP_SERVERS') or None,
                        help='Kafka host:port list (default: OCI_KAFKA_BOOTSTRAP_SERVERS, otherwise discover from pool)')
    parser.add_argument('--endpoint', default=os.getenv('OCI_STREAM_ENDPOINT') or None,
                        metavar='private|public|HTTPS_URL', help='endpoint type or HTTPS messages URL (default: OCI_STREAM_ENDPOINT, otherwise discover)')
    parser.add_argument('--region', default=os.getenv('OCI_REGION') or None,
                        help='stream region (default: OCI_REGION, otherwise instance region)')
    parser.add_argument('--duration', type=positive_float, default=60, help='seconds to start new reads (default: 60)')
    parser.add_argument('--workers', type=positive_int, default=2, help='consumer group instances sharing the rate cap (default: 2; maximum: 128)')
    parser.add_argument('--batch-size', type=positive_int, default=1000, help='maximum messages/read (default: 1000; maximum: 10000)')
    parser.add_argument('--request-rate', type=positive_float, default=2,
                        help='application calls/second across all workers, including commits (default: 2; Kafka background fetches are separate)')
    parser.add_argument('--max-messages', type=positive_int, help='maximum messages consumed across all workers in this process')
    parser.add_argument('--cursor-type', choices=('TRIM_HORIZON', 'LATEST', 'AT_TIME'), default='TRIM_HORIZON',
                        help='starting position for a NEW group (default: TRIM_HORIZON)')
    parser.add_argument('--time', type=timestamp, help='starting timestamp with timezone; required for AT_TIME')
    parser.add_argument('--poll-interval', type=positive_float, default=1, help='seconds between empty reads (default: 1; maximum: 10)')
    parser.add_argument('--report-interval', type=positive_float, default=5)
    parser.add_argument('--connect-timeout', type=positive_float, default=5)
    parser.add_argument('--request-timeout', type=positive_float, default=30)
    parser.add_argument('--print-messages', action=argparse.BooleanOptionalAction, default=True,
                        help='print JSON messages to stdout; base64 values show a 10-character prefix plus ... (default: enabled)')
    parser.add_argument('--json-report', type=Path, help='save final metrics as JSON to this local file')
    parser.add_argument('--debug', action='store_true', help='enable SDK debug logging')
    parser.add_argument('--dry-run', action='store_true', help='show plan without importing OCI, authenticating or consuming')
    args = parser.parse_args(argv)
    for value, setting in ((args.stream_name, 'OCI_STREAM_NAME or provide a stream name'),
                           (args.group_name, 'OCI_STREAM_GROUP_NAME or provide --group-name')):
        if not value or not value.strip():
            parser.error('set ' + setting)
    if args.instance_name is not None and not args.instance_name.strip():
        parser.error('--instance-name must not be empty')
    args.compartment_id = (os.getenv('OCI_COMPARTMENT_ID') or '').strip()
    if not args.compartment_id:
        parser.error('set OCI_COMPARTMENT_ID in .env beside this script')
    if args.workers > 128:
        parser.error('--workers must not exceed 128')
    if args.batch_size > 10000:
        parser.error('--batch-size must not exceed 10000')
    if args.poll_interval > 10:
        parser.error('--poll-interval must not exceed 10 to keep group membership active')
    if (args.cursor_type == 'AT_TIME') != (args.time is not None):
        parser.error('--time is required with AT_TIME and only valid with AT_TIME')
    if args.kafka and args.cursor_type == 'AT_TIME':
        parser.error('--kafka supports TRIM_HORIZON and LATEST; use REST for AT_TIME')
    if args.kafka and args.endpoint and args.endpoint not in ('private', 'public'):
        parser.error('--kafka uses --endpoint private|public; use --kafka-bootstrap-servers for a host:port override')
    if args.kafka and not args.dry_run:
        if not args.kafka_username or not args.kafka_username.strip():
            parser.error('set OCI_KAFKA_USERNAME or provide --kafka-username for --kafka')
        if not os.getenv('OCI_KAFKA_AUTH_TOKEN'):
            parser.error('set OCI_KAFKA_AUTH_TOKEN in .env for --kafka')
    if args.endpoint and args.endpoint not in ('private', 'public'):
        endpoint = urlsplit(args.endpoint)
        if (endpoint.scheme != 'https' or not endpoint.hostname or endpoint.username or endpoint.password or
                endpoint.query or endpoint.fragment or endpoint.path not in ('', '/')):
            parser.error('--endpoint must be private, public, or an HTTPS messages endpoint URL without path, credentials or query')
        args.endpoint = args.endpoint.rstrip('/')
    if args.dry_run:
        endpoint_plan = args.endpoint or 'discover configured messages endpoint'
        if args.endpoint in ('private', 'public'):
            endpoint_plan = f'discover {args.endpoint} endpoint using stream pool type'
        print(f'Dry run: instance-principal authentication; stream name={args.stream_name}')
        print(f'Endpoint: {endpoint_plan}; lookup scope: configured compartment (.env)')
        if args.kafka:
            print('Protocol: Kafka SASL_SSL/PLAIN; user auth token required via OCI_KAFKA_AUTH_TOKEN')
            print('Kafka bootstrap: ' + (args.kafka_bootstrap_servers or 'discover from stream pool Kafka settings'))
        print(f'Group: {args.group_name}; instance prefix: {args.instance_name or "random UUID"}; workers: {args.workers}')
        print(f'New group position: {args.cursor_type}; time: {args.time}; existing group: resume committed offsets')
        print(f'Duration: {args.duration:g}s; batch size: {args.batch_size}; maximum consumed: {args.max_messages or "duration-limited"}')
        print(f'Pacing: {args.request_rate:g} application calls/sec shared by all workers, including commits')
        if args.kafka:
            print('Kafka background fetches are separate; prefetch is limited with 1-second queue/error backoff')
        print('Commit: explicit after each processed batch; message output: ' + str(args.print_messages))
        print('No SDK import, authentication, consumption, offset commits or report writes.')
        return 0
    try:
        import oci
        if args.kafka:
            try:
                import confluent_kafka
            except ImportError as error:
                raise RuntimeError('Kafka client missing; run prepareOL.sh/prepareDebian.sh or install confluent-kafka in the active virtualenv.') from error
        if args.debug:
            logging.basicConfig(level=logging.DEBUG)
        print('Authenticating with OCI instance principals...', file=sys.stderr, flush=True)
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner(
            retry_strategy=oci.retry.NoneRetryStrategy(),
            federation_client_retry_strategy=oci.retry.NoneRetryStrategy())
        config = {'region': args.region or signer.region}
        common = {'signer': signer, 'timeout': (args.connect_timeout, args.request_timeout),
                  'retry_strategy': oci.retry.NoneRetryStrategy()}
        endpoint = None if args.endpoint in ('private', 'public') else args.endpoint
        admin = oci.streaming.StreamAdminClient(config, **common)
        try:
            stream = resolve_stream(oci, admin, args)
            args.stream_id = stream.id
            print(f'Stream: {stream.name}; ID: {stream.id}', file=sys.stderr, flush=True)
            if args.kafka:
                pool = admin.get_stream_pool(stream.stream_pool_id, retry_strategy=oci.retry.NoneRetryStrategy()).data
                if not args.kafka_username.endswith('/' + stream.stream_pool_id):
                    raise RuntimeError('Kafka username must end with / followed by the selected stream pool OCID.')
                endpoint = args.kafka_bootstrap_servers or pool.kafka_settings.bootstrap_servers
            elif not endpoint:
                stream = admin.get_stream(args.stream_id, retry_strategy=oci.retry.NoneRetryStrategy()).data
                if stream.lifecycle_state != 'ACTIVE':
                    raise RuntimeError(f'Stream is {stream.lifecycle_state}; expected ACTIVE.')
                endpoint = stream.messages_endpoint
                print(f'Partitions: {stream.partitions}', file=sys.stderr, flush=True)
        finally:
            admin.base_client.session.close()
        if not endpoint:
            raise RuntimeError('Stream has no messages endpoint; specify --endpoint.')
        print(f'{"Kafka bootstrap" if args.kafka else "Messages endpoint"}: {endpoint}; group: {args.group_name}; workers: {args.workers}', file=sys.stderr, flush=True)
        clients = []
        run_id = uuid.uuid4().hex
        print(f'Pacing: {args.request_rate:g} application calls/sec across {args.workers} worker(s), including commits',
              file=sys.stderr, flush=True)
        try:
            for index in range(args.workers):
                if args.kafka:
                    clients.append(confluent_kafka.Consumer(kafka_config(args, endpoint,
                        f'{args.instance_name or run_id}-{index}')))
                else:
                    clients.append(oci.streaming.StreamClient(config, endpoint, **common))
            return consume(confluent_kafka if args.kafka else oci, clients, args, run_id)
        finally:
            for client in clients:
                if args.kafka:
                    client.close()
                else:
                    client.base_client.session.close()
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        print(f'Error: {type(error).__name__}: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
OCI_STREAM_CONSUMER_PY
