#!/bin/sh
# Standalone OCI SDK consumer. Prefer the caller's .venv or active virtualenv.
# Without a virtualenv, uv supplies the embedded Python SDK dependency.
set -eu
# Load deployment settings from .env beside this script, not the working directory.
OCIKIT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export OCIKIT_DIR
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
REST cursor creation and reads retry temporary service/network failures with
exponential backoff and jitter until stopped. Commits retry for up to 25 seconds
to avoid hanging shutdown; authentication and other permanent errors stop the run.
By default, keep waiting for messages until Ctrl-C. An explicit --duration limits
new reads; in-flight reads and commits drain to their timeouts. --max-messages
also stops the run when the requested number of messages has been consumed.
Ctrl-C/SIGTERM drains fetched batches and exits 130. Metrics are per this process,
not the entire consumer group. Messages are printed as JSON lines to stdout;
Keys remain base64; values show the first 10 base64 characters followed by "...".
Runtime status and metrics go to stderr.
Use --no-print-messages to consume without printing messages.

--api oci (default) uses OCI's Python SDK. --api kafka-python uses the Python
confluent-kafka client; --api kafka-java uses Apache Kafka's Java client. Both
Kafka modes use OCI's Kafka compatibility endpoint over SASL_SSL. Discovery
still uses instance principals and requires read stream-pools. Set
OCI_KAFKA_USERNAME to tenancyName/[domain/]username/streamPoolId and
OCI_KAFKA_AUTH_TOKEN to the OCI user's auth token in .env for SASL/PLAIN.
Java Kafka uses instance principals (OCI_RSA_SHA256) when no Kafka username is
provided; no auth token is needed in that case. Python Kafka requires a username
and auth token. The instance's dynamic group must be allowed to consume streams.
Bootstrap servers
are discovered from the stream pool; OCI_KAFKA_BOOTSTRAP_SERVERS overrides them.
Kafka maps TRIM_HORIZON to earliest and supports AT_TIME for partitions without
committed offsets. Existing committed offsets are preserved in every mode.
Kafka retries and group heartbeats are managed by the selected Kafka client.
Java mode needs the jar built by prepareOL.sh/prepareDebian.sh (JDK and Maven).

Conservative defaults: two workers, 1000 messages per batch, and two application
calls/second shared by all workers (--request-rate). REST cursor creation,
reads and commits share this cap; Kafka consume calls and commits share it.
Kafka background network requests are managed separately by the Kafka clients;
fetch limits and backoff reduce bursts. Other processes share OCI's
service limits, so these settings cannot guarantee that throttling never occurs.
Default --rate is 1000 messages/second across all workers, twice the producer's
default 500 messages/second (producer:consumer = 1:2). This is a message-processing
cap, separate from --request-rate (API calls/second). Actual throughput can be
lower due to available messages, API pacing, or latency. --rate 0 disables only
message-rate pacing; the API-call cap still applies. Kafka may prefetch messages.
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
import random
from pathlib import Path
import signal
import subprocess
import shutil
import select
import sys
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit
import uuid

USAGE_EXAMPLES = """Examples:
  ./runStreamConsumerDemo.sh <stream-name> --group-name my-group --endpoint private --api oci
  ./runStreamConsumerDemo.sh <stream-name> --group-name my-group --endpoint public
  ./runStreamConsumerDemo.sh <stream-name> --group-name my-group --duration 60 --workers 2 --request-rate 2
  ./runStreamConsumerDemo.sh <stream-name> --group-name new-group --cursor-type LATEST
  ./runStreamConsumerDemo.sh <stream-name> --group-name new-group --cursor-type AT_TIME --time 2026-11-09T00:00:00Z
  ./runStreamConsumerDemo.sh <stream-name> --group-name my-group --max-messages 1000 --print-messages
  ./runStreamConsumerDemo.sh --group-name my-group --endpoint private --dry-run
  ./runStreamConsumerDemo.sh <stream-name> --group-name kafka-group --endpoint private --api kafka-python
  ./runStreamConsumerDemo.sh <stream-name> --group-name kafka-group --api kafka-java --dry-run
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


class MessagePacer:
    """Globally pace actual batch counts; fetched batches drain promptly on stop."""
    def __init__(self, rate):
        self.lock = threading.Lock()
        self.rate = rate
        self.next_due = 0.0

    def wait(self, count, stop, deadline):
        if not self.rate or not count:
            return
        with self.lock:
            self.next_due = max(time.monotonic(), self.next_due) + count / self.rate
            due = min(self.next_due, deadline)
        stop.wait(max(0, due - time.monotonic()))


class State:
    """Reserve read capacity so concurrent workers honor the global message cap."""
    def __init__(self, maximum, request_rate=2, message_rate=1000):
        self.lock = threading.Lock()
        self.pacer = RateLimiter(request_rate)
        self.message_pacer = MessagePacer(message_rate)
        self.maximum = maximum
        self.reserved = 0
        self.consumed = self.committed = self.payload_bytes = self.requests = 0
        self.retries = 0
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
                        partitions=dict(self.partitions), errors=list(self.errors), retries=self.retries)


def require_cursor(cursor):
    if not isinstance(cursor, str) or not cursor:
        raise RuntimeError('Streaming returned no usable cursor; stopping without another read.')
    return cursor


class RetryCancelled(Exception):
    """A retryable read or cursor operation was stopped before completion."""


def rest_request(oci, operation, call, state, stop, deadline, instance_name, output_lock, commit=False):
    """Retry the same operation/cursor; every attempt shares the request-rate cap."""
    attempt = 0
    retry_deadline = min(deadline, time.monotonic() + 25) if commit else deadline
    last_error = None
    while True:
        # Always attempt the final commit once, even after a stop signal/deadline.
        draining = commit and attempt == 0
        if not state.pacer.wait(None if draining else stop, None if draining else retry_deadline):
            if commit and last_error is not None:
                raise last_error
            raise RetryCancelled()
        try:
            return call()
        except Exception as error:
            status = getattr(error, 'status', None)
            transient_service = isinstance(status, int) and (status in (408, 429) or 500 <= status < 600 and status != 501)
            request_error = getattr(oci.exceptions, 'RequestException', None)
            transient_network = isinstance(error, OSError) or (
                isinstance(request_error, type) and isinstance(error, request_error))
            if not (transient_service or transient_network):
                raise
            if stop.is_set() or time.monotonic() >= retry_deadline:
                if commit:
                    raise
                raise RetryCancelled() from error
            last_error = error
            attempt += 1
            base = min(30.0, 2.0 ** min(attempt - 1, 5))
            delay = random.uniform(base / 2, base)
            headers = getattr(error, 'headers', None) or {}
            try:
                retry_after = float(headers.get('retry-after', headers.get('Retry-After', 0)))
                if math.isfinite(retry_after):
                    delay = max(delay, retry_after)
            except (TypeError, ValueError):
                pass
            delay = min(delay, max(0, retry_deadline - time.monotonic()))
            with state.lock:
                state.retries += 1
            label = f'HTTP {status} {getattr(error, "code", "")}' if transient_service else type(error).__name__
            with output_lock:
                print(f'Retrying {operation} for {instance_name}: {label}; '
                      f'attempt {attempt}; waiting {delay:.1f}s', file=sys.stderr, flush=True)
            if stop.wait(delay):
                if commit:
                    raise last_error
                raise RetryCancelled() from error


def worker(oci, client, args, state, stop, deadline, instance_name, output_lock):
    try:
        details = dict(group_name=args.group_name, instance_name=instance_name,
                       type=args.cursor_type, commit_on_get=False)
        if args.time is not None:
            details['time'] = args.time
        retry = dict(retry_strategy=oci.retry.NoneRetryStrategy())
        group_details = oci.streaming.models.CreateGroupCursorDetails(**details)
        cursor = require_cursor(rest_request(oci, 'create_group_cursor',
            lambda: client.create_group_cursor(args.stream_id, group_details, **retry),
            state, stop, deadline, instance_name, output_lock).data.value)
        while not stop.is_set() and time.monotonic() < deadline:
            limit = state.claim(args.batch_size)
            if limit is None:
                return
            if not limit:
                stop.wait(.1)
                continue
            try:
                response = rest_request(oci, 'get_messages',
                    lambda: client.get_messages(args.stream_id, cursor, limit=limit, **retry),
                    state, stop, deadline, instance_name, output_lock)
                messages = response.data
                if messages is None or len(messages) > limit:
                    raise RuntimeError('Unexpected GetMessages batch; refusing to commit offsets.')
                next_cursor = require_cursor(response.headers.get('opc-next-cursor'))
                payload_bytes = sum(len(base64.b64decode(message.value or '', validate=True))
                                    for message in messages)
                state.message_pacer.wait(len(messages), stop, deadline)
                if args.print_messages:
                    with output_lock:
                        for message in messages:
                            print(json.dumps(dict(partition=message.partition, offset=message.offset,
                                                  key=message.key, value=(message.value or '')[:10] + '...')), flush=True)
                state.record(messages, payload_bytes)
                # Commit the returned cursor, including the last batch before stopping.
                if messages:
                    cursor = require_cursor(rest_request(oci, 'consumer_commit',
                        lambda: client.consumer_commit(args.stream_id, next_cursor, **retry),
                        state, stop, deadline, instance_name, output_lock, commit=True).data.value)
                    with state.lock:
                        state.committed += len(messages)
                else:
                    cursor = next_cursor
            finally:
                state.release(limit)
            if not messages:
                stop.wait(min(args.poll_interval, max(0, deadline - time.monotonic())))
    except RetryCancelled:
        return
    except Exception as error:
        with state.lock:
            state.errors.append(f'{instance_name}: {type(error).__name__}: {error}')
        stop.set()


def report(snapshot, elapsed, final=False):
    print(f"{'FINAL' if final else 'RUN'} {elapsed:7.1f}s | "
          f"consumed {snapshot['consumed']:,} | committed {snapshot['committed']:,} | "
          f"{snapshot['consumed'] / max(elapsed, .000001):,.0f} msg/s average | "
          f"{snapshot['payload_bytes'] / max(elapsed, .000001) / 1048576:.2f} payload MiB/s | "
          f"retries {snapshot['retries']} | errors {len(snapshot['errors'])}", file=sys.stderr, flush=True)


def kafka_worker(kafka, client, args, state, stop, deadline, instance_name, output_lock):
    try:
        if args.api == 'kafka-python' and args.cursor_type == 'AT_TIME':
            def assigned(consumer, partitions):
                committed = consumer.committed(partitions, timeout=args.request_timeout)
                missing = []
                for partition, saved in zip(partitions, committed):
                    if saved.error:
                        raise kafka.KafkaException(saved.error)
                    if saved.offset >= 0:
                        partition.offset = saved.offset
                    else:
                        missing.append(partition)
                if missing:
                    timestamp_ms = int(args.time.timestamp() * 1000)
                    offsets = consumer.offsets_for_times([kafka.TopicPartition(p.topic, p.partition, timestamp_ms)
                        for p in missing], timeout=args.request_timeout)
                    for partition, found in zip(missing, offsets):
                        if found.error:
                            raise kafka.KafkaException(found.error)
                        partition.offset = found.offset if found.offset >= 0 else kafka.OFFSET_END
                consumer.assign(partitions)
            client.subscribe([args.stream_name], on_assign=assigned)
        else:
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
                state.message_pacer.wait(len(messages), stop, deadline)
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


class JavaKafkaRecord:
    """Adapt a Java consumer record to the shared Kafka processing loop."""
    def __init__(self, data):
        self.data = data

    def error(self):
        return None

    def topic(self):
        return self.data['topic']

    def partition(self):
        return self.data['partition']

    def offset(self):
        return self.data['offset']

    def key(self):
        return base64.b64decode(self.data['key']) if self.data['key'] is not None else None

    def value(self):
        return base64.b64decode(self.data['value']) if self.data['value'] is not None else None


JAVA_KAFKA = SimpleNamespace(KafkaException=RuntimeError,
    TopicPartition=lambda topic, partition, offset: SimpleNamespace(topic=topic, partition=partition, offset=offset))


class JavaKafkaClient:
    """One native Java KafkaConsumer per worker; JSON pipes carry commands only."""
    def __init__(self, args, bootstrap_servers, instance_name):
        root = Path(os.environ.get('OCIKIT_DIR', '.'))
        jar = root / 'kafka-java' / 'target' / 'stream-consumer.jar'
        if not shutil.which('java') or not jar.is_file():
            raise RuntimeError('Java Kafka client missing; run prepareOL.sh/prepareDebian.sh '
                               'or build with mvn -f kafka-java/pom.xml package (JDK 17+).')
        self.timeout = args.request_timeout + args.connect_timeout + args.poll_interval + 10
        self.process = subprocess.Popen(['java', '-jar', str(jar)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, text=True, bufsize=1, start_new_session=True)
        try:
            self._request('init', bootstrap=bootstrap_servers, username=args.kafka_username,
                stream_pool_id=args.stream_pool_id,
                group=args.group_name, instance=instance_name, batch_size=args.batch_size,
                cursor_type=args.cursor_type, timestamp_ms=int(args.time.timestamp() * 1000) if args.time else None,
                connect_timeout_ms=max(1000, int(args.connect_timeout * 1000)),
                request_timeout_ms=max(1000, int(args.request_timeout * 1000)), debug=args.debug)
        except BaseException:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process.stdin.close()
            self.process.stdout.close()
            raise

    def _request(self, operation, **values):
        self.process.stdin.write(json.dumps(dict(op=operation, **values)) + '\n')
        self.process.stdin.flush()
        if not select.select([self.process.stdout], [], [], self.timeout)[0]:
            raise RuntimeError('Java Kafka client timed out responding to ' + operation)
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError('Java Kafka client exited during ' + operation + '; see stderr')
        response = json.loads(line)
        if 'error' in response:
            raise RuntimeError(response['error'])
        return response

    def subscribe(self, topics):
        self._request('subscribe', topics=topics)

    def consume(self, num_messages, timeout):
        return [JavaKafkaRecord(record) for record in self._request('consume',
            limit=num_messages, timeout_ms=max(1, int(timeout * 1000)))['records']]

    def commit(self, offsets, asynchronous=False):
        if asynchronous:
            raise ValueError('Java consumer commits must be synchronous')
        self._request('commit', offsets=[vars(partition) for partition in offsets])
        return []

    def close(self):
        try:
            if self.process.poll() is None:
                self._request('close')
        finally:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process.stdout.close()


def consume(oci, clients, args, run_id):
    stop = threading.Event()
    interrupted = threading.Event()
    state = State(args.max_messages, args.request_rate, args.rate)
    output_lock = threading.Lock()
    old_signals = {}
    def interrupt(signum, frame):
        interrupted.set()
        stop.set()
    for signum in (signal.SIGINT, signal.SIGTERM):
        old_signals[signum] = signal.signal(signum, interrupt)
    started = time.monotonic()
    deadline = started + args.duration if args.duration is not None else float('inf')
    prefix = args.instance_name or run_id
    try:
        with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix='stream-consumer') as pool:
            target = kafka_worker if (args.api != 'oci') else worker
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
                       target_messages_per_second=args.rate,
                       protocol='kafka' if (args.api != 'oci') else 'rest', api=args.api,
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
    print(f'Resolving stream name {args.stream_name!r}; region={args.region}; '
          f'compartment={args.compartment_id} (exact compartment, no child compartments)...', file=sys.stderr, flush=True)
    streams = oci.pagination.list_call_get_all_results(admin.list_streams,
        compartment_id=args.compartment_id, name=args.stream_name, lifecycle_state='ACTIVE',
        retry_strategy=oci.retry.NoneRetryStrategy()).data
    matches = [stream for stream in streams if stream.name == args.stream_name and stream.lifecycle_state == 'ACTIVE']
    if not matches:
        raise RuntimeError(f'No ACTIVE stream named {args.stream_name!r} found in compartment '
            f'{args.compartment_id!r}, region {args.region!r}. Check the exact stream name, '
            'OCI_COMPARTMENT_ID, OCI_REGION/--region, lifecycle state, and instance-principal '
            'read permissions. Child compartments are not searched.')
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
    parser = argparse.ArgumentParser(prog='runStreamConsumerDemo.sh', description=__doc__,
        epilog=USAGE_EXAMPLES, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('stream_name', nargs='?', default=os.getenv('OCI_STREAM_NAME') or None,
                        help='exact stream name (default: OCI_STREAM_NAME in .env)')
    parser.add_argument('--group-name', default=os.getenv('OCI_STREAM_GROUP_NAME') or None,
                        help='consumer group name (required; default: OCI_STREAM_GROUP_NAME in .env)')
    parser.add_argument('--instance-name', help='unique instance name prefix; worker index is appended (default: random UUID)')
    parser.add_argument('--api', choices=('oci', 'kafka-python', 'kafka-java'), default='oci',
                        help='consumer implementation (default: oci)')
    parser.add_argument('--kafka-username', default=os.getenv('OCI_KAFKA_USERNAME') or None,
                        help='tenancyName/[domain/]username/streamPoolId (default: OCI_KAFKA_USERNAME; Java uses instance principals when absent)')
    parser.add_argument('--kafka-bootstrap-servers', default=os.getenv('OCI_KAFKA_BOOTSTRAP_SERVERS') or None,
                        help='Kafka host:port list (default: OCI_KAFKA_BOOTSTRAP_SERVERS, otherwise discover from pool)')
    parser.add_argument('--endpoint', default=os.getenv('OCI_STREAM_ENDPOINT') or None,
                        metavar='private|public|HTTPS_URL', help='endpoint type or HTTPS messages URL (default: OCI_STREAM_ENDPOINT, otherwise discover)')
    parser.add_argument('--region', default=os.getenv('OCI_REGION') or None,
                        help='stream region (default: OCI_REGION, otherwise instance region)')
    parser.add_argument('--duration', type=positive_float,
                        help='seconds to start new reads (default: run continuously until Ctrl-C)')
    parser.add_argument('--workers', type=positive_int, default=2, help='consumer group instances sharing the rate cap (default: 2; maximum: 128)')
    parser.add_argument('--batch-size', type=positive_int, default=1000, help='maximum messages/read (default: 1000; maximum: 10000)')
    parser.add_argument('--request-rate', type=positive_float, default=2,
                        help='application calls/second across all workers, including commits (default: 2; Kafka background fetches are separate)')
    parser.add_argument('--rate', type=float, default=1000,
                        help='processed messages/sec across all workers (default: 1000; twice the producer default of 500, ratio 1:2; 0 = unlimited; independent of --request-rate)')
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
    args.kafka_username = (args.kafka_username or '').strip() or None
    if not math.isfinite(args.rate) or args.rate < 0:
        parser.error('--rate must be finite and nonnegative')
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
    if args.rate:
        args.batch_size = min(args.batch_size, max(1, int(args.rate)))
    if args.poll_interval > 10:
        parser.error('--poll-interval must not exceed 10 to keep group membership active')
    if (args.cursor_type == 'AT_TIME') != (args.time is not None):
        parser.error('--time is required with AT_TIME and only valid with AT_TIME')
    if (args.api != 'oci') and args.endpoint and args.endpoint not in ('private', 'public'):
        parser.error('Kafka APIs use --endpoint private|public; use --kafka-bootstrap-servers for a host:port override')
    if args.api != 'oci' and not args.dry_run:
        if args.api == 'kafka-python' and not args.kafka_username:
            parser.error('set OCI_KAFKA_USERNAME or provide --kafka-username for Python Kafka')
        if args.kafka_username and not os.getenv('OCI_KAFKA_AUTH_TOKEN'):
            parser.error('set OCI_KAFKA_AUTH_TOKEN in .env when using a Kafka username')
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
        print(f'Dry run: api={args.api}; instance-principal discovery; stream name={args.stream_name}')
        print(f'Endpoint: {endpoint_plan}; lookup scope: configured compartment (.env)')
        if args.api == 'kafka-java' and not args.kafka_username:
            print('Protocol: Kafka SASL_SSL/OCI_RSA_SHA256; instance-principal authentication (no auth token needed)')
        elif args.api != 'oci':
            print('Protocol: Kafka SASL_SSL/PLAIN; user auth token required via OCI_KAFKA_AUTH_TOKEN')
        if args.api != 'oci':
            print('Kafka bootstrap: ' + (args.kafka_bootstrap_servers or 'discover from stream pool Kafka settings'))
        print(f'Group: {args.group_name}; instance prefix: {args.instance_name or "random UUID"}; workers: {args.workers}')
        print(f'New group position: {args.cursor_type}; time: {args.time}; existing group: resume committed offsets')
        duration_plan = f'{args.duration:g}s' if args.duration is not None else 'unlimited (until Ctrl-C)'
        print(f'Duration: {duration_plan}; batch size: {args.batch_size}; maximum consumed: {args.max_messages or "unlimited"}')
        print(f'Pacing: {args.request_rate:g} application calls/sec shared by all workers, including commits')
        print(f'Message rate: {args.rate:g} messages/sec across all workers (0=unlimited; producer default: 500, ratio 1:2)')
        if args.api == 'kafka-python':
            print('Kafka background fetches are separate; prefetch is limited with 1-second queue/error backoff')
        elif args.api == 'kafka-java':
            print('Java Kafka: native KafkaConsumer; bounded fetches; shared poll/commit pacing; JDK and built jar required')
        print('Commit: explicit after each processed batch; message output: ' + str(args.print_messages))
        print('No SDK import, authentication, consumption, offset commits or report writes.')
        return 0
    try:
        import oci
        if args.api == 'kafka-python':
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
        args.region = args.region or signer.region
        config = {'region': args.region}
        common = {'signer': signer, 'timeout': (args.connect_timeout, args.request_timeout),
                  'retry_strategy': oci.retry.NoneRetryStrategy()}
        endpoint = None if args.endpoint in ('private', 'public') else args.endpoint
        admin = oci.streaming.StreamAdminClient(config, **common)
        try:
            stream = resolve_stream(oci, admin, args)
            args.stream_id = stream.id
            args.stream_pool_id = stream.stream_pool_id
            print(f'Stream: {stream.name}; ID: {stream.id}', file=sys.stderr, flush=True)
            if (args.api != 'oci'):
                pool = admin.get_stream_pool(stream.stream_pool_id, retry_strategy=oci.retry.NoneRetryStrategy()).data
                if args.kafka_username and not args.kafka_username.endswith('/' + stream.stream_pool_id):
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
        print(f'{"Kafka bootstrap" if args.api != "oci" else "Messages endpoint"}: {endpoint}; group: {args.group_name}; workers: {args.workers}', file=sys.stderr, flush=True)
        if args.api == 'kafka-java':
            print('Java Kafka authentication: ' + ('SASL/PLAIN (user auth token)' if args.kafka_username
                else 'instance principals (OCI_RSA_SHA256)'), file=sys.stderr, flush=True)
        clients = []
        run_id = uuid.uuid4().hex
        print(f'Pacing: {args.request_rate:g} application calls/sec across {args.workers} worker(s), including commits',
              file=sys.stderr, flush=True)
        print(f'Message rate: {args.rate:g} messages/sec across all workers (0=unlimited)', file=sys.stderr, flush=True)
        try:
            for index in range(args.workers):
                instance_name = f'{args.instance_name or run_id}-{index}'
                if args.api == 'kafka-python':
                    clients.append(confluent_kafka.Consumer(kafka_config(args, endpoint, instance_name)))
                elif args.api == 'kafka-java':
                    clients.append(JavaKafkaClient(args, endpoint, instance_name))
                else:
                    clients.append(oci.streaming.StreamClient(config, endpoint, **common))
            backend = confluent_kafka if args.api == 'kafka-python' else JAVA_KAFKA if args.api == 'kafka-java' else oci
            return consume(backend, clients, args, run_id)
        finally:
            for client in clients:
                if (args.api != 'oci'):
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
