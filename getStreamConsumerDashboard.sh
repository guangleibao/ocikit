#!/bin/sh
# Standalone OCI SDK consumer dashboard. Prefer the caller's .venv or active virtualenv.
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
exec "$@" <<'OCI_STREAM_DASHBOARD_PY'
# /// script
# requires-python = ">=3.10"
# dependencies = ["oci>=2.189.0,<3", "opentelemetry-proto>=1.28,<2"]
# ///
"""Live OCI stream consumer dashboard. Press q to quit; arrows/PgUp/PgDn scroll.

Usage: ./getStreamConsumerDashboard.sh STREAM_NAME --group-name GROUP [--otlp]

Use instance principals and .env beside the script (OCI_COMPARTMENT_ID,
OCI_REGION, OCI_STREAM_ENDPOINT). Requires read streams, use stream-pull, and
read stream-pools for endpoint filtering. A private endpoint needs private DNS
and network access. Specify the exact consumer group with --group-name; no
names are guessed or probed. Each run joins with a unique consumer instance.

Create a group cursor with commit_on_get=False. New groups start at LATEST by
default; --cursor-type TRIM_HORIZON starts a new group at retained history.
Existing groups resume committed offsets. Joining an existing group participates
in its partition assignments alongside its other consumers. After decoding and
retaining each batch for display, explicitly commit its returned cursor. Decode
errors are displayed and counted as handled messages. Delivery is at least once:
quitting or a failure before commit can cause a batch to be delivered again.

Poll only the named group's state, and consume through its group cursor at two
API calls/second total, including commits. Counts/rates describe this process;
reservation offsets describe the group. Latest messages appear first and wrap
to the terminal width. The screen refreshes automatically every 100 ms.

--otlp decodes gzip-compressed OTLP Logs binary Protobuf requests (or legacy JSON),
including the producer's body.bytesValue and text body.stringValue. This is OCI
Streaming transport, not an OTLP HTTP/gRPC receiver. Malformed messages are counted
and displayed without stopping the dashboard. Binary bodies show a hex preview.
An interactive terminal and Python curses support are required, except for --dry-run.

References:
https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/using_consumer_groups.htm
https://opentelemetry.io/docs/specs/otlp/
"""
import argparse
import base64
from collections import deque
import curses
import gzip
import io
import json
import os
import signal
import sys
import threading
import textwrap
import time
import uuid
from urllib.parse import urlsplit


class Cancelled(Exception):
    pass


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.status = 'Authenticating and resolving stream...'
        self.group_status = 'waiting'
        self.reservations = []
        self.group_updated = None
        self.committed = 0
        self.recent = deque(maxlen=100)
        self.messages = self.payload_bytes = self.records = self.decode_errors = 0
        self.retries = 0
        self.started = time.monotonic()
        self.fatal = False


class Requests:
    def __init__(self, oci, stop, state):
        self.oci, self.stop, self.state = oci, stop, state
        self.next_call = 0.0

    def call(self, method, *args, **kwargs):
        for attempt in range(4):
            if self.stop.wait(max(0, self.next_call - time.monotonic())):
                raise Cancelled()
            self.next_call = time.monotonic() + .5
            try:
                return method(*args, retry_strategy=self.oci.retry.NoneRetryStrategy(), **kwargs)
            except (self.oci.exceptions.ServiceError, self.oci.exceptions.RequestException, OSError) as error:
                status = getattr(error, 'status', None)
                transient = not isinstance(error, self.oci.exceptions.ServiceError) or (
                    isinstance(status, int) and (status in (408, 429) or 500 <= status < 600 and status != 501))
                if not transient or attempt == 3:
                    raise
                with self.state.lock:
                    self.state.retries += 1
                    self.state.status = f'Retrying {method.__name__}: {type(error).__name__} {status}'
                if self.stop.wait(2 ** attempt):
                    raise Cancelled()


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


def safe_text(value):
    # Stream content and service errors must never inject terminal control codes.
    return ''.join(c if c.isprintable() else ' ' for c in str(value))


def body_preview(body):
    if not isinstance(body, dict):
        raise ValueError('OTLP body must be an AnyValue object')
    if 'bytesValue' in body:
        value = base64.b64decode(body['bytesValue'], validate=True)
        return f'bytes[{len(value)}] {value[:32].hex()}'
    if 'stringValue' in body:
        return safe_text(body['stringValue'])[:2048]
    return safe_text(json.dumps(body, ensure_ascii=True))[:160]


def decode_value(value, otlp):
    """Return decoded byte count, OTLP record count, and bounded preview lines."""
    raw = base64.b64decode(value or '', validate=True)
    if not otlp:
        try:
            preview = safe_text(raw.decode('utf-8'))[:160]
        except UnicodeDecodeError:
            preview = f'bytes[{len(raw)}] {raw[:32].hex()}'
        return len(raw), 0, [preview]
    if raw.startswith(b'\x1f\x8b'):
        from google.protobuf.json_format import MessageToDict
        from google.protobuf.message import DecodeError
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as compressed:
                wire = compressed.read(16 * 1024 * 1024 + 1)
            if len(wire) > 16 * 1024 * 1024:
                raise ValueError('decompressed OTLP request exceeds 16 MiB preview limit')
            request = ExportLogsServiceRequest.FromString(wire)
            document = MessageToDict(request)
        except (OSError, EOFError, DecodeError) as error:
            raise ValueError(f'invalid gzip/Protobuf OTLP request: {error}') from error
    else:
        document = json.loads(raw)  # Retain support for previously published JSON.
    if not isinstance(document, dict) or not isinstance(document.get('resourceLogs'), list):
        raise ValueError('expected OTLP resourceLogs array')
    count, previews = 0, deque(maxlen=10)
    for resource in document['resourceLogs']:
        for scope in resource.get('scopeLogs', []):
            for record in scope.get('logRecords', []):
                if not isinstance(record, dict):
                    raise ValueError('invalid OTLP log record')
                preview = body_preview(record.get('body', {}))
                count += 1
                previews.append(f"{record.get('severityText', '')} {record.get('timeUnixNano', '-')} {preview}")
    return len(raw), count, list(previews)


def capture_messages(state, messages, otlp):
    for message in messages:
        prefix = f'p{message.partition} @{message.offset}'
        try:
            size, records, previews = decode_value(message.value, otlp)
            error = False
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError) as exc:
            size, records, previews, error = 0, 0, [f'DECODE ERROR: {exc}'], True
            try:
                size = len(base64.b64decode(message.value or '', validate=True))
            except (ValueError, TypeError):
                pass
        with state.lock:
            state.messages += 1
            state.payload_bytes += size
            state.records += records
            state.decode_errors += int(error)
            for preview in previews:
                state.recent.append(f'{prefix} {safe_text(preview)}')


def require_cursor(value):
    if not isinstance(value, str) or not value:
        raise RuntimeError('OCI returned no usable cursor')
    return value


def create_group_cursor(oci, client, requests, stream, args):
    details = oci.streaming.models.CreateGroupCursorDetails(
        group_name=args.group_name, instance_name=args.instance_name,
        type=args.cursor_type, commit_on_get=False)
    return require_cursor(requests.call(client.create_group_cursor, stream.id, details).data.value)


def consume_batch(client, requests, stream, cursor, args, state):
    response = requests.call(client.get_messages, stream.id, cursor, limit=args.batch_size)
    next_cursor = require_cursor(response.headers.get('opc-next-cursor'))
    messages = response.data
    if not isinstance(messages, list) or len(messages) > args.batch_size:
        raise RuntimeError('OCI returned an unexpected message batch; no offsets committed')
    capture_messages(state, messages, args.otlp)
    if messages:
        # Commit only after the batch has been processed and retained for display.
        # The commit response cursor must be used on the next read.
        next_cursor = require_cursor(requests.call(client.consumer_commit, stream.id, next_cursor).data.value)
        with state.lock:
            state.committed += len(messages)
    return next_cursor


def refresh_group(oci, client, requests, stream, args, state):
    try:
        data = requests.call(client.get_group, stream.id, args.group_name).data
        status, reservations = 'active', oci.util.to_dict(data).get('reservations') or []
    except oci.exceptions.ServiceError as error:
        status = 'absent/inaccessible' if error.status == 404 else f'HTTP {error.status}'
        reservations = []
    except (oci.exceptions.RequestException, OSError) as error:
        status, reservations = type(error).__name__, []
    with state.lock:
        state.group_status = status
        state.reservations = reservations
        state.group_updated = time.monotonic()


def monitor(args, state, stop):
    admin = client = None
    try:
        import oci
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner(
            retry_strategy=oci.retry.NoneRetryStrategy(),
            federation_client_retry_strategy=oci.retry.NoneRetryStrategy())
        common = dict(signer=signer, timeout=(5, 30), retry_strategy=oci.retry.NoneRetryStrategy())
        config = {'region': os.getenv('OCI_REGION') or signer.region}
        requests = Requests(oci, stop, state)
        admin = oci.streaming.StreamAdminClient(config, **common)
        stream = resolve_stream(admin, requests, args.stream_name, args.compartment_id, args.endpoint)
        endpoint = stream.messages_endpoint if args.endpoint in (None, 'private', 'public') else args.endpoint
        if not endpoint:
            raise RuntimeError('Stream has no messages endpoint')
        if not stream.partitions or stream.partitions < 1:
            raise RuntimeError('Stream has no partitions')
        client = oci.streaming.StreamClient(config, endpoint, **common)
        cursor = create_group_cursor(oci, client, requests, stream, args)
        next_group_refresh = 0.0
        recovered_cursor = False
        while not stop.is_set():
            if time.monotonic() >= next_group_refresh:
                refresh_group(oci, client, requests, stream, args, state)
                next_group_refresh = time.monotonic() + 5
            if stop.is_set():
                break
            with state.lock:
                state.status = f'{stream.name} | {config["region"]} | {stream.partitions} partitions | {endpoint}'
            try:
                cursor = consume_batch(client, requests, stream, cursor, args, state)
                recovered_cursor = False
            except oci.exceptions.ServiceError as error:
                # Rejoin the same explicitly supplied group after an expired
                # cursor; its committed offsets, not a guessed partition, apply.
                if error.status != 400 or recovered_cursor:
                    raise
                cursor = create_group_cursor(oci, client, requests, stream, args)
                recovered_cursor = True
                with state.lock:
                    state.status = 'Rejoined consumer group after invalid/expired cursor'

    except Cancelled:
        pass
    except Exception as error:
        with state.lock:
            state.status = f'ERROR: {type(error).__name__}: {error}'
            state.fatal = True
    finally:
        for resource in (client, admin):
            if resource is not None:
                resource.base_client.session.close()


def dashboard_lines(state, args, width=100):
    now = time.monotonic()
    with state.lock:
        elapsed = max(.001, now - state.started)
        lines = [f'OCI STREAM DASHBOARD | q quit | arrows/PgUp/PgDn scroll | {args.stream_name}',
                 safe_text(state.status),
                 f'Group: {args.group_name} | instance: {args.instance_name}',
                 f'Consumed: {state.messages:,} | committed: {state.committed:,} | '
                 f'{state.messages / elapsed:.1f} msg/s avg | '
                 f'{state.payload_bytes / elapsed / 1048576:.3f} MiB/s avg',
                 f'OTLP records {state.records:,} | decode errors {state.decode_errors} | retries {state.retries}',
                 f'Format: {"OTLP Protobuf + gzip" if args.otlp else "raw"} | '
                 f'new group: {args.cursor_type} | explicit commits | 2 API calls/s',
                 'PART COMMIT OFFSET  RESERVED INSTANCE    GROUP STATUS / AGE']
        age = f'{now - state.group_updated:.0f}s' if state.group_updated is not None else '-'
        for reservation in state.reservations[:4] or [{}]:
            lines.append(f'{str(reservation.get("partition", "-")):<4} '
                         f'{str(reservation.get("committed_offset", "-")):<14} '
                         f'{str(reservation.get("reserved_instance") or "-")[:20]:<20} '
                         f'{state.group_status} / {age}')
        if len(state.reservations) > 4:
            lines.append(f'... {len(state.reservations) - 4} additional partition reservations')
        lines += ['', 'LATEST CONSUMED MESSAGES (newest first; last 100 records retained)']
        for message in reversed(state.recent):
            lines.extend(textwrap.wrap(safe_text(message), width=max(1, width - 1),
                                       subsequent_indent='  ') or [''])
        return [safe_text(line) for line in lines]


def run_screen(screen, args, state, stop):
    try:
        curses.curs_set(0)
    except curses.error:
        pass
    heading_style = curses.A_BOLD
    if curses.has_colors():
        curses.start_color()
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_GREEN)
        heading_style |= curses.color_pair(1)
    screen.keypad(True)
    screen.timeout(100)
    scroll = 0
    thread = threading.Thread(target=monitor, args=(args, state, stop), daemon=True)
    thread.start()
    try:
        while True:
            height, width = screen.getmaxyx()
            lines = dashboard_lines(state, args, width)
            visible = max(1, height - 1)
            scroll = max(0, min(scroll, max(0, len(lines) - visible)))
            screen.erase()
            for row, line in enumerate(lines[scroll:scroll + visible]):
                try:
                    screen.addnstr(row, 0, line, max(0, width - 1), heading_style if scroll + row == 0 else 0)
                except curses.error:
                    pass  # Resize, wide Unicode, or a terminal smaller than one row.
            screen.refresh()
            key = screen.getch()
            if key in (ord('q'), ord('Q')):
                return int(state.fatal)
            if key in (curses.KEY_DOWN, curses.KEY_NPAGE):
                scroll += visible if key == curses.KEY_NPAGE else 1
            elif key in (curses.KEY_UP, curses.KEY_PPAGE):
                scroll -= visible if key == curses.KEY_PPAGE else 1
            elif key == curses.KEY_HOME:
                scroll = 0
            elif key == curses.KEY_END:
                scroll = len(lines)
    finally:
        stop.set()
        # Network I/O runs in a daemon so q restores the terminal immediately,
        # even during instance-principal authentication or a stalled SDK request.


def main(argv=None):
    parser = argparse.ArgumentParser(prog='getStreamConsumerDashboard.sh', description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('stream_name', help='exact stream name')
    parser.add_argument('--group-name', required=True, help='exact consumer group name (required; no default)')
    parser.add_argument('--instance-name', help='unique consumer instance name (default: dashboard-UUID)')
    parser.add_argument('--endpoint', choices=('private', 'public'), help='override .env endpoint type')
    parser.add_argument('--otlp', action='store_true', help='decode gzip-compressed OTLP Logs Protobuf (also accepts legacy JSON)')
    parser.add_argument('--cursor-type', choices=('LATEST', 'TRIM_HORIZON'), default='LATEST')
    parser.add_argument('--batch-size', type=int, default=100, help='messages per group read (default: 100; max: 10000)')
    parser.add_argument('--dry-run', action='store_true', help='show plan without SDK import or network calls')
    args = parser.parse_args(argv)
    if not args.group_name.strip():
        parser.error('--group-name must not be empty')
    if args.instance_name is not None and not args.instance_name.strip():
        parser.error('--instance-name must not be empty')
    args.instance_name = args.instance_name or 'dashboard-' + uuid.uuid4().hex
    args.compartment_id = (os.getenv('OCI_COMPARTMENT_ID') or '').strip()
    if not args.stream_name.strip() or not args.compartment_id:
        parser.error('provide a stream name and set OCI_COMPARTMENT_ID in .env beside this script')
    if not 1 <= args.batch_size <= 10000:
        parser.error('--batch-size must be between 1 and 10000')
    args.endpoint = args.endpoint or (os.getenv('OCI_STREAM_ENDPOINT') or '').strip() or None
    if args.endpoint and args.endpoint not in ('private', 'public'):
        parsed = urlsplit(args.endpoint)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or
                parsed.query or parsed.fragment or parsed.path not in ('', '/')):
            parser.error('OCI_STREAM_ENDPOINT must be private, public, or an HTTPS messages endpoint URL')
        args.endpoint = args.endpoint.rstrip('/')
    if args.dry_run:
        print(f'Dry run: instance principals; stream={args.stream_name}; endpoint={args.endpoint or "discover"}')
        print(f'Group: {args.group_name}; instance: {args.instance_name}; format: {"OTLP Logs Protobuf + gzip (or legacy JSON)" if args.otlp else "raw"}; start: {args.cursor_type}')
        print('Join the specified consumer group; existing groups resume committed offsets; 2 API calls/sec.')
        print('Decode and retain each batch, then explicitly commit offsets. Newest messages shown first.')
        print('Interactive dashboard: q quit; arrows/PgUp/PgDn scroll. No SDK import or network calls.')
        return 0
    stop = threading.Event()
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    def interrupt(signum, frame):
        raise KeyboardInterrupt()
    try:
        signal.signal(signal.SIGTERM, interrupt)
        if not sys.stdout.isatty():
            raise RuntimeError('dashboard requires an interactive terminal (or use --dry-run)')
        # Python source arrives on stdin via the shell heredoc. Curses must read
        # keystrokes from the controlling terminal instead of that exhausted pipe.
        with open('/dev/tty', 'rb', buffering=0) as terminal:
            original_stdin = os.dup(0)
            try:
                os.dup2(terminal.fileno(), 0)
                return curses.wrapper(run_screen, args, State(), stop)
            finally:
                os.dup2(original_stdin, 0)
                os.close(original_stdin)
    except KeyboardInterrupt:
        return 130
    except (OSError, RuntimeError, curses.error) as error:
        print(f'Error: {error}', file=sys.stderr)
        return 1
    finally:
        stop.set()
        signal.signal(signal.SIGTERM, previous_sigterm)


if __name__ == '__main__':
    sys.exit(main())
OCI_STREAM_DASHBOARD_PY
