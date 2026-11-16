"""Offline checks for the Python embedded in the standalone producer."""
from contextlib import redirect_stderr, redirect_stdout
import base64
import gzip
import io
import os
import signal
import threading
import time
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest


SCRIPT = Path(__file__).resolve().parents[1] / 'runStreamProducerDemo.sh'
SOURCE = SCRIPT.read_text().split("<<'OCI_STREAM_PRODUCER_PY'\n", 1)[1].rsplit('\nOCI_STREAM_PRODUCER_PY', 1)[0]
PRODUCER = {'__name__': 'stream_producer'}
exec(compile(SOURCE, str(SCRIPT), 'exec'), PRODUCER)


class EndpointTests(unittest.TestCase):
    def setUp(self):
        self.streams = [SimpleNamespace(name='events', lifecycle_state='ACTIVE', stream_pool_id=pool)
                        for pool in ('private-pool', 'public-pool')]
        self.oci = Mock()
        self.oci.pagination.list_call_get_all_results.return_value.data = self.streams
        self.admin = Mock()
        self.admin.get_stream_pool.side_effect = lambda pool, **kw: SimpleNamespace(
            data=SimpleNamespace(is_private=pool == 'private-pool'))

    def resolve(self, endpoint):
        args = SimpleNamespace(stream_name='events', compartment_id='compartment', endpoint=endpoint)
        with redirect_stdout(io.StringIO()):
            return PRODUCER['resolve_stream'](self.oci, self.admin, args)

    def test_selects_private_and_public_from_pool_metadata(self):
        for endpoint in ('private', 'public'):
            with self.subTest(endpoint=endpoint):
                self.assertEqual(self.resolve(endpoint).stream_pool_id, endpoint + '-pool')

    def test_type_mismatch_fails(self):
        self.oci.pagination.list_call_get_all_results.return_value.data = self.streams[:1]
        with self.assertRaisesRegex(RuntimeError, 'No ACTIVE stream.*public endpoint'):
            self.resolve('public')

    def test_duplicate_matches_fail_and_pool_lookup_is_cached(self):
        self.oci.pagination.list_call_get_all_results.return_value.data = [self.streams[0]] * 2
        with self.assertRaisesRegex(RuntimeError, 'Multiple ACTIVE streams.*private endpoint'):
            self.resolve('private')
        self.admin.get_stream_pool.assert_called_once()

    def test_unknown_pool_type_fails(self):
        self.admin.get_stream_pool.side_effect = None
        self.admin.get_stream_pool.return_value.data.is_private = None
        with self.assertRaisesRegex(RuntimeError, 'Cannot determine endpoint type'):
            self.resolve('private')

    def test_automatic_and_url_modes_preserve_lookup(self):
        self.oci.pagination.list_call_get_all_results.return_value.data = self.streams[:1]
        for endpoint in (None, 'https://messages.example.com'):
            self.assertIs(self.resolve(endpoint), self.streams[0])
        self.admin.get_stream_pool.assert_not_called()

    def dry_run(self, arguments, env=None):
        output = io.StringIO()
        settings = {'OCI_COMPARTMENT_ID': 'compartment', **(env or {})}
        with patch.dict(os.environ, settings, clear=True), redirect_stdout(output), redirect_stderr(output):
            result = PRODUCER['main'](['events', '--dry-run', *arguments])
        self.assertEqual(result, 0)
        return output.getvalue()

    def test_dry_run_directives(self):
        for endpoint in ('private', 'public'):
            self.assertIn('discover ' + endpoint + ' endpoint', self.dry_run(['--endpoint', endpoint]))

    def test_default_duration_is_unlimited_and_timed_runs_remain_available(self):
        self.assertIn('Duration: unlimited (until Ctrl-C)', self.dry_run([]))
        self.assertIn('Duration: 120s', self.dry_run(['--duration', '120']))

    def test_default_rate_is_half_consumer_default_and_can_be_overridden(self):
        self.assertIn('Rate: 500 messages/sec', self.dry_run([]))
        self.assertIn('Rate: 100 messages/sec', self.dry_run(['--rate', '100']))
        self.assertIn('Rate: 0 messages/sec', self.dry_run(['--rate', '0']))
        with redirect_stdout(io.StringIO()) as output, self.assertRaises(SystemExit) as error:
            PRODUCER['main'](['--help'])
        self.assertEqual(error.exception.code, 0)
        self.assertIn('ratio 1:2', output.getvalue())

    def test_message_rate_globally_paces_producer_allocations(self):
        with patch.object(PRODUCER['time'], 'monotonic', return_value=0):
            allocator = PRODUCER['Allocator'](float('inf'), 50, 500, None, 0)
            self.assertAlmostEqual(allocator.claim()[2], .1)
            self.assertAlmostEqual(allocator.claim()[2], .2)

    def test_continuous_run_can_be_interrupted(self):
        args = SimpleNamespace(duration=None, rate=0, max_messages=None, workers=1,
                               report_interval=5, json_report=None, stream_id='stream',
                               stream_name='events', message_size=1024)
        old_handler = signal.getsignal(signal.SIGINT)
        def interrupt_worker(oci, client, args, allocator, metrics, stop, run_id, worker_id):
            self.assertEqual(allocator.deadline, float('inf'))
            self.assertIsNotNone(allocator.claim())
            signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
            self.assertTrue(stop.is_set())
        with patch.dict(PRODUCER, {'worker': interrupt_worker}), redirect_stdout(io.StringIO()):
            result = PRODUCER['stress'](self.oci, [Mock()], args, 100, 'run')
        self.assertEqual(result, 130)
        self.assertIs(signal.getsignal(signal.SIGINT), old_handler)

    def test_message_cap_still_applies_without_duration(self):
        allocator = PRODUCER['Allocator'](float('inf'), 2, 0, 3, time.monotonic())
        self.assertEqual(allocator.claim()[:2], (0, 2))
        self.assertEqual(allocator.claim()[:2], (2, 1))
        self.assertIsNone(allocator.claim())

    def test_environment_directive_and_cli_override(self):
        env = {'OCI_STREAM_ENDPOINT': 'private'}
        self.assertIn('discover private endpoint', self.dry_run([], env))
        self.assertIn('discover public endpoint', self.dry_run(['--endpoint', 'public'], env))

    def test_otlp_plan_accounts_for_protobuf_and_gzip_overhead(self):
        with patch.dict(PRODUCER, {'motd_sample': lambda: ('motd\n', '/etc/motd')}):
            self.assertIn('Format: raw UTF-8 MOTD', self.dry_run([]))
            output = self.dry_run(['--otlp', '--message-size', '100000', '--batch-size', '100', '--rate', '0'])
            self.assertIn('Format: OTLP Logs Protobuf + gzip', output)
            self.assertIn('effective batch size: 9', output)
            # Raw payload fits; its OTLP representation exceeds the request budget.
            self.dry_run(['--message-size', '999900'])
            with self.assertRaises(SystemExit) as error:
                self.dry_run(['--otlp', '--message-size', '999900'])
            self.assertEqual(error.exception.code, 2)

    def test_worker_publishes_otlp_envelope_and_counts_full_value_bytes(self):
        for otlp in (False, True):
            with self.subTest(otlp=otlp):
                text = 'Message of the day: Welcome! 你好\n'
                args = SimpleNamespace(message_size=len(text.encode()), message_text=text, otlp=otlp, key=None,
                                       stream_id='stream', error_backoff=0)
                oci, client = Mock(), Mock()
                oci.streaming.models.PutMessagesDetailsEntry.side_effect = lambda **kw: SimpleNamespace(**kw)
                oci.streaming.models.PutMessagesDetails.side_effect = lambda **kw: SimpleNamespace(**kw)
                client.put_messages.return_value.data.entries = [SimpleNamespace(error=None, partition='0')] * 2
                allocator = PRODUCER['Allocator'](float('inf'), 2, 0, 2, time.monotonic())
                metrics = PRODUCER['Metrics']()
                with patch.object(PRODUCER['os'], 'urandom') as random_bytes, \
                     patch.object(PRODUCER['time'], 'time_ns', return_value=1758112881118549000):
                    PRODUCER['worker'](oci, client, args, allocator, metrics, threading.Event(), 'run', 0)
                random_bytes.assert_not_called()
                messages = client.put_messages.call_args.args[1].messages
                self.assertEqual(len(messages), 2)
                value = base64.b64decode(messages[0].value, validate=True)
                if otlp:
                    self.assertTrue(value.startswith(b'\x1f\x8b'))
                    request = ExportLogsServiceRequest.FromString(gzip.decompress(value))
                    self.assertEqual(request.resource_logs[0].resource.attributes[0].value.string_value,
                                     'ocikit-stream-producer')
                    record = request.resource_logs[0].scope_logs[0].log_records[0]
                    self.assertEqual(record.time_unix_nano, 1758112881118549000)
                    self.assertEqual(record.severity_number, 9)
                    self.assertEqual(record.body.WhichOneof('value'), 'string_value')
                    self.assertEqual(record.body.string_value, text)
                    bound = PRODUCER['otlp_payload_budget'](len(text.encode()), body_text=text)
                    self.assertLessEqual(len(value), bound)
                else:
                    self.assertEqual(value.decode('utf-8'), text)
                self.assertNotEqual(messages[0].key, messages[1].key)
                self.assertEqual(metrics.snapshot()['payload_bytes'], 2 * len(value))

    def test_otlp_budget_covers_incompressible_and_compressible_bodies(self):
        for size in (1, 127, 128, 16384, 100000, 990000):
            for body in (os.urandom(size), b'\0' * size):
                with self.subTest(size=size, compressible=body[0] == 0):
                    payload = PRODUCER['otlp_payload'](body, 2**64 - 1)
                    self.assertLessEqual(len(payload), PRODUCER['otlp_payload_budget'](size))
                    request = ExportLogsServiceRequest.FromString(gzip.decompress(payload))
                    self.assertEqual(request.resource_logs[0].scope_logs[0].log_records[0].body.bytes_value, body)

    def test_url_override_and_invalid_directive(self):
        self.assertIn('Endpoint: https://messages.example.com\n',
                      self.dry_run(['--endpoint', 'https://messages.example.com/']))
        with self.assertRaises(SystemExit) as error:
            self.dry_run(['--endpoint', 'invalid'])
        self.assertEqual(error.exception.code, 2)

    def test_motd_uses_system_text_or_generates_fallback(self):
        with patch.object(PRODUCER['Path'], 'read_text', return_value='Welcome!\n'):
            self.assertEqual(PRODUCER['motd_sample'](), ('Welcome!\n', '/etc/motd'))
        for read_options in ({'return_value': ' \n'}, {'side_effect': FileNotFoundError()}):
            with self.subTest(read_options=read_options), \
                 patch.object(PRODUCER['Path'], 'read_text', **read_options):
                text, source = PRODUCER['motd_sample']()
                self.assertEqual(source, 'generated greeting')
                self.assertIn('Message of the day', text)

    def test_default_body_is_one_complete_motd_in_raw_and_otlp_modes(self):
        text = 'Message of the day: Welcome! 你好\n'
        for options in ([], ['--otlp']):
            with patch.dict(PRODUCER, {'motd_sample': lambda: (text, '/etc/motd')}):
                output = self.dry_run(options)
                self.assertIn(f'payload: {len(text.encode())} bytes', output)
                self.assertIn('maximum attempted: unlimited', output)
                if options:
                    # Planning and publishing must size the exact same text.
                    budget = PRODUCER['otlp_payload_budget'](len(text.encode()), body_text=text)
                    self.assertIn(f'decoded value budget: <= {budget:,}', output)

    def test_message_size_repeats_complete_utf8_motds_without_truncation(self):
        captured = {}
        real_budget = PRODUCER['otlp_payload_budget']
        def budget(size, *, body_text=None):
            captured['text'] = body_text
            return real_budget(size, body_text=body_text)
        text = 'MOTD: 你好!'
        with patch.dict(PRODUCER, {'motd_sample': lambda: (text, '/etc/motd'), 'otlp_payload_budget': budget}):
            self.dry_run(['--otlp', '--message-size', '100'])
        message = captured['text']
        self.assertGreaterEqual(len(message.encode('utf-8')), 100)
        self.assertEqual(message, (text + '\n') * (len(message) // len(text + '\n')))

    def test_sample_implies_otlp_defaults_to_one_and_allows_repeat_count(self):
        text = 'Welcome "visitor"!\n你好\n'
        with patch.dict(PRODUCER, {'motd_sample': lambda: (text, '/etc/motd')}):
            output = self.dry_run(['--otlp-sample', '--message-size', '9999999'])
            self.assertIn('Format: OTLP Logs Protobuf + gzip', output)
            self.assertIn('maximum attempted: 1\n', output)
            self.assertIn('MOTD sample: /etc/motd; body.string_value', output)
            self.assertIn('MOTD message:\n' + text, output)
            budget = PRODUCER['otlp_payload_budget'](len(text.encode()), body_text=text)
            self.assertIn(f'decoded value budget: <= {budget:,} bytes/message', output)
            self.assertIn('maximum attempted: 3\n', self.dry_run(['--otlp-sample', '--max-messages', '3']))

    def test_sample_size_limit_bounds_input_before_protobuf_allocation(self):
        with patch.dict(PRODUCER, {'motd_sample': lambda: ('\n' * 1000001, '/etc/motd')}), \
             self.assertRaises(SystemExit) as error:
            self.dry_run(['--otlp-sample'])
        self.assertEqual(error.exception.code, 2)

    def test_worker_pushes_motd_as_otlp_text_and_counts_serialized_bytes(self):
        text = 'Message of the day: 你好!\nWelcome "visitor".\n'
        args = SimpleNamespace(message_size=len(text.encode()), message_text=text, otlp=True,
                               key=None, stream_id='stream', error_backoff=0)
        oci, client = Mock(), Mock()
        oci.streaming.models.PutMessagesDetailsEntry.side_effect = lambda **kw: SimpleNamespace(**kw)
        oci.streaming.models.PutMessagesDetails.side_effect = lambda **kw: SimpleNamespace(**kw)
        client.put_messages.return_value.data.entries = [SimpleNamespace(error=None, partition='0')]
        allocator = PRODUCER['Allocator'](float('inf'), 100, 0, 1, time.monotonic())
        metrics = PRODUCER['Metrics']()
        with patch.object(PRODUCER['os'], 'urandom') as random_bytes:
            PRODUCER['worker'](oci, client, args, allocator, metrics, threading.Event(), 'run', 0)
        random_bytes.assert_not_called()
        client.put_messages.assert_called_once()
        messages = client.put_messages.call_args.args[1].messages
        self.assertEqual(len(messages), 1)
        raw = base64.b64decode(messages[0].value, validate=True)
        request = ExportLogsServiceRequest.FromString(gzip.decompress(raw))
        record = request.resource_logs[0].scope_logs[0].log_records[0]
        self.assertEqual(record.body.WhichOneof('value'), 'string_value')
        self.assertEqual(record.body.string_value, text)
        self.assertGreater(record.time_unix_nano, 0)
        self.assertEqual(metrics.snapshot()['payload_bytes'], len(raw))


if __name__ == '__main__':
    unittest.main()
