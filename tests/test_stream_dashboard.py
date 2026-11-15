"""Offline checks for dashboard decoding, independent cursors and terminal input."""
import base64
from contextlib import redirect_stdout, redirect_stderr
import io
import json
import os
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).resolve().parents[1] / 'getStreamConsumerDashboard.sh'
SOURCE = SCRIPT.read_text().split("<<'OCI_STREAM_DASHBOARD_PY'\n", 1)[1].rsplit('\nOCI_STREAM_DASHBOARD_PY', 1)[0]
MODULE = {'__name__': 'stream_dashboard'}
exec(compile(SOURCE, str(SCRIPT), 'exec'), MODULE)


def encoded(value):
    return base64.b64encode(value).decode()


class ServiceError(Exception):
    def __init__(self, status):
        self.status = status


class DashboardTests(unittest.TestCase):
    def test_raw_binary_and_text_previews_strip_control_characters(self):
        decode = MODULE['decode_value']
        self.assertEqual(decode(encoded(b'hello\x1b\nworld'), False), (12, 0, ['hello  world']))
        size, count, lines = decode(encoded(b'\xff\x00'), False)
        self.assertEqual((size, count), (2, 0))
        self.assertIn('ff00', lines[0])

    def test_otlp_multiple_records_text_and_producer_binary_body(self):
        document = {'resourceLogs': [{'scopeLogs': [{'logRecords': [
            {'severityText': 'INFO', 'timeUnixNano': '123', 'body': {'stringValue': 'hello'}},
            {'body': {'bytesValue': encoded(b'\x00\xffabc')}}]}]}]}
        raw = json.dumps(document).encode()
        size, count, lines = MODULE['decode_value'](encoded(raw), True)
        self.assertEqual((size, count), (len(raw), 2))
        self.assertIn('INFO 123 hello', lines[0])
        self.assertIn('bytes[5] 00ff616263', lines[1])

    def test_otlp_decoder_accepts_actual_producer_output(self):
        producer = SCRIPT.with_name('runStreamProducerDemo.sh').read_text()
        source = producer.split("<<'OCI_STREAM_PRODUCER_PY'\n", 1)[1].rsplit('\nOCI_STREAM_PRODUCER_PY', 1)[0]
        namespace = {'__name__': 'producer'}
        exec(compile(source, 'producer', 'exec'), namespace)
        payload = namespace['otlp_payload'](encoded(b'\x00\xffabc'), 123)
        size, count, lines = MODULE['decode_value'](encoded(payload), True)
        self.assertEqual((size, count), (len(payload), 1))
        self.assertIn('bytes[5] 00ff616263', lines[0])

    def test_bad_otlp_is_counted_and_does_not_stop_following_messages(self):
        state = MODULE['State']()
        messages = [SimpleNamespace(partition='0', offset=i, value=encoded(value))
                    for i, value in enumerate((b'bad json', b'{}', b'{"resourceLogs":[]}'))]
        MODULE['capture_messages'](state, messages, True)
        self.assertEqual(state.messages, 3)
        self.assertEqual(state.decode_errors, 2)
        self.assertEqual(state.payload_bytes, sum(len(base64.b64decode(m.value)) for m in messages))
        self.assertIn('DECODE ERROR', state.recent[0])

    def test_recent_messages_and_preview_lengths_are_bounded(self):
        state = MODULE['State']()
        message = SimpleNamespace(partition='0', offset=1, value=encoded(b'a' * 1000))
        MODULE['capture_messages'](state, [message] * 200, False)
        self.assertEqual(state.messages, 200)
        self.assertEqual(len(state.recent), 100)
        self.assertLess(len(state.recent[0]), 200)

    def test_partition_cursor_never_joins_group_or_commits(self):
        oci, client, requests = Mock(), Mock(), Mock()
        oci.exceptions.ServiceError = ServiceError
        requests.call.side_effect = [SimpleNamespace(data=SimpleNamespace(value='initial')),
            SimpleNamespace(data=[SimpleNamespace(partition='0', offset=42, value=encoded(b'hi'))],
                            headers={'opc-next-cursor': 'next'})]
        cursors, offsets, state = {}, {}, MODULE['State']()
        args = SimpleNamespace(cursor_type='LATEST', batch_size=100, otlp=False)
        for _ in range(2):
            MODULE['poll_partition'](oci, client, requests, SimpleNamespace(id='stream'), '0',
                                     cursors, offsets, args, state)
        oci.streaming.models.CreateCursorDetails.assert_called_once_with(partition='0', type='LATEST')
        self.assertIs(requests.call.call_args_list[1].args[0], client.get_messages)
        self.assertEqual(requests.call.call_args_list[1].args[2], 'initial')
        self.assertEqual((cursors, offsets), ({'0': 'next'}, {'0': 42}))
        client.create_group_cursor.assert_not_called()
        client.consumer_commit.assert_not_called()
        self.assertEqual(state.messages, 1)

    def test_expired_cursor_resumes_after_last_observed_offset(self):
        oci, client, requests = Mock(), Mock(), Mock()
        oci.exceptions.ServiceError = ServiceError
        cursors, offsets = {'0': 'expired'}, {'0': 42}
        args = SimpleNamespace(cursor_type='LATEST', batch_size=100, otlp=False)
        parameters = (oci, client, requests, SimpleNamespace(id='stream'), '0',
                      cursors, offsets, args, MODULE['State']())
        requests.call.side_effect = ServiceError(400)
        with self.assertRaises(ServiceError):
            MODULE['poll_partition'](*parameters)
        self.assertEqual(cursors, {})
        requests.call.side_effect = None
        requests.call.return_value.data.value = 'resumed'
        MODULE['poll_partition'](*parameters)
        oci.streaming.models.CreateCursorDetails.assert_called_with(partition='0', type='AFTER_OFFSET', offset=42)

    def test_missing_next_cursor_does_not_advance_or_count(self):
        oci, client, requests = Mock(), Mock(), Mock()
        oci.exceptions.ServiceError = ServiceError
        requests.call.return_value = SimpleNamespace(data=[], headers={})
        cursors, state = {'0': 'initial'}, MODULE['State']()
        with self.assertRaisesRegex(RuntimeError, 'no usable cursor'):
            MODULE['poll_partition'](oci, client, requests, SimpleNamespace(id='s'), '0', cursors, {},
                                     SimpleNamespace(batch_size=100), state)
        self.assertEqual(cursors, {'0': 'initial'})
        self.assertEqual(state.messages, 0)

    def test_render_contains_group_offsets_and_sanitized_errors(self):
        state = MODULE['State']()
        state.groups['cp1'] = ('found', [{'partition': '0', 'committed_offset': 42,
                                         'reserved_instance': 'worker\x1b'}], None)
        args = SimpleNamespace(stream_name='events', cursor_type='LATEST', otlp=True)
        text = '\n'.join(MODULE['dashboard_lines'](state, args))
        self.assertIn('cp9', text)
        self.assertIn('42', text)
        self.assertIn('worker', text)
        self.assertNotIn('\x1b', text)

    def test_q_quits_and_sets_stop_while_monitor_is_busy(self):
        screen, stop = Mock(), threading.Event()
        screen.getmaxyx.return_value = (24, 100)
        screen.getch.side_effect = [MODULE['curses'].KEY_NPAGE, ord('q')]
        args = SimpleNamespace(stream_name='events', cursor_type='LATEST', otlp=False)
        with patch.dict(MODULE, {'monitor': lambda *unused: stop.wait(2)}), \
             patch.object(MODULE['curses'], 'curs_set'), \
             patch.object(MODULE['curses'], 'has_colors', return_value=False):
            self.assertEqual(MODULE['run_screen'](screen, args, MODULE['State'](), stop), 0)
        self.assertTrue(stop.is_set())

    def test_dry_run_without_sdk_and_endpoint_override(self):
        for options in ([], ['--otlp', '--endpoint', 'public']):
            output = io.StringIO()
            with patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'c', 'OCI_STREAM_ENDPOINT': 'private'}, clear=True), \
                 patch.dict('sys.modules', {'oci': None}), redirect_stdout(output):
                self.assertEqual(MODULE['main'](['events', '--dry-run', *options]), 0)
            self.assertIn('cp1-cp9', output.getvalue())
            self.assertIn('OTLP Logs JSON' if options else 'raw', output.getvalue())
            self.assertIn('endpoint=public' if options else 'endpoint=private', output.getvalue())

    def test_invalid_batch_size_is_rejected(self):
        with patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'c'}), redirect_stderr(io.StringIO()), \
             self.assertRaises(SystemExit):
            MODULE['main'](['events', '--dry-run', '--batch-size', '0'])

    def test_monitor_probes_cp1_through_cp9_and_closes_clients(self):
        oci, stop, state = Mock(), threading.Event(), MODULE['State']()
        oci.exceptions.ServiceError = ServiceError
        oci.exceptions.RequestException = ConnectionError
        oci.util.to_dict.side_effect = lambda value: value
        oci.auth.signers.InstancePrincipalsSecurityTokenSigner.return_value.region = 'test-region'
        admin = oci.streaming.StreamAdminClient.return_value
        client = oci.streaming.StreamClient.return_value
        stream = SimpleNamespace(id='stream', name='events', partitions=2,
            messages_endpoint='https://messages.example.com', lifecycle_state='ACTIVE')
        admin.list_streams.return_value = SimpleNamespace(data=[stream], headers={})
        admin.get_stream.return_value.data = stream
        client.create_cursor.return_value.data.value = 'cursor'
        client.get_messages.return_value = SimpleNamespace(data=[], headers={'opc-next-cursor': 'next'})
        def group(stream_id, name, **kwargs):
            if name == 'cp9':
                stop.set()
            if name == 'cp2':
                raise ServiceError(404)
            return SimpleNamespace(data={'reservations': []})
        client.get_group.side_effect = group
        class ImmediateRequests:
            def __init__(self, *unused):
                pass
            def call(self, method, *args, **kwargs):
                return method(*args, **kwargs)
        with patch.dict('sys.modules', {'oci': oci}), patch.dict(MODULE, {'Requests': ImmediateRequests}):
            MODULE['monitor'](SimpleNamespace(stream_name='events', compartment_id='c', endpoint=None,
                cursor_type='LATEST', batch_size=100, otlp=False), state, stop)
        self.assertFalse(state.fatal, state.status)
        self.assertEqual([c.args[1] for c in client.get_group.call_args_list], [f'cp{i}' for i in range(1, 10)])
        self.assertEqual(state.groups['cp2'][0], 'absent/inaccessible')
        self.assertEqual(oci.streaming.models.CreateCursorDetails.call_count, 2)
        client.create_group_cursor.assert_not_called()
        client.consumer_commit.assert_not_called()
        admin.base_client.session.close.assert_called_once()
        client.base_client.session.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
