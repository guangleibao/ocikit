"""Offline checks for dashboard decoding, explicit consumer groups, commits and terminal input."""
import base64
from contextlib import redirect_stdout, redirect_stderr
import gzip
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
        payload = namespace['otlp_payload'](b'\x00\xffabc', 123)
        size, count, lines = MODULE['decode_value'](encoded(payload), True)
        self.assertEqual((size, count), (len(payload), 1))
        self.assertIn('bytes[5] 00ff616263', lines[0])
        text = 'Message of the day: Welcome to OCIKit! 你好\nEnjoy your day.'
        sample = namespace['otlp_payload'](b'', 123, body_text=text)
        _, count, lines = MODULE['decode_value'](encoded(sample), True)
        self.assertEqual(count, 1)
        self.assertIn('Message of the day: Welcome to OCIKit! 你好 Enjoy your day.', lines[0])

    def test_bad_otlp_is_counted_and_does_not_stop_following_messages(self):
        state = MODULE['State']()
        messages = [SimpleNamespace(partition='0', offset=i, value=encoded(value))
                    for i, value in enumerate((b'bad json', b'{}', b'{"resourceLogs":[]}'))]
        MODULE['capture_messages'](state, messages, True)
        self.assertEqual(state.messages, 3)
        self.assertEqual(state.decode_errors, 2)
        self.assertEqual(state.payload_bytes, sum(len(base64.b64decode(m.value)) for m in messages))
        self.assertIn('DECODE ERROR', state.recent[0])

    def test_corrupt_gzip_and_invalid_protobuf_are_decode_errors(self):
        state = MODULE['State']()
        for raw in (b'\x1f\x8btruncated', gzip.compress(b'\xff')):
            MODULE['capture_messages'](state,
                [SimpleNamespace(partition='0', offset=1, value=encoded(raw))], True)
        self.assertEqual(state.messages, 2)
        self.assertEqual(state.decode_errors, 2)
        self.assertTrue(all('DECODE ERROR' in line for line in state.recent))

    def test_recent_messages_and_preview_lengths_are_bounded(self):
        state = MODULE['State']()
        message = SimpleNamespace(partition='0', offset=1, value=encoded(b'a' * 1000))
        MODULE['capture_messages'](state, [message] * 200, False)
        self.assertEqual(state.messages, 200)
        self.assertEqual(len(state.recent), 100)
        self.assertLess(len(state.recent[0]), 200)

    def args(self, **overrides):
        settings = dict(stream_name='events', group_name='orders', instance_name='dashboard-test',
            compartment_id='c', endpoint=None, cursor_type='LATEST', batch_size=100, otlp=False,
            api='oci', connect_timeout=5, request_timeout=30, poll_interval=1, time=None, debug=False,
            kafka_username=None, kafka_bootstrap_servers=None)
        settings.update(overrides)
        return SimpleNamespace(**settings)

    def test_group_cursor_uses_exact_supplied_group_and_disables_auto_commit(self):
        oci, client, requests = Mock(), Mock(), Mock()
        requests.call.return_value.data.value = 'group-cursor'
        cursor = MODULE['create_group_cursor'](oci, client, requests, SimpleNamespace(id='stream'), self.args())
        self.assertEqual(cursor, 'group-cursor')
        oci.streaming.models.CreateGroupCursorDetails.assert_called_once_with(
            group_name='orders', instance_name='dashboard-test', type='LATEST', commit_on_get=False)
        self.assertIs(requests.call.call_args.args[0], client.create_group_cursor)
        client.create_cursor.assert_not_called()

    def test_group_batch_is_captured_before_commit_and_uses_commit_response_cursor(self):
        client, requests, state = Mock(), Mock(), MODULE['State']()
        message = SimpleNamespace(partition='0', offset=42, value=encoded(b'hello'))
        def call(method, stream_id, cursor, **kwargs):
            if method is client.get_messages:
                self.assertEqual(cursor, 'group-cursor')
                return SimpleNamespace(data=[message], headers={'opc-next-cursor': 'next'})
            self.assertIs(method, client.consumer_commit)
            self.assertEqual(cursor, 'next')
            self.assertEqual(state.messages, 1)
            self.assertIn('hello', state.recent[0])
            return SimpleNamespace(data=SimpleNamespace(value='committed-cursor'))
        requests.call.side_effect = call
        cursor = MODULE['consume_batch'](client, requests, SimpleNamespace(id='stream'),
                                          'group-cursor', self.args(), state)
        self.assertEqual(cursor, 'committed-cursor')
        self.assertEqual(state.committed, 1)

    def test_empty_batch_advances_cursor_without_commit(self):
        client, requests, state = Mock(), Mock(), MODULE['State']()
        requests.call.return_value = SimpleNamespace(data=[], headers={'opc-next-cursor': 'next'})
        self.assertEqual(MODULE['consume_batch'](client, requests, SimpleNamespace(id='s'),
            'initial', self.args(), state), 'next')
        self.assertEqual(requests.call.call_count, 1)
        self.assertEqual(state.committed, 0)

    def test_failed_commit_keeps_messages_visible_without_counting_them_as_committed(self):
        client, requests, state = Mock(), Mock(), MODULE['State']()
        requests.call.side_effect = [SimpleNamespace(data=[SimpleNamespace(partition='0', offset=1,
            value=encoded(b'hello'))], headers={'opc-next-cursor': 'next'}), ConnectionError('commit failed')]
        with self.assertRaises(ConnectionError):
            MODULE['consume_batch'](client, requests, SimpleNamespace(id='s'), 'initial', self.args(), state)
        self.assertEqual(state.messages, 1)
        self.assertEqual(state.committed, 0)
        self.assertIn('hello', state.recent[0])

    def test_missing_next_cursor_does_not_advance_or_commit(self):
        client, requests, state = Mock(), Mock(), MODULE['State']()
        requests.call.return_value = SimpleNamespace(data=[], headers={})
        with self.assertRaisesRegex(RuntimeError, 'no usable cursor'):
            MODULE['consume_batch'](client, requests, SimpleNamespace(id='s'), 'initial', self.args(), state)
        self.assertEqual(requests.call.call_count, 1)
        self.assertEqual(state.messages, 0)

    def test_render_shows_only_named_group_and_latest_message_first(self):
        state = MODULE['State']()
        state.reservations = [{'partition': '0', 'committed_offset': 42, 'reserved_instance': 'worker\x1b'}]
        state.recent.extend(['old-message', 'new-message'])
        text = '\n'.join(MODULE['dashboard_lines'](state, self.args()))
        self.assertIn('group: orders', text)
        self.assertNotIn('cp1', text)
        self.assertNotIn('cp9', text)
        self.assertIn('42', text)
        self.assertIn('worker', text)
        self.assertNotIn('\x1b', text)
        self.assertLess(text.index('new-message'), text.index('old-message'))

    def test_long_latest_message_wraps_at_terminal_width(self):
        state = MODULE['State']()
        state.recent.append('Message of the day: ' + 'hello ' * 40)
        lines = MODULE['dashboard_lines'](state, self.args(), width=40)
        preview = lines[lines.index('LATEST CONSUMED MESSAGES (newest first; last 100 records retained)') + 1:]
        self.assertGreater(len(preview), 1)
        self.assertTrue(all(len(line) <= 39 for line in preview))

    def test_q_quits_and_sets_stop_while_monitor_is_busy(self):
        screen, stop = Mock(), threading.Event()
        screen.getmaxyx.return_value = (24, 100)
        screen.getch.side_effect = [MODULE['curses'].KEY_NPAGE, ord('q')]
        args = self.args()
        with patch.dict(MODULE, {'monitor': lambda *unused: stop.wait(2)}), \
             patch.object(MODULE['curses'], 'curs_set'), \
             patch.object(MODULE['curses'], 'has_colors', return_value=False):
            self.assertEqual(MODULE['run_screen'](screen, args, MODULE['State'](), stop), 0)
        self.assertTrue(stop.is_set())

    def test_q_terminates_active_java_bridge(self):
        screen, stop, state = Mock(), threading.Event(), MODULE['State']()
        screen.getmaxyx.return_value = (24, 100)
        screen.getch.return_value = ord('q')
        state.backend_process = process = Mock()
        process.poll.return_value = None
        with patch.dict(MODULE, {'monitor': lambda *unused: stop.wait(2)}), \
             patch.object(MODULE['curses'], 'curs_set'), \
             patch.object(MODULE['curses'], 'has_colors', return_value=False):
            MODULE['run_screen'](screen, self.args(api='kafka-java'), state, stop)
        process.terminate.assert_called_once()

    def test_kafka_batch_commits_highest_next_offset_for_each_partition(self):
        kafka, client, requests, state = Mock(), Mock(), Mock(), MODULE['State']()
        records = []
        for partition, offset in ((0, 1), (1, 10), (0, 5)):
            record = Mock()
            record.partition.return_value, record.offset.return_value = partition, offset
            record.topic.return_value, record.value.return_value, record.error.return_value = 'events', b'hello', None
            records.append(record)
        client.consume.return_value = records
        client.commit.return_value = []
        MODULE['consume_kafka_batch'](kafka, client, requests, None, self.args(), state)
        self.assertEqual([call.args for call in kafka.TopicPartition.call_args_list],
                         [('events', 0, 6), ('events', 1, 11)])
        self.assertEqual(state.committed, 3)
        self.assertEqual(requests.wait.call_count, 2)

    def test_auto_refresh_displays_new_message_without_scrolling(self):
        screen, stop, state = Mock(), threading.Event(), MODULE['State']()
        screen.getmaxyx.return_value = (24, 100)
        state.recent.extend(f'old-message-{i}' for i in range(100))
        def keypress():
            if screen.getch.call_count == 1:
                state.recent.append('LATEST MOTD: Welcome to OCIKit')
                screen.addnstr.reset_mock()
                return -1
            return ord('q')
        screen.getch.side_effect = keypress
        with patch.dict(MODULE, {'monitor': lambda *unused: stop.wait(2)}), \
             patch.object(MODULE['curses'], 'curs_set'), \
             patch.object(MODULE['curses'], 'has_colors', return_value=False):
            MODULE['run_screen'](screen, self.args(), state, stop)
        rendered = [call.args[2] for call in screen.addnstr.call_args_list]
        self.assertIn('LATEST MOTD: Welcome to OCIKit', rendered)
        self.assertLess(rendered.index('LATEST MOTD: Welcome to OCIKit'), rendered.index('old-message-99'))
        screen.timeout.assert_called_once_with(100)

    def test_dry_run_without_sdk_and_endpoint_override(self):
        for options in ([], ['--otlp', '--endpoint', 'public']):
            output = io.StringIO()
            with patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'c', 'OCI_STREAM_ENDPOINT': 'private'}, clear=True), \
                 patch.dict('sys.modules', {'oci': None}), redirect_stdout(output):
                self.assertEqual(MODULE['main'](['events', '--group-name', 'orders', '--dry-run', *options]), 0)
            self.assertIn('Group: orders', output.getvalue())
            self.assertNotIn('cp1-cp9', output.getvalue())
            self.assertIn('OTLP Logs Protobuf + gzip' if options else 'raw', output.getvalue())
            self.assertIn('endpoint=public' if options else 'endpoint=private', output.getvalue())

    def test_invalid_batch_size_is_rejected(self):
        with patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'c'}), redirect_stderr(io.StringIO()), \
             self.assertRaises(SystemExit):
            MODULE['main'](['events', '--group-name', 'orders', '--dry-run', '--batch-size', '0'])

    def test_group_name_is_required_even_if_environment_contains_one(self):
        for options in ([], ['--group-name', '   ']):
            with patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'c', 'OCI_STREAM_GROUP_NAME': 'cp1'}), \
                 redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                MODULE['main'](['events', '--dry-run', *options])
            self.assertEqual(error.exception.code, 2)

    def run_monitor(self, expired=False):
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
        client.create_group_cursor.return_value.data.value = 'group-cursor'
        client.consumer_commit.return_value.data.value = 'committed-cursor'
        client.get_group.return_value.data = {'reservations': []}
        def messages(*args, **kwargs):
            if expired and client.get_messages.call_count == 1:
                raise ServiceError(400)
            return SimpleNamespace(data=[SimpleNamespace(partition='0', offset=42, value=encoded(b'hello'))],
                                   headers={'opc-next-cursor': 'next'})
        client.get_messages.side_effect = messages
        class ImmediateRequests:
            def __init__(self, *unused):
                pass
            def call(self, method, *args, **kwargs):
                response = method(*args, **kwargs)
                if method is client.consumer_commit:
                    stop.set()
                return response
        with patch.dict('sys.modules', {'oci': oci}), patch.dict(MODULE, {'Requests': ImmediateRequests}):
            MODULE['monitor'](self.args(), state, stop)
        self.assertFalse(state.fatal, state.status)
        client.get_group.assert_called_once_with('stream', 'orders')
        client.create_cursor.assert_not_called()
        self.assertEqual(state.messages, 1)
        self.assertEqual(state.committed, 1)
        self.assertEqual(client.create_group_cursor.call_count, 2 if expired else 1)
        self.assertEqual(oci.streaming.models.CreateGroupCursorDetails.call_count, 2 if expired else 1)
        for call in oci.streaming.models.CreateGroupCursorDetails.call_args_list:
            self.assertEqual(call.kwargs['group_name'], 'orders')
        admin.base_client.session.close.assert_called_once()
        client.base_client.session.close.assert_called_once()

    def test_monitor_consumes_only_the_specified_group_and_closes_clients(self):
        self.run_monitor()

    def test_expired_cursor_rejoins_only_the_specified_group(self):
        self.run_monitor(expired=True)

    def test_each_api_can_dry_run_without_credentials_or_sdk(self):
        for api in ('oci', 'kafka-python', 'kafka-java'):
            with self.subTest(api=api), patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'c'}, clear=True), \
                 patch.dict('sys.modules', {'oci': None, 'confluent_kafka': None}), redirect_stdout(io.StringIO()) as output:
                self.assertEqual(MODULE['main'](['events', '--group-name', 'orders', '--api', api, '--dry-run']), 0)
            self.assertIn('api=' + api, output.getvalue())
            if api == 'kafka-java':
                self.assertIn('OCI_RSA_SHA256', output.getvalue())
            if api == 'kafka-python':
                self.assertIn('SASL_SSL/PLAIN', output.getvalue())

    def test_kafka_credentials_and_endpoint_validation(self):
        cases = [(['--api', 'kafka-python'], {}),
                 (['--api', 'kafka-python', '--kafka-username', 'tenant/user/pool'], {}),
                 (['--api', 'kafka-java', '--kafka-username', 'tenant/user/pool'], {}),
                 (['--api', 'kafka-python', '--dry-run'], {'OCI_STREAM_ENDPOINT': 'https://messages.example.com'}),
                 (['--api', 'unsupported', '--dry-run'], {})]
        for options, env in cases:
            with self.subTest(options=options), \
                 patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'c', **env}, clear=True), \
                 redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                MODULE['main'](['events', '--group-name', 'orders', *options])
            self.assertEqual(error.exception.code, 2)

    def test_kafka_monitors_discover_bootstrap_decode_and_commit_for_each_backend(self):
        source = SCRIPT.with_name('runStreamProducerDemo.sh').read_text().split("<<'OCI_STREAM_PRODUCER_PY'\n", 1)[1].rsplit('\nOCI_STREAM_PRODUCER_PY', 1)[0]
        producer = {'__name__': 'producer'}
        exec(compile(source, 'producer', 'exec'), producer)
        for api in ('kafka-python', 'kafka-java'):
            for otlp in (False, True):
                with self.subTest(api=api, otlp=otlp):
                    oci, kafka, stop, state = Mock(), Mock(), threading.Event(), MODULE['State']()
                    oci.exceptions.ServiceError = ServiceError
                    oci.exceptions.RequestException = ConnectionError
                    oci.auth.signers.InstancePrincipalsSecurityTokenSigner.return_value.region = 'test-region'
                    admin = oci.streaming.StreamAdminClient.return_value
                    stream = SimpleNamespace(id='stream', name='events', partitions=2, stream_pool_id='pool',
                        messages_endpoint='https://messages.example.com', lifecycle_state='ACTIVE')
                    admin.list_streams.return_value = SimpleNamespace(data=[stream], headers={})
                    admin.get_stream.return_value.data = stream
                    admin.get_stream_pool.return_value.data.kafka_settings.bootstrap_servers = 'kafka.example.com:9092'
                    record, client = Mock(), Mock()
                    record.topic.return_value = 'events'
                    record.partition.return_value, record.offset.return_value, record.error.return_value = 0, 42, None
                    record.value.return_value = (producer['otlp_payload'](b'', 123, body_text='Message of the day: Hello!')
                                                if otlp else b'Message of the day: Hello!')
                    client.consume.return_value = [record]
                    client.commit.side_effect = lambda **kwargs: stop.set() or []
                    kafka.Consumer.return_value = client
                    kafka.TopicPartition.side_effect = lambda topic, partition, offset: SimpleNamespace(
                        topic=topic, partition=partition, offset=offset)
                    class ImmediateRequests:
                        def __init__(self, *unused): pass
                        def wait(self): pass
                        def call(self, method, *args, **kwargs): return method(*args, **kwargs)
                    import stream_kafka
                    args = self.args(api=api, otlp=otlp,
                        kafka_username='tenant/user/pool' if api == 'kafka-python' else None)
                    with patch.dict('sys.modules', {'oci': oci, 'confluent_kafka': kafka}), \
                         patch.dict(MODULE, {'Requests': ImmediateRequests}), \
                         patch.object(stream_kafka, 'JavaKafkaClient', return_value=client) as java_factory, \
                         patch.dict(os.environ, {'OCI_KAFKA_AUTH_TOKEN': 'test-token'}):
                        MODULE['monitor'](args, state, stop)
                    self.assertFalse(state.fatal, state.status)
                    self.assertEqual((state.messages, state.committed), (1, 1))
                    self.assertIn('Message of the day: Hello!', state.recent[0])
                    self.assertEqual(state.reservations[0]['committed_offset'], 43)
                    self.assertEqual(client.commit.call_args.kwargs['offsets'][0].offset, 43)
                    client.subscribe.assert_called_once_with(['events'])
                    client.close.assert_called_once()
                    admin.base_client.session.close.assert_called_once()
                    oci.streaming.StreamClient.assert_not_called()
                    if api == 'kafka-python':
                        config = kafka.Consumer.call_args.args[0]
                        self.assertEqual(config['group.id'], 'orders')
                        self.assertEqual(config['bootstrap.servers'], 'kafka.example.com:9092')
                        self.assertFalse(config['enable.auto.commit'])
                        java_factory.assert_not_called()
                    else:
                        self.assertEqual(java_factory.call_args.args[1], 'kafka.example.com:9092')
                        self.assertIsNone(java_factory.call_args.args[0].kafka_username)

    def test_kafka_record_and_commit_errors_do_not_increment_committed_count(self):
        for failure in ('record', 'commit'):
            with self.subTest(failure=failure):
                kafka, client, requests, state = Mock(), Mock(), Mock(), MODULE['State']()
                kafka.KafkaException = RuntimeError
                record = Mock()
                record.error.return_value = 'read failed' if failure == 'record' else None
                record.value.return_value = b'hello'
                record.partition.return_value, record.offset.return_value, record.topic.return_value = 0, 1, 'events'
                client.consume.return_value = [record]
                client.commit.side_effect = RuntimeError('commit failed')
                with self.assertRaises(RuntimeError):
                    MODULE['consume_kafka_batch'](kafka, client, requests, None, self.args(), state)
                self.assertEqual(state.committed, 0)
                if failure == 'record':
                    client.commit.assert_not_called()


if __name__ == '__main__':
    unittest.main()
