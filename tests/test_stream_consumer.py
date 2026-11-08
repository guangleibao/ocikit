"""Offline consumer group protocol, limits, and command line checks."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import threading
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).resolve().parents[1] / 'runStreamConsumer.sh'
SOURCE = SCRIPT.read_text().split("<<'OCI_STREAM_CONSUMER_PY'\n", 1)[1].rsplit('\nOCI_STREAM_CONSUMER_PY', 1)[0]
CONSUMER = {'__name__': 'stream_consumer'}
exec(compile(SOURCE, str(SCRIPT), 'exec'), CONSUMER)


class ConsumerTests(unittest.TestCase):
    def setUp(self):
        self.oci = Mock()
        self.client = Mock()
        self.client.create_group_cursor.return_value.data.value = 'initial'
        self.message = SimpleNamespace(partition='0', offset=1, key='aw==', value='aGk=')
        self.client.get_messages.return_value = SimpleNamespace(
            data=[self.message], headers={'opc-next-cursor': 'next'})
        self.client.consumer_commit.return_value.data.value = 'committed'
        self.args = SimpleNamespace(stream_id='stream', group_name='group', cursor_type='TRIM_HORIZON',
                                    time=None, batch_size=100, print_messages=False, poll_interval=.001)

    def run_worker(self, maximum=1):
        state = CONSUMER['State'](maximum)
        CONSUMER['worker'](self.oci, self.client, self.args, state, threading.Event(),
                           time.monotonic() + 1, 'unique-0', threading.Lock())
        return state.snapshot()

    def test_explicit_commit_including_final_batch(self):
        snapshot = self.run_worker()
        self.assertEqual((snapshot['consumed'], snapshot['committed'], snapshot['payload_bytes']), (1, 1, 2))
        self.assertFalse(snapshot['errors'])
        details = self.oci.streaming.models.CreateGroupCursorDetails.call_args.kwargs
        self.assertFalse(details['commit_on_get'])
        self.assertEqual(details['instance_name'], 'unique-0')
        self.assertEqual(self.client.get_messages.call_args.kwargs['limit'], 1)
        self.assertEqual(self.client.consumer_commit.call_args.args, ('stream', 'next'))

    def test_uses_commit_cursor_for_next_read(self):
        snapshot = self.run_worker(maximum=2)
        self.assertEqual(snapshot['committed'], 2)
        self.assertEqual([call.args[1] for call in self.client.get_messages.call_args_list],
                         ['initial', 'committed'])

    def test_empty_read_advances_cursor_without_commit(self):
        self.client.get_messages.side_effect = [SimpleNamespace(data=[], headers={'opc-next-cursor': 'empty-next'}),
                                               self.client.get_messages.return_value]
        self.assertEqual(self.run_worker()['committed'], 1)
        self.assertEqual(self.client.get_messages.call_args.args[1], 'empty-next')
        self.client.consumer_commit.assert_called_once()

    def test_missing_cursor_does_not_commit(self):
        self.client.get_messages.return_value.headers = {}
        self.assertTrue(self.run_worker()['errors'])
        self.client.consumer_commit.assert_not_called()

    def test_processing_failure_does_not_commit(self):
        self.message.value = 'bad base64!'
        self.assertTrue(self.run_worker()['errors'])
        self.client.consumer_commit.assert_not_called()

    def test_commit_failure_stops_and_reports_uncommitted_messages(self):
        self.client.consumer_commit.side_effect = RuntimeError('commit failed')
        snapshot = self.run_worker()
        self.assertEqual((snapshot['consumed'], snapshot['committed']), (1, 0))
        self.assertIn('commit failed', snapshot['errors'][0])
        self.client.get_messages.assert_called_once()

    def test_global_capacity_reservations(self):
        state = CONSUMER['State'](3)
        self.assertEqual(state.claim(2), 2)
        self.assertEqual(state.claim(2), 1)
        self.assertEqual(state.claim(2), 0)
        state.record([self.message] * 2, 4)
        state.release(2)
        state.record([self.message], 2)
        state.release(1)
        self.assertIsNone(state.claim(2))

    def test_full_run_discovers_endpoint_caps_workers_and_writes_report(self):
        self.message.value = 'aGVsbG8gd29ybGQ='
        stream = SimpleNamespace(name='events', id='stream', lifecycle_state='ACTIVE',
                                 stream_pool_id='pool', messages_endpoint='https://messages.example.com',
                                 partitions=2)
        self.oci.pagination.list_call_get_all_results.return_value.data = [stream]
        admin = self.oci.streaming.StreamAdminClient.return_value
        admin.get_stream_pool.return_value.data.is_private = True
        admin.get_stream.return_value.data = stream
        clients = [self.client, Mock()]
        clients[1].create_group_cursor.return_value.data.value = 'initial-2'
        clients[1].get_messages.return_value = self.client.get_messages.return_value
        clients[1].consumer_commit.return_value.data.value = 'committed-2'
        self.oci.streaming.StreamClient.side_effect = clients
        with tempfile.TemporaryDirectory() as directory, patch.dict('sys.modules', {'oci': self.oci}):
            path = Path(directory) / 'report.json'
            output = self.cli(['events', '--group-name', 'group', '--endpoint', 'private', '--workers', '2',
                               '--batch-size', '2', '--max-messages', '3', '--json-report', str(path)])
            summary = json.loads(path.read_text())
        messages = [json.loads(line) for line in output.splitlines()]
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0], dict(partition='0', offset=1, key='aw==', value='aGVsbG8gd2...'))
        self.assertIn('FINAL', self.stderr_output)
        self.assertIn('Messages endpoint:', self.stderr_output)
        self.assertEqual((summary['consumed'], summary['committed']), (3, 3))
        self.assertFalse(summary['errors'])
        instances = [call.kwargs['instance_name'] for call in
                     self.oci.streaming.models.CreateGroupCursorDetails.call_args_list]
        self.assertEqual(len(set(instances)), 2)
        for call in self.oci.streaming.StreamClient.call_args_list:
            self.assertEqual(call.args[1], 'https://messages.example.com')
        admin.base_client.session.close.assert_called_once()
        for client in clients:
            client.base_client.session.close.assert_called_once()

    def cli(self, arguments):
        output = io.StringIO()
        errors = io.StringIO()
        with patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'compartment'}, clear=True), \
                redirect_stdout(output), redirect_stderr(errors):
            result = CONSUMER['main'](arguments)
        self.assertEqual(result, 0)
        self.stderr_output = errors.getvalue()
        return output.getvalue()

    def test_message_output_can_be_disabled(self):
        output = self.cli(['events', '--group-name', 'group', '--dry-run', '--no-print-messages'])
        self.assertIn('message output: False', output)

    def test_dry_run_and_timestamp(self):
        for endpoint in ('private', 'public', 'https://messages.example.com'):
            output = self.cli(['events', '--group-name', 'group', '--endpoint', endpoint, '--dry-run'])
            self.assertIn(endpoint, output)
        self.assertIn('AT_TIME', self.cli(['events', '--group-name', 'group', '--dry-run',
            '--cursor-type', 'AT_TIME', '--time', '2026-11-09T00:00:00Z']))

    def test_invalid_parameters(self):
        cases = [[], ['--batch-size', '10001'], ['--workers', '129'], ['--poll-interval', '11'],
                 ['--cursor-type', 'AT_TIME'], ['--time', '2026-11-09T00:00:00Z'],
                 ['--cursor-type', 'AT_TIME', '--time', '2026-11-09T00:00:00'], ['--endpoint', 'bad']]
        for options in cases:
            with self.subTest(options=options), self.assertRaises(SystemExit) as error:
                arguments = ['events', '--dry-run'] + (['--group-name', 'group'] if options else []) + options
                self.cli(arguments)
            self.assertEqual(error.exception.code, 2)

    def test_help_includes_examples(self):
        with redirect_stdout(io.StringIO()) as output, self.assertRaises(SystemExit) as error:
            CONSUMER['main'](['--help'])
        self.assertEqual(error.exception.code, 0)
        self.assertIn('Examples:', output.getvalue())
        self.assertIn('--endpoint private', output.getvalue())


if __name__ == '__main__':
    unittest.main()
