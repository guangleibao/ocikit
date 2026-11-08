"""Offline consumer group protocol, limits, and command line checks."""
from contextlib import redirect_stderr, redirect_stdout
from concurrent.futures import ThreadPoolExecutor
import io
import json
import os
import signal
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
        state = CONSUMER['State'](maximum, request_rate=10000)
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
                               '--batch-size', '2', '--max-messages', '3', '--json-report', str(path),
                               '--request-rate', '1000'])
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

    def cli(self, arguments, env=None):
        output = io.StringIO()
        errors = io.StringIO()
        with patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'compartment', **(env or {})}, clear=True), \
                redirect_stdout(output), redirect_stderr(errors):
            result = CONSUMER['main'](arguments)
        self.assertEqual(result, 0)
        self.stderr_output = errors.getvalue()
        return output.getvalue()

    def kafka_record(self, partition=0, offset=1, value=b'hello world'):
        record = Mock()
        record.error.return_value = None
        record.topic.return_value = 'events'
        record.partition.return_value = partition
        record.offset.return_value = offset
        record.value.return_value = value
        record.key.return_value = b'k'
        return record

    def test_kafka_full_run_discovery_credentials_output_and_cleanup(self):
        kafka = Mock()
        stream = SimpleNamespace(name='events', id='stream', lifecycle_state='ACTIVE', stream_pool_id='pool')
        self.oci.pagination.list_call_get_all_results.return_value.data = [stream]
        admin = self.oci.streaming.StreamAdminClient.return_value
        admin.get_stream_pool.return_value.data = SimpleNamespace(is_private=True,
            kafka_settings=SimpleNamespace(bootstrap_servers='private.kafka.example:9092'))
        client = kafka.Consumer.return_value
        client.consume.return_value = [self.kafka_record()]
        client.commit.return_value = [SimpleNamespace(error=None)]
        with patch.dict('sys.modules', {'oci': self.oci, 'confluent_kafka': kafka}):
            output = self.cli(['events', '--group-name', 'group', '--kafka', '--endpoint', 'private',
                               '--workers', '1', '--max-messages', '1', '--request-rate', '1000'],
                              {'OCI_KAFKA_USERNAME': 'tenant/user/pool', 'OCI_KAFKA_AUTH_TOKEN': 'secret-token'})
        message = json.loads(output)
        self.assertEqual(message['value'], 'aGVsbG8gd2...')
        config = kafka.Consumer.call_args.args[0]
        self.assertEqual(config['bootstrap.servers'], 'private.kafka.example:9092')
        self.assertEqual(config['security.protocol'], 'SASL_SSL')
        self.assertEqual(config['sasl.mechanism'], 'PLAIN')
        self.assertEqual(config['sasl.password'], 'secret-token')
        self.assertEqual(config['group.id'], 'group')
        self.assertEqual(config['auto.offset.reset'], 'earliest')
        self.assertFalse(config['enable.auto.commit'])
        self.assertFalse(config['enable.auto.offset.store'])
        self.assertEqual(config['heartbeat.interval.ms'], 10000)
        self.assertEqual(config['queued.min.messages'], 1)
        self.assertEqual(config['fetch.queue.backoff.ms'], 1000)
        self.assertFalse(client.commit.call_args.kwargs['asynchronous'])
        kafka.TopicPartition.assert_called_once_with('events', 0, 2)
        client.close.assert_called_once()
        admin.base_client.session.close.assert_called_once()
        self.oci.streaming.StreamClient.assert_not_called()
        self.assertNotIn('secret-token', output + self.stderr_output)

    def run_kafka_worker(self, records, commit_error=None):
        kafka, client = Mock(), Mock()
        kafka.KafkaException = RuntimeError
        client.consume.return_value = records
        client.commit.return_value = [SimpleNamespace(error=commit_error)]
        self.args.print_messages = True
        self.args.stream_name = 'events'
        state = CONSUMER['State'](len(records), request_rate=10000)
        with redirect_stdout(io.StringIO()) as output:
            CONSUMER['kafka_worker'](kafka, client, self.args, state, threading.Event(),
                                    time.monotonic() + 1, 'unique-0', threading.Lock())
        return state.snapshot(), kafka, client, output.getvalue()

    def test_kafka_commits_next_offset_for_each_partition(self):
        snapshot, kafka, client, output = self.run_kafka_worker([
            self.kafka_record(0, 1), self.kafka_record(1, 10), self.kafka_record(0, 5)])
        self.assertEqual((snapshot['consumed'], snapshot['committed']), (3, 3))
        self.assertEqual(snapshot['payload_bytes'], 33)
        self.assertFalse(snapshot['errors'])
        self.assertEqual([call.args for call in kafka.TopicPartition.call_args_list],
                         [('events', 0, 6), ('events', 1, 11)])
        client.commit.assert_called_once()

    def test_kafka_commit_failure_is_reported(self):
        snapshot, _, client, _ = self.run_kafka_worker([self.kafka_record()], 'commit failed')
        self.assertEqual((snapshot['consumed'], snapshot['committed']), (1, 0))
        self.assertIn('commit failed', snapshot['errors'][0])
        client.consume.assert_called_once()

    def test_kafka_message_error_does_not_commit(self):
        record = self.kafka_record()
        record.error.return_value = 'read failed'
        snapshot, _, client, output = self.run_kafka_worker([record])
        self.assertTrue(snapshot['errors'])
        client.commit.assert_not_called()
        self.assertEqual(output, '')

    def test_kafka_dry_run_needs_no_token_and_rejects_unsupported_options(self):
        output = self.cli(['events', '--group-name', 'group', '--kafka', '--dry-run'])
        self.assertIn('Kafka SASL_SSL/PLAIN', output)
        for options in (['--cursor-type', 'AT_TIME', '--time', '2026-11-09T00:00:00Z'],
                        ['--endpoint', 'https://messages.example.com']):
            with self.subTest(options=options), self.assertRaises(SystemExit) as error:
                self.cli(['events', '--group-name', 'group', '--kafka', '--dry-run', *options])
            self.assertEqual(error.exception.code, 2)

    def test_kafka_requires_user_and_token(self):
        for env in ({}, {'OCI_KAFKA_USERNAME': 'tenant/user/pool'}):
            with self.subTest(env=env), self.assertRaises(SystemExit) as error:
                self.cli(['events', '--group-name', 'group', '--kafka'], env)
            self.assertEqual(error.exception.code, 2)

    def test_kafka_latest_config(self):
        args = SimpleNamespace(group_name='group', kafka_username='tenant/user/pool',
                               cursor_type='LATEST', connect_timeout=5, request_timeout=30)
        with patch.dict(os.environ, {'OCI_KAFKA_AUTH_TOKEN': 'token'}):
            config = CONSUMER['kafka_config'](args, 'host:9092', 'worker-0')
        self.assertEqual(config['auto.offset.reset'], 'latest')

    def test_message_output_can_be_disabled(self):
        output = self.cli(['events', '--group-name', 'group', '--dry-run', '--no-print-messages'])
        self.assertIn('message output: False', output)

    def test_dry_run_and_timestamp(self):
        for endpoint in ('private', 'public', 'https://messages.example.com'):
            output = self.cli(['events', '--group-name', 'group', '--endpoint', endpoint, '--dry-run'])
            self.assertIn(endpoint, output)
        self.assertIn('AT_TIME', self.cli(['events', '--group-name', 'group', '--dry-run',
            '--cursor-type', 'AT_TIME', '--time', '2026-11-09T00:00:00Z']))

    def test_conservative_defaults_in_dry_run(self):
        output = self.cli(['events', '--group-name', 'group', '--dry-run'])
        self.assertIn('workers: 2', output)
        self.assertIn('batch size: 1000', output)
        self.assertIn('Pacing: 2 application calls/sec shared by all workers, including commits', output)
        self.assertIn('Duration: unlimited (until Ctrl-C)', output)

    def test_explicit_duration_in_dry_run(self):
        output = self.cli(['events', '--group-name', 'group', '--dry-run', '--duration', '120'])
        self.assertIn('Duration: 120s', output)

    def test_continuous_run_has_no_deadline_and_drains_on_interrupt(self):
        args = SimpleNamespace(max_messages=None, request_rate=2, duration=None, workers=1,
                               instance_name=None, kafka=False, report_interval=5, json_report=None)
        previous_handler = signal.getsignal(signal.SIGINT)
        def idle_worker(oci, client, args, state, stop, deadline, instance_name, output_lock):
            self.assertEqual(deadline, float('inf'))
            self.assertFalse(stop.is_set())
            signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
            self.assertTrue(stop.is_set())
        with patch.dict(CONSUMER, {'worker': idle_worker}), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            result = CONSUMER['consume'](self.oci, [self.client], args, 'run')
        self.assertEqual(result, 130)
        self.assertIs(signal.getsignal(signal.SIGINT), previous_handler)

    def test_explicit_duration_stops_empty_stream(self):
        args = SimpleNamespace(max_messages=None, request_rate=10000, duration=.03, workers=1,
                               instance_name=None, kafka=False, report_interval=5, json_report=None,
                               group_name='group', stream_id='stream', cursor_type='TRIM_HORIZON',
                               time=None, batch_size=1000, print_messages=False, poll_interval=.001)
        self.client.get_messages.return_value.data = []
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            result = CONSUMER['consume'](self.oci, [self.client], args, 'run')
        self.assertEqual(result, 0)
        self.assertGreater(self.client.get_messages.call_count, 0)
        self.client.consumer_commit.assert_not_called()

    def test_invalid_parameters(self):
        cases = [[], ['--batch-size', '10001'], ['--workers', '129'], ['--poll-interval', '11'],
                 ['--duration', '0'], ['--duration', '-1'],
                 ['--request-rate', '0'], ['--request-rate', 'nan'], ['--request-rate', 'inf'],
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


class PacingTests(unittest.TestCase):
    def test_shared_limiter_spaces_concurrent_workers(self):
        limiter = CONSUMER['RateLimiter'](40)
        starts = []
        def call():
            self.assertTrue(limiter.wait())
            starts.append(time.monotonic())
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: call(), range(8)))
        starts.sort()
        self.assertTrue(all(b - a >= .020 for a, b in zip(starts, starts[1:])))

    def test_idle_time_does_not_accumulate_burst_credits(self):
        now = [100.0]
        clock = SimpleNamespace(monotonic=lambda: now[0], sleep=lambda delay: now.__setitem__(0, now[0] + delay))
        with patch.dict(CONSUMER, {'time': clock}):
            limiter = CONSUMER['RateLimiter'](2)
            limiter.wait()
            now[0] += 10
            limiter.wait()
            first = now[0]
            limiter.wait()
            self.assertAlmostEqual(now[0] - first, .5)

    def test_cancel_and_deadline_do_not_start_calls(self):
        limiter = CONSUMER['RateLimiter'](2)
        stop = threading.Event()
        stop.set()
        self.assertFalse(limiter.wait(stop))
        self.assertFalse(limiter.wait(deadline=time.monotonic() - 1))

    def test_stop_after_read_still_paces_and_commits_final_batch(self):
        oci, client = Mock(), Mock()
        stop = threading.Event()
        client.create_group_cursor.return_value.data.value = 'initial'
        client.consumer_commit.return_value.data.value = 'committed'
        message = SimpleNamespace(partition='0', value='aGk=')
        def read(*args, **kwargs):
            stop.set()
            return SimpleNamespace(data=[message], headers={'opc-next-cursor': 'next'})
        client.get_messages.side_effect = read
        state = CONSUMER['State'](1, request_rate=40)
        args = SimpleNamespace(group_name='group', cursor_type='TRIM_HORIZON', time=None,
                               stream_id='stream', batch_size=1000, print_messages=False)
        started = time.monotonic()
        CONSUMER['worker'](oci, client, args, state, stop, started + 1, 'worker-0', threading.Lock())
        self.assertEqual(state.committed, 1)
        self.assertFalse(state.errors)
        self.assertGreaterEqual(time.monotonic() - started, .045)


if __name__ == '__main__':
    unittest.main()
