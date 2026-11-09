"""Offline stream lookup, fixed group probing, and JSON output checks."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).resolve().parents[1] / 'getStreamConsumerGroups.sh'
SOURCE = SCRIPT.read_text().split("<<'OCI_STREAM_GROUPS_PY'\n", 1)[1].rsplit('\nOCI_STREAM_GROUPS_PY', 1)[0]
MODULE = {'__name__': 'stream_consumer_groups'}
exec(compile(SOURCE, str(SCRIPT), 'exec'), MODULE)


class ServiceError(Exception):
    def __init__(self, status):
        self.status, self.code, self.message = status, 'TestError', 'test failure'


class GroupTests(unittest.TestCase):
    def setUp(self):
        self.oci = Mock()
        self.oci.exceptions.ServiceError = ServiceError
        self.oci.exceptions.RequestException = ConnectionError
        self.oci.util.to_dict.side_effect = lambda data: data
        self.oci.auth.signers.InstancePrincipalsSecurityTokenSigner.return_value.region = 'us-ashburn-1'
        self.admin = self.oci.streaming.StreamAdminClient.return_value
        self.client = self.oci.streaming.StreamClient.return_value
        self.stream = SimpleNamespace(id='stream-id', name='events', lifecycle_state='ACTIVE',
            stream_pool_id='pool-id', messages_endpoint='https://private.example.com')
        self.admin.list_streams.return_value = SimpleNamespace(data=[self.stream], headers={})
        self.admin.get_stream.return_value.data = self.stream
        self.now = 0
        self.clock = SimpleNamespace(monotonic=lambda: self.now, sleep=self.advance)

    def advance(self, delay):
        self.now += delay

    def run_main(self, env=None, options=None):
        out, err = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {'OCI_COMPARTMENT_ID': 'compartment', **(env or {})}, clear=True), \
                patch.dict('sys.modules', {'oci': self.oci}), patch.dict(MODULE, {'time': self.clock}), \
                redirect_stdout(out), redirect_stderr(err):
            status = MODULE['main'](['events', *(options or [])])
        return status, json.loads(out.getvalue()) if out.getvalue() else None, err.getvalue()

    def test_exactly_nine_probes_preserve_all_group_details(self):
        def group(stream, name, **kwargs):
            if name not in ('cp1', 'cp9'):
                raise ServiceError(404)
            return SimpleNamespace(data=dict(stream_id=stream, group_name=name,
                reservations=[dict(partition='0', committed_offset=42, reserved_instance='worker-0')]))
        self.client.get_group.side_effect = group
        status, data, err = self.run_main()
        self.assertEqual(status, 0)
        self.assertEqual([call.args[1] for call in self.client.get_group.call_args_list],
                         [f'cp{i}' for i in range(1, 10)])
        self.assertEqual([g['group_name'] for g in data['groups']], ['cp1', 'cp9'])
        self.assertEqual(data['groups'][0]['reservations'][0]['committed_offset'], 42)
        self.assertEqual(len(data['unavailable_groups']), 7)
        self.assertEqual(data['unavailable_groups'][0]['reason'], 'not_found_or_not_authorized')
        self.assertIn('2 found', err)
        self.assertEqual(self.oci.streaming.StreamClient.call_args.args[1], self.stream.messages_endpoint)
        self.admin.base_client.session.close.assert_called_once()
        self.client.base_client.session.close.assert_called_once()
        self.client.create_group_cursor.assert_not_called()
        self.client.consumer_commit.assert_not_called()
        self.assertGreaterEqual(self.now, 5)

    def test_missing_groups_return_empty_array(self):
        self.client.get_group.side_effect = ServiceError(404)
        status, data, _ = self.run_main()
        self.assertEqual(status, 0)
        self.assertEqual(data['groups'], [])
        self.assertEqual(len(data['unavailable_groups']), 9)

    def test_probe_failure_is_visible_and_does_not_skip_other_groups(self):
        self.client.get_group.side_effect = lambda stream, name, **kwargs: (_ for _ in ()).throw(
            ServiceError(503 if name == 'cp1' else 404))
        status, data, err = self.run_main()
        self.assertEqual(status, 1)
        self.assertEqual(data['errors'][0]['group_name'], 'cp1')
        self.assertEqual(data['errors'][0]['status'], 503)
        self.assertEqual(len(data['unavailable_groups']), 8)
        self.assertEqual(self.client.get_group.call_count, 12)
        self.assertIn('Retrying OCI request', err)

    def test_private_directive_filters_pool_and_discovers_endpoint(self):
        self.admin.get_stream_pool.return_value.data.is_private = True
        self.client.get_group.side_effect = ServiceError(404)
        status, data, _ = self.run_main({'OCI_STREAM_ENDPOINT': 'private'})
        self.assertEqual(status, 0)
        self.assertEqual(data['messages_endpoint'], self.stream.messages_endpoint)
        self.admin.get_stream_pool.assert_called_once()

    def test_endpoint_flag_overrides_environment_for_each_type(self):
        self.client.get_group.side_effect = ServiceError(404)
        for endpoint in ('private', 'public'):
            with self.subTest(endpoint=endpoint):
                self.admin.get_stream_pool.return_value.data.is_private = endpoint == 'private'
                status, data, _ = self.run_main({'OCI_STREAM_ENDPOINT': 'public' if endpoint == 'private' else 'private'},
                                               ['--endpoint', endpoint])
                self.assertEqual(status, 0)
                self.assertEqual(data['messages_endpoint'], self.stream.messages_endpoint)

    def test_endpoint_flag_rejects_invalid_values(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.run_main(options=['--endpoint', 'invalid'])
        self.assertEqual(error.exception.code, 2)

    def test_pagination_finds_stream_on_later_page(self):
        self.admin.list_streams.side_effect = [SimpleNamespace(data=[], headers={'opc-next-page': 'page-2'}),
                                             SimpleNamespace(data=[self.stream], headers={})]
        self.client.get_group.side_effect = ServiceError(404)
        self.assertEqual(self.run_main()[0], 0)
        self.assertEqual(self.admin.list_streams.call_args.kwargs['page'], 'page-2')

    def test_missing_or_ambiguous_stream_fails_before_probing(self):
        for streams in ([], [self.stream, self.stream]):
            with self.subTest(streams=len(streams)):
                self.admin.list_streams.return_value.data = streams
                status, data, err = self.run_main()
                self.assertEqual(status, 1)
                self.assertIsNone(data)
                self.assertIn('Error:', err)
        self.client.get_group.assert_not_called()

    def test_help_describes_probe_range_and_requires_stream_parameter(self):
        with redirect_stdout(io.StringIO()) as output, self.assertRaises(SystemExit) as error:
            MODULE['main'](['--help'])
        self.assertEqual(error.exception.code, 0)
        self.assertIn('cp1 through cp9', output.getvalue())
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            MODULE['main']([])
        self.assertEqual(error.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
