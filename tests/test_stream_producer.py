"""Offline checks for the Python embedded in the standalone producer."""
from contextlib import redirect_stderr, redirect_stdout
import io
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).resolve().parents[1] / 'runStreamProducer.sh'
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

    def test_environment_directive_and_cli_override(self):
        env = {'OCI_STREAM_ENDPOINT': 'private'}
        self.assertIn('discover private endpoint', self.dry_run([], env))
        self.assertIn('discover public endpoint', self.dry_run(['--endpoint', 'public'], env))

    def test_url_override_and_invalid_directive(self):
        self.assertIn('Endpoint: https://messages.example.com\n',
                      self.dry_run(['--endpoint', 'https://messages.example.com/']))
        with self.assertRaises(SystemExit) as error:
            self.dry_run(['--endpoint', 'invalid'])
        self.assertEqual(error.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
