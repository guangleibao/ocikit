"""Exercise the shell collector with fixture-backed curl; never contact real IMDS."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'getInstanceMetadata.sh'
FAKE_CURL = r'''
import json
import os
from pathlib import Path
import sys
args = sys.argv[1:]
assert args[0] == '-q', args
assert args[args.index('--noproxy') + 1] == '*'
assert args[args.index('-H') + 1] == 'Authorization: Bearer Oracle'
assert args[args.index('--connect-timeout') + 1] == '3'
assert args[args.index('--max-time') + 1] == '30'
assert '--location' not in args and '-L' not in args
assert not any(arg.startswith('--retry') for arg in args)
assert args[args.index('--write-out') + 1] == '%{http_code}'
url = args[-1]
assert url.startswith('http://imds.invalid/opc/v2/'), url
path = url.removeprefix('http://imds.invalid/opc/v2')
fixture = json.loads(Path(os.environ['IMDS_FIXTURE']).read_text())
log = Path(os.environ['IMDS_LOG'])
calls = log.read_text().splitlines() if log.exists() else []
entry = fixture.get(path, {'status': 404, 'body': 'Not found\n'})
if isinstance(entry, list):
    entry = entry[min(calls.count(path), len(entry) - 1)]
with log.open('a') as stream:
    stream.write(path + '\n')
body = entry.get('body', '')
Path(args[args.index('--output') + 1]).write_text(body if isinstance(body, str) else json.dumps(body))
sys.stdout.write(str(entry.get('status', 200)))
sys.stderr.write(entry.get('error', ''))
sys.exit(entry.get('exit', 0))
'''


class MetadataTests(unittest.TestCase):
    def run_collector(self, fixture=None, options=()):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy2(SCRIPT, root / SCRIPT.name)
            (root / '.env').write_text('OCI_METADATA_BASE_URL=http://imds.invalid/opc/v2/\n')
            binary = root / 'bin'
            binary.mkdir()
            (binary / 'curl').write_text('#!' + sys.executable + '\n' + FAKE_CURL)
            (binary / 'curl').chmod(0o755)
            (binary / 'sleep').write_text('#!/bin/sh\ntouch "$IMDS_SLEEP_LOG"\nexit 99\n')
            (binary / 'sleep').chmod(0o755)
            fixture_path, log = root / 'fixture.json', root / 'calls'
            fixture_path.write_text(json.dumps(fixture if fixture is not None else self.fixture()))
            temporary = root / 'temporary'
            temporary.mkdir()
            env = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ['PATH'],
                       IMDS_FIXTURE=str(fixture_path), IMDS_LOG=str(log), TMPDIR=str(temporary),
                       IMDS_SLEEP_LOG=str(root / 'slept'))
            result = subprocess.run(['bash', str(root / SCRIPT.name), *options],
                                    cwd=root, env=env, capture_output=True, text=True, timeout=30)
            calls = log.read_text().splitlines() if log.exists() else []
            self.assertEqual(list(temporary.iterdir()), [], 'temporary metadata should be cleaned up')
            self.assertFalse((root / 'slept').exists(), 'failed IMDS paths must not trigger sleeps')
        return result, calls

    def fixture(self):
        return {
            '/instance/': {'body': {'id': 'instance', 'metadata': {'user_data': 'BASE64',
                           'custom': {'nested': [1, 2]}}, 'extendedMetadata': {'a': True}}},
            '/vnics/': {'body': [{'vnicId': 'vnic', 'secondaryPrivateIps': ['10.0.0.2']}]},
            '/volumeAttachments/': {'body': [{'id': 'volume'}]},
            '/host/': {'body': {'rack': 'fixture-rack', 'nested': {'value': 42}}},
            '/identity/cert.pem': {'body': 'TEST CERTIFICATE\n'},
            '/identity/intermediate.pem': {'body': 'TEST INTERMEDIATE\n'},
            '/identity/key.pem': {'body': 'TEST PRIVATE KEY\n'},
        }

    def test_collects_all_known_roots_and_preserves_nested_and_pem_data(self):
        result, calls = self.run_collector()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        metadata = report['metadata']
        self.assertEqual(metadata['/host/']['nested'], {'value': 42})
        self.assertEqual(metadata['/instance/']['metadata']['custom'], {'nested': [1, 2]})
        self.assertEqual(metadata['/identity/key.pem'], 'TEST PRIVATE KEY\n')
        self.assertEqual(metadata['/volumeAttachments/'], [{'id': 'volume'}])
        self.assertEqual(report['endpoints']['/host/']['http_status'], 200)
        self.assertEqual(report['summary']['unavailable'], 2)
        self.assertEqual(len(calls), 9)
        self.assertEqual(len(set(calls)), len(calls))

    def test_discovers_new_roots_and_nested_text_and_json_listings(self):
        fixture = self.fixture()
        fixture.update({
            '/': {'body': ['instance/', 'host/', 'extra/']},
            '/extra/': {'body': 'leaf\nsub/\n'},
            '/extra/leaf': {'body': {'new': 'metadata'}},
            '/extra/sub/': {'body': ['value']},
            '/extra/sub/value': {'body': 'arbitrary text\n'},
        })
        result, calls = self.run_collector(fixture)
        self.assertEqual(result.returncode, 0, result.stderr)
        metadata = json.loads(result.stdout)['metadata']
        self.assertEqual(metadata['/extra/sub/value'], 'arbitrary text\n')
        self.assertEqual(metadata['/extra/leaf'], {'new': 'metadata'})
        self.assertEqual(calls.count('/instance/'), 1)
        self.assertEqual(calls.count('/host/'), 1)

    def test_failed_root_listing_does_not_prevent_host_or_other_probes(self):
        fixture = self.fixture()
        fixture['/'] = {'status': 403, 'body': 'Forbidden'}
        fixture['/host/'] = {'status': 500, 'body': {'error': 'host unavailable'}}
        result, calls = self.run_collector(fixture)
        self.assertEqual(result.returncode, 1)
        report = json.loads(result.stdout)
        self.assertIn('/vnics/', report['metadata'])
        self.assertEqual(report['endpoints']['/host/']['body'], {'error': 'host unavailable'})
        self.assertEqual(report['summary']['errors'], 2)
        self.assertEqual(len(calls), 9)

    def test_optional_missing_host_is_explicit_and_nonfatal(self):
        fixture = self.fixture()
        fixture['/host/'] = {'status': 404, 'body': 'not supported'}
        result, _ = self.run_collector(fixture)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['endpoints']['/host/']['state'], 'unavailable')

    def test_immediately_skips_http_and_transport_errors_without_retry(self):
        for failure in ({'status': 404}, {'status': 429}, {'status': 503}, {'status': 403},
                        {'status': '000', 'exit': 28, 'error': 'timeout'},
                        {'status': '000', 'exit': 7, 'error': 'connection refused'}):
            with self.subTest(failure=failure):
                fixture = self.fixture()
                fixture['/volumeAttachments/'] = [failure, {'body': {'recovered': True}}]
                result, calls = self.run_collector(fixture)
                self.assertEqual(result.returncode, 0 if failure['status'] == 404 else 1, result.stderr)
                report = json.loads(result.stdout)
                self.assertEqual(report['endpoints']['/volumeAttachments/']['attempts'], 1)
                self.assertEqual(calls.count('/volumeAttachments/'), 1)
                self.assertNotIn('/volumeAttachments/', report['metadata'])
                self.assertIn('/host/', report['metadata'])
                self.assertNotIn('Retrying', result.stderr)
                self.assertEqual(len(calls), 9)

    def test_transport_failure_preserves_partial_output_and_exit_status(self):
        fixture = self.fixture()
        fixture['/host/'] = {'status': '000', 'exit': 7, 'error': 'connection refused'}
        result, _ = self.run_collector(fixture)
        self.assertEqual(result.returncode, 1)
        endpoint = json.loads(result.stdout)['endpoints']['/host/']
        self.assertEqual(endpoint['http_status'], 0)
        self.assertEqual(endpoint['curl_exit_code'], 7)
        self.assertEqual(endpoint['error'], 'connection refused')

    def test_instance_only_retains_original_json_output(self):
        fixture = self.fixture()
        result, calls = self.run_collector(fixture, options=['--instance-only'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), fixture['/instance/']['body'])
        self.assertEqual(calls, ['/instance/'])

    def test_missing_or_non_object_instance_is_failure(self):
        for entry in ({'status': 404}, {'body': 'not JSON'}, {'body': []}, {'body': 'null'}):
            with self.subTest(entry=entry):
                fixture = self.fixture()
                fixture['/instance/'] = entry
                result, _ = self.run_collector(fixture)
                self.assertEqual(result.returncode, 1)
                self.assertFalse(json.loads(result.stdout)['summary']['instance_valid'])

    def test_json_false_null_and_empty_text_are_preserved(self):
        fixture = self.fixture()
        fixture.update({'/host/': {'body': 'false'}, '/identity/key.pem': {'body': 'null'},
                        '/identity/cert.pem': {'body': ''}})
        result, _ = self.run_collector(fixture)
        self.assertEqual(result.returncode, 0, result.stderr)
        metadata = json.loads(result.stdout)['metadata']
        self.assertIs(metadata['/host/'], False)
        self.assertIsNone(metadata['/identity/key.pem'])
        self.assertEqual(metadata['/identity/cert.pem'], '')

    def test_discovery_does_not_follow_urls_or_parent_paths(self):
        for bad in ('../', 'https://outside.invalid/', '//outside.invalid/', '%2e%2e/', '-option', './'):
            with self.subTest(path=bad):
                fixture = self.fixture()
                fixture['/'] = {'body': [bad]}
                result, calls = self.run_collector(fixture)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len(calls), 9)

    def test_explicit_extra_endpoint_and_invalid_arguments(self):
        fixture = self.fixture()
        fixture['/host/extra'] = {'body': {'extra': 42}}
        result, calls = self.run_collector(fixture, options=['--endpoint', 'host/extra'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['metadata']['/host/extra'], {'extra': 42})
        for options in (['--endpoint', '../bad'], ['--endpoint', 'http://elsewhere'],
                        ['--endpoint', 'host//bad'], ['--retries', '0'], ['--unknown'],
                        ['--instance-only', '--endpoint', 'host/']):
            with self.subTest(options=options):
                result, calls = self.run_collector(options=options)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(calls, [])

    def test_recursive_directory_listing_is_bounded_and_reported(self):
        fixture = self.fixture()
        fixture['/'] = {'body': ['extra/']}
        for depth in range(20):
            fixture['/extra/' + 'child/' * depth] = {'body': ['child/']}
        result, calls = self.run_collector(fixture)
        self.assertEqual(result.returncode, 1)
        report = json.loads(result.stdout)
        self.assertTrue(report['summary']['discovery_truncated'])
        self.assertIn('/host/', report['metadata'])
        self.assertLess(len(calls), 30)

    def test_permanent_http_failure_is_not_retried(self):
        fixture = self.fixture()
        fixture['/host/'] = {'status': 403, 'body': 'forbidden'}
        fixture['/'] = {'body': {}}
        fixture['/identity/'] = {'body': {}}
        result, calls = self.run_collector(fixture)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(calls.count('/host/'), 1)
        self.assertEqual(json.loads(result.stdout)['endpoints']['/host/']['attempts'], 1)


if __name__ == '__main__':
    unittest.main()
