"""Run shell launchers offline with environments whose activation cannot be sourced."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class LauncherTests(unittest.TestCase):
    def test_selects_python_without_sourcing_activation(self):
        for script in ('runStreamConsumerDemo.sh', 'runStreamProducerDemo.sh'):
            for selection in ('caller', 'adjacent', 'active'):
                with self.subTest(script=script, selection=selection), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    deployment, caller = root / 'deployment', root / 'caller'
                    deployment.mkdir()
                    caller.mkdir()
                    shutil.copy2(ROOT / script, deployment / script)
                    (deployment / '.env').write_text('OCI_COMPARTMENT_ID=offline-compartment\n')
                    environment = {'caller': caller / '.venv', 'adjacent': deployment / '.venv',
                                   'active': root / 'active-environment'}[selection]
                    (environment / 'bin').mkdir(parents=True)
                    (environment / 'bin' / 'python').symlink_to(sys.executable)
                    (environment / 'bin' / 'activate').write_text(': "$OCIKIT_ACTIVATION_MUST_NOT_RUN"\n')
                    env = dict(os.environ)
                    for key in ('VIRTUAL_ENV', 'OSTYPE', 'OCIKIT_ACTIVATION_MUST_NOT_RUN'):
                        env.pop(key, None)
                    if selection == 'active':
                        env['VIRTUAL_ENV'] = str(environment)
                    args = ['sh', str(deployment / script), 'events', '--dry-run']
                    if 'Consumer' in script:
                        args.extend(['--group-name', 'offline-group'])
                    result = subprocess.run(args, cwd=caller, env=env, capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn('Dry run:', result.stdout)
                    self.assertNotIn('parameter not set', result.stderr)


if __name__ == '__main__':
    unittest.main()
