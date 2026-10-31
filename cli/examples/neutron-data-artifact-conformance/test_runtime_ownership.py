"""Failure diagnostics must never write into an unowned directory."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class RuntimeOwnership(unittest.TestCase):
    def run_refused(self, cwd, runtime):
        env = dict(os.environ, ADMIN_DATABASE_URL='postgres://unused.invalid/unused',
                   NEUTRON_CLI=sys.executable, ARTIFACT_TS_DEPS=str(cwd),
                   ARTIFACT_TSC=sys.executable)
        env.pop('ARTIFACT_RUNTIME', None)
        if runtime is not None:
            env['ARTIFACT_RUNTIME'] = str(runtime)
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('run.py'))],
                                cwd=cwd, env=env, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('FAIL:', result.stderr)

    def test_missing_runtime_does_not_write_to_working_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            cwd = Path(directory)
            self.run_refused(cwd, None)
            self.assertEqual(list(cwd.iterdir()), [])

    def test_existing_runtime_is_not_modified(self):
        with tempfile.TemporaryDirectory() as directory:
            cwd = Path(directory)
            sentinel = cwd / 'sentinel'
            sentinel.write_text('unowned data')
            self.run_refused(cwd, cwd)
            self.assertEqual(sorted(p.name for p in cwd.iterdir()), ['sentinel'])
            self.assertEqual(sentinel.read_text(), 'unowned data')


if __name__ == '__main__':
    unittest.main()
