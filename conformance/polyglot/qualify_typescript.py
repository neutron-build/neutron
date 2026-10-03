"""Qualify an actual packed SQL package in an owned outside-origin consumer."""
from pathlib import Path
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile

root = Path.cwd().resolve()
consumer = Path(tempfile.mkdtemp(prefix='neutron-polyglot-ts-consumer-')).resolve()
package = root / 'typescript/packages/neutron-sql'

def run(argv, cwd=consumer, live=False):
    env = dict(os.environ)
    for key in ['NODE_OPTIONS','NODE_PATH','PYTHONPATH','PYTHONHOME']:
        env.pop(key,None)
    database_url = env.get('NEUTRON_TEST_DATABASE_URL')
    for key in list(env):
        if key.startswith('PG') or key.endswith(('DATABASE_URL','DB_URL')) or key == 'NEUTRON_SQL_TEST_URL':
            env.pop(key)
    if live and database_url:
        env['NEUTRON_TEST_DATABASE_URL'] = database_url
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        stdout, _ = process.communicate(timeout=180)
    except BaseException as exc:
        try: os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        while True:
            try: process.wait(); break
            except KeyboardInterrupt: continue
        process.stdout.close(); process.stderr.close()
        if isinstance(exc, subprocess.TimeoutExpired):
            raise RuntimeError('package qualification subprocess timed out') from exc
        raise
    if process.returncode:
        raise RuntimeError('package qualification subprocess failed')
    return stdout

try:
    if consumer.is_relative_to(root): raise RuntimeError('outside-origin consumer required')
    packed = json.loads(run(['npm', 'pack', '--json', '--pack-destination', str(consumer)], package))[0]
    archive = consumer / packed['filename']
    (consumer / 'package.json').write_text(json.dumps({'private': True, 'type': 'module'}))
    versions = {}
    for driver in ['pg', 'postgres']:
        versions[driver] = json.loads((package / 'node_modules' / driver / 'package.json').read_text())['version']
    run(['npm', 'install', '--no-audit', '--no-fund', str(archive), *[f'{k}@{v}' for k, v in versions.items()]])
    installed = consumer / 'node_modules/@neutron-build/sql'
    tooling = consumer / 'tooling'; tooling.mkdir()
    for name in ['runner.py', 'oracle.py', 'protocol.py']:
        (tooling / name).write_bytes((root / 'conformance/polyglot' / name).read_bytes())
    adapter = tooling / 'typescript.mjs'
    adapter.write_bytes((root / 'conformance/polyglot/adapters/typescript.mjs').read_bytes())
    manifests = []
    for driver in versions:
        command = ['node', str(adapter), '--module', str(installed / 'dist/index.js')]
        if driver == 'postgres': command.append('--postgres-js')
        manifest = consumer / f'{driver}-cases.json'
        manifest.write_text(json.dumps({'protocol': 'polyglot-conformance-v1', 'cases': [{'id': 'scalar-extremes', 'kind': 'adapter-read', 'command': command}]}))
        manifests.append(manifest)
    (consumer / 'toolchain.txt').write_text(run(['node','--version']) + run([sys.executable,'-I','--version']))
    paths = [p for p in consumer.rglob('*') if p.is_file() and not p.is_symlink()]
    artifacts = consumer / 'artifacts.json'
    artifacts.write_text(json.dumps({'files': [{'path': str(p.relative_to(consumer)), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(paths)]}))
    results = []
    for driver, manifest in zip(versions, manifests):
        bootstrap = "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_path(sys.argv.pop(1),run_name='__main__')"
        output = run([sys.executable, '-I', '-B', '-c', bootstrap, str(tooling), str(tooling / 'runner.py'), '--manifest', str(manifest),
            '--artifact-manifest', str(artifacts), '--artifact-root', str(consumer), '--required'], live=True)
        result = json.loads(output)
        if result.get('status') != 'pass' or result.get('executed') != 1 or result.get('kind') != 'adapter-conformance':
            raise RuntimeError('required installed TypeScript adapter failed')
        results.append({'driver': driver, 'result': result})
    report = {'status': 'pass', 'scope': 'outside-origin installed TypeScript scalar read only', 'consumer': str(consumer), 'results': results}
    (consumer / 'result.json').write_text(json.dumps(report))
    print(json.dumps({'status': 'pass', 'drivers': list(versions), 'consumer': str(consumer), 'artifact_manifest': str(artifacts)}))
except BaseException:
    print(json.dumps({'status': 'fail', 'diagnostics': 'installed TypeScript scalar qualification failed', 'consumer': str(consumer)}))
    raise SystemExit(1)
