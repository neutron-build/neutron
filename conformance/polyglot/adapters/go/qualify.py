#!/usr/bin/env python3
"""Build a real Go source-snapshot consumer outside the origin and run oracle.

Run from the integrated repository root with Go1.26+, psycopg and the private
NEUTRON_TEST_DATABASE_URL. This is archived-module scalar-read qualification;
it does not claim a published module, external registry install, or full ORM
coverage. All consumer source/build artifacts remain in an owned temporary dir.
"""
from pathlib import Path
import hashlib
import io
import json
import os
import signal
import subprocess
import sys
import tarfile
import tempfile

root = Path.cwd().resolve()
consumer = Path(tempfile.mkdtemp(prefix='neutron-polyglot-go-consumer-')).resolve()
if consumer.is_relative_to(root):
    raise SystemExit('consumer must be outside the origin')
env = {**os.environ, 'GOWORK': 'off', 'GOTOOLCHAIN': 'local', 'GOFLAGS': ''}

def run(argv, cwd=root, timeout=180):
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        stdout, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise RuntimeError('Go qualification subprocess timed out')
    if process.returncode:
        # Registry/driver errors can contain private URLs; never print them.
        raise RuntimeError('Go qualification subprocess failed')
    return stdout

try:
    tracked = run(['git', 'ls-files', '-z', '--', 'go']).split('\0')
    paths = sorted(root / name for name in tracked if name)
    if not paths or not (root / 'go/orm/scalar.go').is_file():
        raise RuntimeError('actual Go scalar package required')
    archive = consumer / 'go-module-source.tar'
    module = consumer / 'module'
    module.mkdir()
    with tarfile.open(archive, 'w', format=tarfile.PAX_FORMAT) as tar:
        for source in paths:
            if not source.is_file() or source.is_symlink():
                raise RuntimeError('regular tracked Go source required')
            relative = source.relative_to(root / 'go')
            data = source.read_bytes()
            info = tarfile.TarInfo(relative.as_posix())
            info.size, info.mode, info.mtime = len(data), 0o644, 0
            tar.addfile(info, io.BytesIO(data))
            destination = module / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
    app = consumer / 'app'
    app.mkdir()
    adapter = root / 'conformance/polyglot/adapters/go/main.go'
    (app / 'main.go').write_bytes(adapter.read_bytes())
    original = (module / 'go.mod').read_text()
    target = 'github.com/neutron-build/neutron/go'
    if not original.startswith('module ' + target + '\n'):
        raise RuntimeError('Go module identity mismatch')
    (app / 'go.mod').write_text(original.replace('module ' + target,
        'module neutron-polyglot-go-consumer', 1) +
        '\nrequire ' + target + ' v0.0.0\nreplace ' + target + ' => ../module\n')
    (app / 'go.sum').write_bytes((module / 'go.sum').read_bytes())
    binary = consumer / 'adapter'
    run(['go', 'build', '-mod=readonly', '-trimpath', '-o', str(binary), '.'], app)
    run(['go', 'mod', 'verify'], app)
    coordinator = consumer / 'coordinator'
    coordinator.mkdir()
    for name in ['runner.py', 'oracle.py', 'protocol.py']:
        (coordinator / name).write_bytes((root / 'conformance/polyglot' / name).read_bytes())
    (coordinator / 'scalar-cases.json').write_bytes((root / 'conformance/polyglot/cases/scalars.json').read_bytes())
    (consumer / 'toolchain.txt').write_text(run(['go', 'version']) + run(['go', 'version', '-m', str(binary)]))
    # Snapshot the exact module and consumer bytes, binary, and build identity.
    artifact_paths = [archive, binary, consumer / 'toolchain.txt']
    artifact_paths.extend(p for directory in [module, app, coordinator] for p in directory.rglob('*') if p.is_file())
    artifacts = consumer / 'artifacts.json'
    artifacts.write_text(json.dumps({'files': [
        {'path': str(p.relative_to(consumer)), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
        for p in sorted(artifact_paths)]}))
    manifest = consumer / 'cases.json'
    manifest.write_text(json.dumps({'protocol': 'polyglot-conformance-v1', 'cases': [
        {'id': 'scalar-extremes', 'kind': 'adapter-read', 'command': [str(binary)]}]}))
    result = json.loads(run([sys.executable, str(coordinator / 'runner.py'),
        '--manifest', str(manifest), '--artifact-manifest', str(artifacts),
        '--artifact-root', str(consumer), '--required']))
    report = {'status': 'pass', 'scope': 'outside-origin archived Go module scalar read only',
              'consumer': str(consumer), 'result': result}
    (consumer / 'result.json').write_text(json.dumps(report))
    print(json.dumps({'status': 'pass', 'consumer': str(consumer),
                      'artifact_manifest': str(artifacts), 'scope': report['scope']}))
except Exception:
    print(json.dumps({'status': 'fail', 'diagnostics': 'outside-origin Go scalar qualification failed',
                      'consumer': str(consumer)}))
    raise SystemExit(1)
