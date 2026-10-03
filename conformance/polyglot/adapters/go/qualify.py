#!/usr/bin/env python3
"""Build a real Go source-snapshot consumer outside the origin and run oracle.

Run from the integrated repository root with Go1.26+, psycopg and the private
NEUTRON_TEST_DATABASE_URL. This is archived-module scalar-read qualification;
it does not claim a published module, external registry install, or full ORM
coverage. All consumer source/build artifacts remain in an owned temporary dir.
"""
from pathlib import Path, PurePosixPath
import hashlib
import io
import json
import os
import re
import signal
import subprocess
import sys
import tarfile
import tempfile

root = Path.cwd().resolve()
consumer = Path(tempfile.mkdtemp(prefix='neutron-polyglot-go-consumer-')).resolve()
if consumer.is_relative_to(root):
    raise SystemExit('consumer must be outside the origin')
build_env = {key: value for key, value in os.environ.items()
             if not key.endswith('DATABASE_URL') and not key.startswith('PG') and key != 'DB_URL'}
build_env.update({'GOWORK': 'off', 'GOTOOLCHAIN': 'local', 'GOFLAGS': ''})

def run(argv, cwd=root, timeout=180, environment=None):
    process = subprocess.Popen(argv, cwd=cwd, env=build_env if environment is None else environment, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        stdout, _ = process.communicate(timeout=timeout)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate()
        raise

    if process.returncode:
        # Registry/driver errors can contain private URLs; never print them.
        raise RuntimeError('Go qualification subprocess failed')
    return stdout

def source_paths(provenance):
    manifest, revision = root / 'source-manifest.json', root / 'source-revision'
    if manifest.exists() or revision.exists():
        if manifest.is_symlink() or revision.is_symlink() or not manifest.is_file() or not revision.is_file():
            raise RuntimeError('regular coordinator provenance files required')
        names = json.loads(manifest.read_text())
        raw_revision = revision.read_text()
        if not re.fullmatch(r'[0-9a-f]{40}\n?', raw_revision):
            raise RuntimeError('exact coordinator source revision required')
        sha = raw_revision.rstrip('\n')
        (provenance / 'source-manifest.json').write_bytes(manifest.read_bytes())
        (provenance / 'source-revision').write_bytes(revision.read_bytes())
        kind = 'coordinator-source-archive'
    else:
        names = [name for name in run(['git', 'ls-files', '-z', '--', 'go']).split('\0') if name]
        sha = run(['git', 'rev-parse', 'HEAD']).strip()
        (provenance / 'source-manifest.json').write_text(json.dumps(names))
        (provenance / 'source-revision').write_text(sha)
        kind = 'git-worktree-snapshot'
    if not re.fullmatch(r'[0-9a-f]{40}', sha) or not isinstance(names, list) or not names:
        raise RuntimeError('exact source revision and path manifest required')
    if any(not isinstance(name, str) or not name or '\\' in name or '\0' in name or
           any(part in {'', '.', '..'} for part in name.split('/')) or
           PurePosixPath(name).is_absolute() for name in names) or len(set(names)) != len(names):
        raise RuntimeError('regular relative unique source paths required')
    paths = []
    for name in names:
        if not name.startswith('go/'):
            continue
        source = root / name
        prefix = root
        for part in PurePosixPath(name).parts:
            prefix = prefix / part
            if prefix.is_symlink():
                raise RuntimeError('source symlink ancestors refused')
        if not source.is_file() or not source.resolve().is_relative_to(root / 'go'):
            raise RuntimeError('regular contained Go source required')
        paths.append(source)
    (provenance / 'identity.json').write_text(json.dumps({'revision': sha, 'kind': kind}))
    return sorted(paths)

try:
    database_url = os.environ.get('NEUTRON_TEST_DATABASE_URL')
    if not database_url:
        raise RuntimeError('private disposable database environment required')
    provenance = consumer / 'provenance'
    provenance.mkdir()
    (provenance / 'qualify.py').write_bytes(Path(__file__).read_bytes())
    paths = source_paths(provenance)
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
    if re.search(r'(?m)^\s*replace\b', original):
        raise RuntimeError('source module replacement directives refused')
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
    manifest = consumer / 'cases.json'
    manifest.write_text(json.dumps({'protocol': 'polyglot-conformance-v1', 'cases': [
        {'id': 'scalar-extremes', 'kind': 'adapter-read', 'command': [str(binary)]}]}))
    # Bind the command manifest as well as source provenance and executable.
    artifact_paths = [archive, binary, consumer / 'toolchain.txt', manifest]
    artifact_paths.extend(p for directory in [module, app, coordinator, provenance] for p in directory.rglob('*') if p.is_file())
    artifacts = consumer / 'artifacts.json'
    artifacts.write_text(json.dumps({'files': [
        {'path': str(p.relative_to(consumer)), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
        for p in sorted(artifact_paths)]}))
    runner_env = {**build_env, 'NEUTRON_TEST_DATABASE_URL': database_url}
    result = json.loads(run([sys.executable, str(coordinator / 'runner.py'),
        '--manifest', str(manifest), '--artifact-manifest', str(artifacts),
        '--artifact-root', str(consumer), '--required'], environment=runner_env))
    report = {'status': 'pass', 'scope': 'outside-origin archived Go module scalar read only',
              'consumer': str(consumer), 'result': result}
    (consumer / 'result.json').write_text(json.dumps(report))
    print(json.dumps({'status': 'pass', 'consumer': str(consumer),
                      'artifact_manifest': str(artifacts), 'scope': report['scope']}))
except BaseException:
    print(json.dumps({'status': 'fail', 'diagnostics': 'outside-origin Go scalar qualification failed',
                      'consumer': str(consumer)}))
    raise SystemExit(1)
