"""Pack/install the real SQL client and compare both drivers with native oracle.

Run from repository root with an already built SQL package, Node/npm, psycopg,
and a private NEUTRON_TEST_DATABASE_URL. Artifacts remain in an owned temporary
consumer directory; this is a local package qualification, not an npm release.
"""
from pathlib import Path
import hashlib
import json
import subprocess
import sys
import tempfile

root = Path.cwd().resolve()
consumer = Path(tempfile.mkdtemp(prefix='.polyglot-ts-consumer-', dir=root))
package = root / 'typescript/packages/neutron-sql'

def run(argv, cwd=root):
    result = subprocess.run(argv, cwd=cwd, capture_output=True, text=True)
    if result.returncode:
        # Registry/driver subprocess diagnostics may include private URLs.
        raise RuntimeError('package qualification subprocess failed')
    return result.stdout

try:
    packed = json.loads(run(['npm', 'pack', '--json', '--pack-destination', str(consumer)], package))[0]
    archive = consumer / packed['filename']
    (consumer / 'package.json').write_text(json.dumps({'private': True, 'type': 'module'}))
    versions = {}
    for driver in ['pg', 'postgres']:
        versions[driver] = json.loads((package / 'node_modules' / driver / 'package.json').read_text())['version']
    run(['npm', 'install', '--no-audit', '--no-fund', str(archive), *[f'{k}@{v}' for k, v in versions.items()]], consumer)
    installed = consumer / 'node_modules/@neutron-build/sql'
    paths = [archive, consumer / 'package-lock.json', root / 'conformance/polyglot/adapters/typescript.mjs']
    paths.extend(p for p in installed.rglob('*') if p.is_file())
    artifacts = consumer / 'artifacts.json'
    artifacts.write_text(json.dumps({'files': [{'path': str(p.relative_to(root)), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(paths)]}))
    results = []
    for driver in versions:
        command = ['node', 'conformance/polyglot/adapters/typescript.mjs', '--module', str(installed / 'dist/index.js')]
        if driver == 'postgres': command.append('--postgres-js')
        manifest = consumer / f'{driver}-cases.json'
        manifest.write_text(json.dumps({'protocol': 'polyglot-conformance-v1', 'cases': [{'id': 'scalar-extremes', 'kind': 'adapter-read', 'command': command}]}))
        output = run([sys.executable, 'conformance/polyglot/runner.py', '--manifest', str(manifest), '--artifact-manifest', str(artifacts), '--required'])
        results.append({'driver': driver, 'result': json.loads(output)})
    report = {'status': 'pass', 'scope': 'installed TypeScript scalar read only', 'consumer': str(consumer.relative_to(root)), 'results': results}
    (consumer / 'result.json').write_text(json.dumps(report))
    print(json.dumps({'status': 'pass', 'drivers': list(versions), 'artifact_manifest': str(artifacts.relative_to(root))}))
except Exception:
    print(json.dumps({'status': 'fail', 'diagnostics': 'installed TypeScript scalar qualification failed', 'consumer': str(consumer.relative_to(root))}))
    raise SystemExit(1)
