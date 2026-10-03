"""Run trusted manifests against isolated native-oracle fixtures."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import uuid
from protocol import PROTOCOL, redact, verify_artifacts

ROOT = Path(__file__).resolve().parent
EXPECTED = [['1', '9223372036854775807', '12345678901234567890.123456789', '2024-01-02 03:04:05.123456+00', True, 'null']]

def invoke(command: list[str], request: dict, timeout: float) -> dict:
    if not command or not all(isinstance(x, str) for x in command):
        raise ValueError('adapter command must be nonempty argv')
    proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        stdout, stderr = proc.communicate(json.dumps(request), timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        raise ValueError('adapter timeout')
    if proc.returncode:
        raise ValueError(redact(stderr + stdout, os.environ.get('NEUTRON_TEST_DATABASE_URL', '')) or 'adapter failed')
    try:
        result = json.loads(stdout)
    except ValueError as exc:
        raise ValueError('adapter stdout must contain one JSON envelope') from exc
    if not isinstance(result, dict) or result.get('protocol') != PROTOCOL or result.get('case_id') != request['case_id']:
        raise ValueError('adapter envelope identity mismatch')
    if result.get('status') != 'pass':
        raise ValueError('required case failed or unsupported')
    return result

def validate_manifest(manifest: dict) -> list:
    if manifest.get('protocol') != PROTOCOL:
        raise ValueError('manifest protocol mismatch')
    cases = manifest.get('cases')
    if not isinstance(cases, list) or not cases:
        raise ValueError('nonempty case list required')
    seen = set()
    for case in cases:
        if case.get('id') != 'scalar-extremes' or case['id'] in seen:
            raise ValueError('unknown or duplicate case')
        seen.add(case['id'])
        if case.get('kind') not in {'oracle-self-check', 'adapter-read'}:
            raise ValueError('unsupported case kind')
        if case['kind'] == 'adapter-read' and not case.get('command'):
            raise ValueError('adapter command required')
    return cases

def run(manifest: dict, artifacts: dict, required: bool, timeout: float) -> dict:
    cases = validate_manifest(manifest)
    if not os.environ.get('NEUTRON_TEST_DATABASE_URL'):
        if required:
            raise ValueError('required live database URL missing')
        return {'status': 'not-run', 'executed': 0, 'reason': 'live database not configured'}
    passed = 0
    for case in cases:
        request = {'protocol': PROTOCOL, 'case_id': case['id'], 'profile': 'postgres-direct', 'schema_scope': 'neutron_polyglot_' + uuid.uuid4().hex, 'ownership_token': secrets.token_hex(32), 'artifact_hashes': artifacts}
        oracle = [sys.executable, str(ROOT / 'oracle.py')]
        setup = False
        primary_error = None
        try:
            setup = True
            invoke(oracle, {**request, 'action': 'setup'}, timeout)
            baseline = invoke(oracle, {**request, 'action': 'observe'}, timeout)
            if baseline.get('rows') != EXPECTED:
                raise ValueError('native fixture differs from independently specified expected rows')
            if case['kind'] == 'adapter-read':
                result = invoke(case['command'], {**request, 'action': 'observe'}, timeout)
                if result.get('artifact_hashes') != artifacts or result.get('rows') != baseline['rows']:
                    raise ValueError('adapter artifact identity or independent oracle comparison failed')
            passed += 1
        except Exception as exc:
            primary_error = exc
        finally:
            if setup:
                try:
                    invoke(oracle, {**request, 'action': 'cleanup'}, timeout)
                except Exception as cleanup:
                    raise ValueError(f'cleanup failed for {request["schema_scope"]}; manual reconciliation required') from cleanup
        if primary_error:
            raise primary_error
    return {'status': 'pass', 'executed': passed, 'kind': 'oracle-self-check' if all(c['kind'] == 'oracle-self-check' for c in cases) else 'adapter-conformance', 'artifact_hashes': artifacts}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--artifact-manifest', type=Path)
    parser.add_argument('--artifact-root', type=Path, default=Path.cwd())
    parser.add_argument('--required', action='store_true')
    parser.add_argument('--validate-only', action='store_true')
    parser.add_argument('--timeout', type=float, default=30)
    args = parser.parse_args()
    try:
        if args.timeout <= 0:
            raise ValueError('positive timeout required')
        manifest = json.loads(args.manifest.read_text())
        validate_manifest(manifest)
        if args.validate_only:
            print(json.dumps({'status': 'validated', 'executed': 0}))
            return 0
        artifact_file = args.artifact_manifest or (Path(os.environ['NEUTRON_ARTIFACT_MANIFEST']) if os.environ.get('NEUTRON_ARTIFACT_MANIFEST') else None)
        if artifact_file is None:
            raise ValueError('artifact manifest required')
        artifacts = verify_artifacts(json.loads(artifact_file.read_text()), args.artifact_root)
        print(json.dumps(run(manifest, artifacts, args.required or os.environ.get('NEUTRON_LIVE_REQUIRED') == '1', args.timeout)))
        return 0
    except Exception as exc:
        print(json.dumps({'status': 'fail', 'diagnostics': redact(str(exc), os.environ.get('NEUTRON_TEST_DATABASE_URL', ''))}))
        return 1

if __name__ == '__main__':
    raise SystemExit(main())
