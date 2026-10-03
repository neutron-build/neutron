"""Serial native-oracle calibration, then separately reviewed characterization.

Consumer descriptor JSON: clients maps each frozen client label to command
(argv list), artifact_root and artifact_manifest; source_revision is exact SHA.
No command is run until all five installed artifact manifests pass validation.
"""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from protocol import verify_artifacts

PROFILE = Path(__file__).with_name('profile.json')
MARKER = 'polyglot-performance-v1:'

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def telemetry():
    result = {'wall_ns': time.time_ns(), 'loadavg': list(os.getloadavg())}
    for name in ('stat', 'meminfo'):
        path = Path('/proc') / name
        if path.exists(): result[name] = path.read_text()
    return result

def calibration(samples, profile):
    """Ordered pairs are same-client/raw A/A, never different language runs."""
    times = [item['elapsed_ns'] for item in samples]
    ratios = [times[i+1] / times[i] for i in range(0, len(times), 2)]
    cv = statistics.stdev(times) / statistics.mean(times)
    low, high = profile['aa_pair_ratio']
    mlow, mhigh = profile['aa_median_ratio']
    reasons = []
    if min(times) < profile['minimum_sample_ns']: reasons.append('sample-too-short')
    if not all(low <= value <= high for value in ratios): reasons.append('pair-drift')
    if not mlow <= statistics.median(ratios) <= mhigh: reasons.append('median-drift')
    if cv > profile['maximum_cv']: reasons.append('variance')
    return {'accepted': not reasons, 'reasons': reasons, 'pair_ratios': ratios, 'cv': cv}

def invoke(command, request, timeout=90):
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        output, _ = process.communicate(json.dumps(request), timeout=timeout)
    except BaseException:
        try: os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        process.wait()
        raise
    if process.returncode: raise ValueError('performance consumer failed (native diagnostics suppressed)')
    result = json.loads(output)
    for key in ('protocol', 'schema_scope', 'mode', 'workload', 'iterations', 'warmup', 'artifact_hashes'):
        if result.get(key) != request[key]: raise ValueError('performance response identity mismatch')
    expected = sum(9007199254740993 + i % 64 + 1 for i in range(request['iterations']))
    count = request['iterations'] * (4 if request['workload'] == 'transaction' else 1)
    if result.get('checksum') != str(expected) or result.get('query_count') != count:
        raise ValueError('performance checksum/query count mismatch')
    elapsed = result.get('elapsed_ns')
    if type(elapsed) is not int or elapsed <= 0: raise ValueError('invalid elapsed time')
    return result

def ownership(conn, scope, token):
    row = conn.execute('SELECT pg_catalog.obj_description(oid, %s) FROM pg_catalog.pg_namespace WHERE nspname=%s', ('pg_namespace', scope)).fetchone()
    if row != (MARKER + token,): raise ValueError('native fixture ownership mismatch')

def sample(conn, scope, token, descriptor, hashes, profile, mode, workload):
    from psycopg import sql
    ownership(conn, scope, token)
    conn.execute(sql.SQL("UPDATE {}.perf_fixture SET body='base:' || id::text").format(sql.Identifier(scope)))
    request = {'protocol': profile['protocol'], 'schema_scope': scope, 'mode': mode,
        'workload': workload, 'warmup': profile['warmup'], 'iterations': profile['iterations'],
        'artifact_hashes': hashes}
    before = telemetry()
    result = invoke(descriptor['command'], request)
    after = telemetry()
    ownership(conn, scope, token)
    actual = conn.execute(sql.SQL('SELECT id, big::text, body FROM {}.perf_fixture ORDER BY id').format(sql.Identifier(scope))).fetchall()
    expected = []
    for key in range(1, 65):
        body = 'base:' + str(key)
        if workload == 'transaction':
            final = max(i for i in range(profile['iterations']) if i % 64 + 1 == key)
            body = 'measure:' + str(final)
        expected.append((key, str(9007199254740993 + key), body))
    if actual != expected: raise ValueError('independent native full-state oracle differs')
    return {**result, 'host_before': before, 'host_after': after, 'native_oracle': 'pass'}

def run(args):
    profile = json.loads(PROFILE.read_text())
    descriptor = json.loads(args.consumers.read_text())
    import re
    if not re.fullmatch('[0-9a-f]{40}', descriptor.get('source_revision', '')):
        raise ValueError('exact source revision required')
    if set(descriptor['clients']) != set(profile['clients']): raise ValueError('all five frozen clients required')
    identities = {}
    for label, client in descriptor['clients'].items():
        if not isinstance(client['command'], list) or not client['command'] or not all(isinstance(x, str) for x in client['command']):
            raise ValueError('consumer command must be argv')
        manifest = Path(client['artifact_manifest'])
        identities[label] = verify_artifacts(json.loads(manifest.read_text()), Path(client['artifact_root']))
        if label.startswith(('python-','typescript-')):
            runtime=json.loads((Path(client['artifact_root'])/'performance/runtime-identity.json').read_text())
            executable=Path(client['command'][0]).resolve()
            if str(executable)!=runtime['executable'] or digest(executable)!=runtime['sha256']:
                raise ValueError('consumer runtime bytes changed after preparation')
    binding = {'profile_sha256': digest(PROFILE), 'consumers_sha256': digest(args.consumers),
        'source_revision': descriptor['source_revision'], 'artifacts': identities,
        'harness_sha256': {str(path.relative_to(Path(__file__).resolve().parents[1])): digest(path)
            for path in (Path(__file__).resolve(), Path(__file__).with_name('prepare.py'), Path(__file__).resolve().parents[1] / 'protocol.py')}}
    if args.phase == 'characterize':
        if args.calibration is None or args.review is None: raise ValueError('separate calibration and review required')
        prior = json.loads(args.calibration.read_text())
        review = json.loads(args.review.read_text())
        if prior.get('phase') != 'calibrate' or prior.get('binding') != binding or not prior.get('accepted'):
            raise ValueError('passing matching calibration required')
        if review.get('calibration_sha256') != digest(args.calibration) or review.get('verdict') != 'accepted' or not review.get('reviewer'):
            raise ValueError('independent calibration review identity required')
    import psycopg
    from psycopg import sql
    scope, token = 'neutron_polyglot_' + uuid.uuid4().hex, uuid.uuid4().hex + uuid.uuid4().hex
    report = {'phase': args.phase, 'binding': binding, 'profile': profile, 'samples': {}, 'gates': {},
        'accepted': False, 'claims': profile['claims'], 'schema_scope':scope, 'ownership_token':token}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    def save(): args.output.write_text(json.dumps(report, indent=2) + '\n')
    with open('/tmp/neutron-polyglot-performance.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with psycopg.connect(os.environ['NEUTRON_TEST_DATABASE_URL'], autocommit=True) as conn:
            report['postgres'] = conn.execute("SELECT version(), current_setting('default_transaction_isolation'), current_setting('shared_buffers'), current_setting('max_connections')").fetchone()
            # Fail before mutation on an unqualified endpoint/isolation.
            if not report['postgres'][0].startswith('PostgreSQL ') or report['postgres'][1] != profile['isolation']:
                raise ValueError('native PostgreSQL READ COMMITTED endpoint required')
            created = False
            try:
                conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(scope)))
                conn.execute(sql.SQL('COMMENT ON SCHEMA {} IS {}').format(sql.Identifier(scope), sql.Literal(MARKER + token)))
                created = True
                conn.execute(sql.SQL('CREATE TABLE {}.perf_fixture(id integer PRIMARY KEY, big bigint NOT NULL, body text NOT NULL)').format(sql.Identifier(scope)))
                conn.execute(sql.SQL("INSERT INTO {}.perf_fixture SELECT i, 9007199254740993::bigint+i, 'base:'||i FROM generate_series(1,64) i").format(sql.Identifier(scope)))
                conn.execute(sql.SQL('ANALYZE {}.perf_fixture').format(sql.Identifier(scope)))
                for label in profile['clients']:
                    for workload in profile['workloads']:
                        key=label+'/'+workload
                        report.setdefault('preflight', {})[key] = [sample(conn, scope, token, descriptor['clients'][label], identities[label], profile, mode, workload) for mode in ('raw', 'orm')]
                        save()
                for label in profile['clients']:
                    client = descriptor['clients'][label]
                    for workload in profile['workloads']:
                        key = label + '/' + workload
                        # Correctness runs precede all timing evidence; they are retained
                        # but excluded from calibration and characterization statistics.
                        sequence = ['raw'] * (2 * profile['aa_pairs']) if args.phase == 'calibrate' else ['raw','orm','orm','raw'] * profile['abba_rounds']
                        samples = report['samples'][key] = []
                        for mode in sequence:
                            samples.append(sample(conn, scope, token, client, identities[label], profile, mode, workload))
                            save()
                        if args.phase == 'calibrate': report['gates'][key] = calibration(samples, profile)
                        else:
                            bracket = [max(samples[i]['elapsed_ns'],samples[i+3]['elapsed_ns']) / min(samples[i]['elapsed_ns'],samples[i+3]['elapsed_ns']) for i in range(0,len(samples),4)]
                            raw = [x['elapsed_ns'] for x in samples if x['mode']=='raw']
                            orm = [x['elapsed_ns'] for x in samples if x['mode']=='orm']
                            accepted = max(bracket) <= profile['maximum_bracket_drift'] and min(raw+orm) >= profile['minimum_sample_ns']
                            report['gates'][key] = {'accepted':accepted, 'bracket_drift':bracket,
                                'median_orm_over_raw':statistics.median(orm)/statistics.median(raw)}
                        save()
                report['accepted'] = all(g['accepted'] for g in report['gates'].values())
                for label,client in descriptor['clients'].items():
                    if verify_artifacts(json.loads(Path(client['artifact_manifest']).read_text()),Path(client['artifact_root']))!=identities[label]:
                        raise ValueError('installed artifact identity changed during campaign')
            finally:
                if created:
                    ownership(conn, scope, token)
                    conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(scope)))
                save()
    return 0 if report['accepted'] else 2

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--consumers', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--phase', choices=('calibrate','characterize'), default='calibrate')
    parser.add_argument('--calibration', type=Path)
    parser.add_argument('--review', type=Path)
    args = parser.parse_args()
    try: return run(args)
    except Exception:
        print('performance campaign refused or failed; native diagnostics suppressed', file=sys.stderr)
        return 1

if __name__ == '__main__': raise SystemExit(main())
