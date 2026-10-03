"""Independent native-oracle qualification of every declared writer/reader pair.

Trusted adapter commands only. Each descriptor binds its own installed artifact
root/manifest; no source adapter fallback or skip-as-pass is permitted.
"""
from __future__ import annotations
import argparse
import json
import math
import os
from pathlib import Path
import secrets
import sys
import uuid
from protocol import PROTOCOL, redact, verify_artifacts
from runner import EXPECTED, ROOT, invoke

INSERTED = ['2', '-9223372036854775808', '-98765432109876543210.000000001',
            '2038-01-19 03:14:07.654321+00', True, 'null']

def descriptors(manifest: dict) -> list[dict]:
    if manifest.get('protocol') != PROTOCOL:
        raise ValueError('cross manifest protocol mismatch')
    clients = manifest.get('clients')
    if not isinstance(clients, list) or len(clients) < 3:
        raise ValueError('at least three actual language clients required')
    seen = set(); result = []
    for client in clients:
        name = client.get('id')
        if not isinstance(name, str) or not name or name in seen:
            raise ValueError('unique client identity required')
        seen.add(name)
        if client.get('language') not in {'typescript','python','go'}:
            raise ValueError('known client language required')
        command = client.get('command')
        if not isinstance(command, list) or not command or not all(isinstance(x,str) and x for x in command):
            raise ValueError('actual client command required')
        artifacts = verify_artifacts(json.loads(Path(client['artifact_manifest']).read_text()),Path(client['artifact_root']))
        result.append({**client,'hashes':artifacts})
    if {c['language'] for c in result} != {'typescript','python','go'}:
        raise ValueError('all three language clients required')
    return result

def run_cross(clients: list[dict], timeout: float) -> dict:
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('finite positive timeout required')
    if not os.environ.get('NEUTRON_TEST_DATABASE_URL'):
        raise ValueError('required native database URL missing')
    oracle = [sys.executable,str(ROOT/'oracle.py')]
    results = []
    for writer in clients:
        for reader in clients:
            request = {'protocol':PROTOCOL,'case_id':'scalar-extremes','profile':'postgres-direct',
                'schema_scope':'neutron_polyglot_'+uuid.uuid4().hex,'ownership_token':secrets.token_hex(32)}
            try:
                invoke(oracle,{**request,'action':'setup'},timeout)
                baseline = invoke(oracle,{**request,'action':'observe'},timeout)
                if baseline.get('rows') != EXPECTED:
                    raise ValueError('independent native baseline differs')
                written = invoke(writer['command'],{**request,'action':'insert','artifact_hashes':writer['hashes']},timeout)
                if written.get('artifact_hashes') != writer['hashes']:
                    raise ValueError('writer artifacts differ')
                observed = invoke(oracle,{**request,'action':'observe'},timeout)
                if observed.get('rows') != EXPECTED+[INSERTED]:
                    raise ValueError('actual writer differs from independent expected SQL values')
                read = invoke(reader['command'],{**request,'action':'observe','artifact_hashes':reader['hashes']},timeout)
                if read.get('artifact_hashes') != reader['hashes'] or read.get('rows') != observed['rows']:
                    raise ValueError('reader artifacts or independent native comparison differs')
                results.append({'writer':writer['id'],'reader':reader['id'],'status':'pass'})
            finally:
                try: invoke(oracle,{**request,'action':'cleanup'},timeout)
                except Exception as exc:
                    raise ValueError('cross schema cleanup failed; manual reconciliation required: '+request['schema_scope']) from exc
    return {'status':'pass','kind':'cross-language-scalar-write-read','executed':len(results),'results':results,
            'scope':'declared scalar fixture only; not full ORM or engine certification'}

def main() -> int:
    parser=argparse.ArgumentParser();parser.add_argument('--manifest',type=Path,required=True);parser.add_argument('--timeout',type=float,default=30)
    args=parser.parse_args()
    try:
        print(json.dumps(run_cross(descriptors(json.loads(args.manifest.read_text())),args.timeout)));return 0
    except Exception as exc:
        print(json.dumps({'status':'fail','diagnostics':redact(str(exc),os.environ.get('NEUTRON_TEST_DATABASE_URL',''))}));return 1

if __name__=='__main__': raise SystemExit(main())
