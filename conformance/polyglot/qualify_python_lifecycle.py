"""Extend an actual qualified wheel install with native lifecycle acceptance.

Existing scalar artifacts remain unchanged. Two fresh owned schemas qualify
sync/async installed consumers, then independent coordinator SQL checks final
rows. No pytest, editable installs, automatic dependency resolution or skips.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parent))
from protocol import verify_artifacts

CASES=['graph-generated-identity-rollback','prewrite-hooks-bound-values-known-commit',
    'graph-selectin-identity-reuse','aftercommit-failure-retains-native-commit',
    'late-graph-constraint-full-rollback','early-stream-close-and-escaped-lifetime',
    'manual-transaction-terminal-and-raw-control-fence']

def run(args):
    root=args.consumer.resolve()
    if root.is_relative_to(Path(__file__).resolve().parents[2]): raise ValueError('outside-origin installed consumer required')
    if not re.fullmatch('[0-9a-f]{40}',args.source_revision): raise ValueError('exact revision required')
    original=json.loads((root/'artifacts.json').read_text())
    verify_artifacts(original,root)
    directory=root/'lifecycle-qualification';directory.mkdir(exist_ok=False)
    adapter=directory/'installed_python_lifecycle.py'
    adapter.write_bytes(Path(__file__).with_name('installed_python_lifecycle.py').read_bytes())
    revision=directory/'source-revision';revision.write_text(args.source_revision+'\n')
    manifest=directory/'artifacts.json'
    files=original['files']+[{'path':str(path.relative_to(root)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()} for path in (adapter,revision)]
    manifest.write_text(json.dumps({'files':files},indent=2)+'\n')
    prefix=root/'client-env'
    report={'protocol':'polyglot-installed-lifecycle-v1','source_revision':args.source_revision,
        'scope':'installed Python sync/async scalar hooks, explicit insert graphs, stream and transaction lifetime',
        'limitations':'No universal ORM parity, cascade/implicit graph, savepoint/merge/competitor certification',
        'status':'fail','modes':{}}
    report_path=directory/'result.json'
    import psycopg
    from psycopg import sql
    with psycopg.connect(os.environ['NEUTRON_TEST_DATABASE_URL'],autocommit=True) as native:
        version=native.execute('SELECT version()').fetchone()[0]
        if not version.startswith('PostgreSQL '): raise ValueError('native PostgreSQL required')
        report['postgres']=version
        for mode in ('sync','async'):
            hashes=verify_artifacts(json.loads(manifest.read_text()),root)
            scope='neutron_polyglot_'+uuid.uuid4().hex;token=uuid.uuid4().hex+uuid.uuid4().hex
            request={'protocol':report['protocol'],'schema_scope':scope,'ownership_token':token,'artifact_hashes':hashes}
            created=False
            try:
                native.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(scope)))
                native.execute(sql.SQL('COMMENT ON SCHEMA {} IS {}').format(sql.Identifier(scope),sql.Literal(report['protocol']+':'+token)))
                created=True
                command=[str(prefix/'bin/python'),'-I','-B',str(adapter),'--prefix',str(prefix),'--mode',mode]
                env={k:v for k,v in os.environ.items() if k not in ('PYTHONHOME','PYTHONPATH','NODE_OPTIONS','NODE_PATH')}
                process=subprocess.Popen(command,cwd=root,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
                try: output,_=process.communicate(json.dumps(request),timeout=120)
                except BaseException:
                    try: os.killpg(process.pid,signal.SIGKILL)
                    except ProcessLookupError: pass
                    process.wait();raise
                if process.returncode: raise ValueError('installed lifecycle consumer failed (native diagnostics suppressed)')
                result=json.loads(output)
                if any(result.get(key)!=value for key,value in request.items()) or result.get('status')!='pass' or result.get('mode')!=mode or result.get('cases')!=CASES:
                    raise ValueError('installed lifecycle result identity/corpus differs')
                # This final oracle executes in the coordinator's separate native
                # connection, without importing the installed ORM or its codecs.
                final=native.execute(sql.SQL('SELECT p.name,c.label FROM {}.parents p JOIN {}.children c ON c.parent_id=p.id ORDER BY c.label').format(sql.Identifier(scope),sql.Identifier(scope))).fetchall()
                if final!=[('committed-after-callback-failure','a'),('committed-after-callback-failure','b')]: raise ValueError('coordinator native lifecycle oracle differs')
                verify_artifacts(json.loads(manifest.read_text()),root)
                report['modes'][mode]={'result':result,'command':command,'coordinator_native_oracle':'pass'}
                report_path.write_text(json.dumps(report,indent=2)+'\n')
            finally:
                if created:
                    marker=native.execute('SELECT pg_catalog.obj_description(oid,%s) FROM pg_catalog.pg_namespace WHERE nspname=%s',('pg_namespace',scope)).fetchone()
                    if marker!=(report['protocol']+':'+token,): raise ValueError('cleanup ownership mismatch')
                    native.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(scope)))
        report['status']='pass'
    report_path.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'status':'pass','result':str(report_path),'artifact_manifest':str(manifest),'scope':report['scope']}))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--consumer',type=Path,required=True);parser.add_argument('--source-revision',required=True)
    try: run(parser.parse_args())
    except Exception:
        print(json.dumps({'status':'fail','diagnostics':'installed Python lifecycle qualification failed'}));raise SystemExit(1)
