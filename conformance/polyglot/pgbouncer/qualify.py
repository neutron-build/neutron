"""Pinned loopback PgBouncer session/transaction native qualification.

Requires an isolated PostgreSQL administrator endpoint and five fresh installed
performance consumers. Creates/drops only marked schema and unique runtime role;
does not install software, alter existing roles/configuration, or certify cloud TLS.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from protocol import verify_artifacts
spec=importlib.util.spec_from_file_location('performance',ROOT/'performance/runner.py')
performance=importlib.util.module_from_spec(spec);spec.loader.exec_module(performance)

def port():
    with socket.socket() as channel:
        channel.bind(('127.0.0.1',0));return channel.getsockname()[1]

def private(path,value):
    path.write_text(value);path.chmod(0o600)

def qualify(args):
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict,make_conninfo
    binary=args.binary.resolve()
    if not re.fullmatch('[0-9a-f]{64}',args.binary_sha256) or hashlib.sha256(binary.read_bytes()).hexdigest()!=args.binary_sha256:
        raise ValueError('pinned binary hash differs')
    # Exact version output is retained; expected version must be pinned before
    # dispatch, rather than inferred as proof from whichever binary happens to run.
    tool_env=dict(os.environ)
    library_hashes={}
    if args.library_path:
        tool_env['LD_LIBRARY_PATH']=str(args.library_path.resolve())
        library_hashes={str(path.resolve()):hashlib.sha256(path.read_bytes()).hexdigest() for path in args.library_path.rglob('*') if path.is_file() and '.so' in path.name}
    version=subprocess.run([str(binary),'--version'],env=tool_env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=10)
    if version.returncode or not version.stdout.strip().splitlines() or version.stdout.strip().splitlines()[0]!=args.version: raise ValueError('pinned PgBouncer version differs')
    descriptor=json.loads(args.consumers.read_text())
    profile=json.loads((ROOT/'performance/profile.json').read_text())
    if set(descriptor['clients'])!=set(profile['clients']): raise ValueError('five qualified consumers required')
    artifacts={label:verify_artifacts(json.loads(Path(client['artifact_manifest']).read_text()),Path(client['artifact_root'])) for label,client in descriptor['clients'].items()}
    admin_url=os.environ['NEUTRON_TEST_DATABASE_URL']
    direct=conninfo_to_dict(admin_url)
    host=direct.get('host','')
    if host not in ('127.0.0.1','localhost','::1'): raise ValueError('this profile requires isolated loopback PostgreSQL')
    database=direct.get('dbname','')
    # INI values are not SQL quoting contexts. Refuse ambiguous injected lines,
    # spaces, quotes or multi-host routing instead of weakening configuration.
    if not re.fullmatch('[A-Za-z0-9_.-]+',database): raise ValueError('simple isolated database name required')
    backend_port=int(direct.get('port','5432'))
    if not 1<=backend_port<=65535: raise ValueError('valid direct PostgreSQL port required')
    scope='neutron_polyglot_'+uuid.uuid4().hex
    token=uuid.uuid4().hex+uuid.uuid4().hex
    role='neutron_pool_'+uuid.uuid4().hex
    password=uuid.uuid4().hex+uuid.uuid4().hex
    console='neutron_console_'+uuid.uuid4().hex
    console_password=uuid.uuid4().hex+uuid.uuid4().hex
    work=Path(tempfile.mkdtemp(prefix='neutron-pgbouncer-'));work.chmod(0o700)
    report={'protocol':'polyglot-pgbouncer-v1','status':'fail','binary_sha256':args.binary_sha256,
        'version':args.version,'native_toolchain':version.stdout.strip(),'local_library_hashes':library_hashes,'source_revision':descriptor['source_revision'],'artifact_hashes':artifacts,
        'configuration':{'auth':'plain on loopback only','default_pool_size':1,'prepare':'disabled in all five clients',
            'max_prepared_statements':0,'server_reset_query':'DISCARD ALL','server_reset_query_always':1},
        'scope':'pinned local session/transaction pooling with explicit unprepared clients; direct native migration DDL/advisory lock',
        'limitations':'No provider/cloud TLS, serverless authentication, prepared-cache support or pooled migrations certification',
        'modes':{}}
    report_path=args.output
    def save(): report_path.parent.mkdir(parents=True,exist_ok=True);report_path.write_text(json.dumps(report,indent=2)+'\n')
    userlist=work/'users.txt'
    private(userlist,f'"{role}" "{password}"\n"{console}" "{console_password}"\n')
    created_role=created_schema=False
    with psycopg.connect(admin_url,autocommit=True) as native:
        report['postgres']=native.execute('SELECT version()').fetchone()[0]
        if not report['postgres'].startswith('PostgreSQL '): raise ValueError('native PostgreSQL required')
        try:
            native.execute(sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD {}').format(sql.Identifier(role),sql.Literal(password)));created_role=True
            native.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(scope)))
            native.execute(sql.SQL('COMMENT ON SCHEMA {} IS {}').format(sql.Identifier(scope),sql.Literal(performance.MARKER+token)));created_schema=True
            native.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(sql.Identifier(database),sql.Identifier(role)))
            native.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(scope),sql.Identifier(role)))
            native.execute(sql.SQL('CREATE TABLE {}.perf_fixture(id integer PRIMARY KEY,big bigint NOT NULL,body text NOT NULL)').format(sql.Identifier(scope)))
            native.execute(sql.SQL("INSERT INTO {}.perf_fixture SELECT i,9007199254740993::bigint+i,'base:'||i FROM generate_series(1,64)i").format(sql.Identifier(scope)))
            native.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON {}.perf_fixture TO {}').format(sql.Identifier(scope),sql.Identifier(role)))
            for mode in ('session','transaction'):
                listen_port=port()
                if listen_port==backend_port: raise ValueError('proxy and direct migration endpoint overlap')
                config=work/(mode+'.ini')
                private(config,f'''[databases]
fixture = host={host} port={backend_port} dbname={database}
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = {listen_port}
unix_socket_dir =
auth_type = plain
auth_file = {userlist}
admin_users = {console}
pool_mode = {mode}
default_pool_size = 1
min_pool_size = 0
reserve_pool_size = 0
max_client_conn = 30
max_prepared_statements = 0
server_reset_query = DISCARD ALL
server_reset_query_always = 1
server_tls_sslmode = disable
log_connections = 0
log_disconnections = 0
log_pooler_errors = 0
pidfile = {work/(mode+'.pid')}
''')
                # All consumers accept a PostgreSQL URI; node drivers do not
                # accept psycopg's libpq key=value connection string. Role and
                # password above are generated hex identifiers, never input.
                proxy_url=f'postgresql://{role}:{password}@127.0.0.1:{listen_port}/fixture?connect_timeout=2&sslmode=disable'
                child_env={key:value for key,value in os.environ.items() if not key.startswith('PG') and not key.endswith(('DATABASE_URL','DB_URL'))}
                if args.library_path: child_env['LD_LIBRARY_PATH']=str(args.library_path.resolve())
                with open(work/(mode+'.private.log'),'wb') as log:
                    os.chmod(log.name,0o600)
                    process=subprocess.Popen([str(binary),str(config)],env=child_env,stdout=log,stderr=log,start_new_session=True)
                    try:
                        deadline=time.monotonic()+12
                        while True:
                            if process.poll() is not None: raise ValueError('owned PgBouncer startup failed')
                            try:
                                with psycopg.connect(proxy_url,autocommit=True,prepare_threshold=None) as probe: probe.execute('SELECT 1')
                                break
                            except psycopg.Error:
                                if time.monotonic()>=deadline: raise ValueError('owned PgBouncer readiness timed out')
                                time.sleep(0.1)
                        checks=report['modes'][mode]={'clients':{},'cases':[],'config_sha256':hashlib.sha256(config.read_bytes()).hexdigest()}
                        # Native persistent prepared SQL is supported only while
                        # a session remains attached. Transaction mode explicitly
                        # resets it rather than claiming statement-cache support.
                        with psycopg.connect(proxy_url,autocommit=True,prepare_threshold=None) as client:
                            client.execute('PREPARE owned_statement AS SELECT 7')
                            if mode=='session':
                                if client.execute('EXECUTE owned_statement').fetchone()!=(7,): raise ValueError('session prepared SQL lost during attachment')
                            else:
                                try: client.execute('EXECUTE owned_statement')
                                except psycopg.Error as error:
                                    if error.sqlstate!='26000': raise
                                else: raise ValueError('transaction prepared session state leaked')
                        with psycopg.connect(proxy_url,autocommit=True,prepare_threshold=None) as client:
                            try: client.execute('EXECUTE owned_statement')
                            except psycopg.Error as error:
                                if error.sqlstate!='26000': raise
                            else: raise ValueError('prepared state leaked to next borrower')
                        checks['cases'].append('pinned-unprepared-policy-and-prepared-state-reset')
                        with psycopg.connect(proxy_url,autocommit=True,prepare_threshold=None) as client:
                            client.execute("SET app.tenant='owned-tenant'")
                            if mode=='session' and client.execute("SELECT current_setting('app.tenant')").fetchone()!=('owned-tenant',): raise ValueError('session state not retained')
                            if mode=='transaction':
                                value=client.execute("SELECT current_setting('app.tenant',true)").fetchone()[0]
                                if value not in (None,''): raise ValueError('transaction state leaked')
                            try: client.execute(sql.SQL('CREATE TABLE {}.denied(id integer)').format(sql.Identifier(scope)))
                            except psycopg.Error as error:
                                if error.sqlstate!='42501': raise
                            else: raise ValueError('runtime role unexpectedly acquired schema DDL authority')
                        with psycopg.connect(proxy_url,autocommit=True,prepare_threshold=None) as client:
                            if client.execute("SELECT current_setting('app.tenant',true)").fetchone()[0] not in (None,''): raise ValueError('tenant state leaked to new borrower')
                        checks['cases'].append('least-privilege-and-tenant-reset')
                        # Two statements inside one explicit transaction retain
                        # one server even in transaction pooling mode.
                        with psycopg.connect(proxy_url,autocommit=True,prepare_threshold=None) as client:
                            with client.transaction():
                                first=client.execute('SELECT pg_backend_pid()').fetchone()[0]
                                second=client.execute('SELECT pg_backend_pid()').fetchone()[0]
                                if first!=second: raise ValueError('transaction was not server pinned')
                        checks['cases'].append('native-transaction-server-pinning')
                        with psycopg.connect(proxy_url,autocommit=True,prepare_threshold=None) as client:
                            errors=[];needle='owned_cancel_'+uuid.uuid4().hex
                            def sleeping():
                                try: client.execute('SELECT pg_sleep(20) /* '+needle+' */')
                                except BaseException as error: errors.append(getattr(error,'sqlstate',None))
                            thread=threading.Thread(target=sleeping,daemon=True);thread.start()
                            deadline=time.monotonic()+5;pid=None
                            while time.monotonic()<deadline:
                                hit=native.execute("SELECT pid FROM pg_stat_activity WHERE usename=%s AND state='active' AND query LIKE %s",(role,'%'+needle+'%')).fetchone()
                                if hit: pid=hit[0];break
                                time.sleep(0.05)
                            try:
                                if pid is None: raise ValueError('owned cancellation target did not dispatch')
                                client.cancel_safe(timeout=3)
                                thread.join(5)
                                if thread.is_alive() or errors!=['57014']: raise ValueError('native pool cancellation failed')
                                if client.execute('SELECT 1').fetchone()!=(1,): raise ValueError('canceled connection did not recover')
                            finally:
                                if thread.is_alive() and pid is not None:
                                    native.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=%s AND usename=%s AND query LIKE %s',(pid,role,'%'+needle+'%'));thread.join(5)
                        checks['cases'].append('native-cancel-and-safe-borrower-reuse')
                        try:
                            os.environ['NEUTRON_TEST_DATABASE_URL']=proxy_url
                            for label,consumer in descriptor['clients'].items():
                                results=[]
                                for workload in profile['workloads']:
                                    for implementation in ('raw','orm'):
                                        results.append(performance.sample(native,scope,token,consumer,artifacts[label],profile,implementation,workload))
                                checks['clients'][label]=results;save()
                        finally: os.environ['NEUTRON_TEST_DATABASE_URL']=admin_url
                        # Migration authority stays on the direct admin channel;
                        # application pooling/prepare policy is not used for DDL.
                        with native.transaction():
                            lock=int(token[:15],16)
                            native.execute('SELECT pg_advisory_xact_lock(%s)',(lock,))
                            native.execute(sql.SQL('CREATE TABLE {}.direct_migration(id integer PRIMARY KEY)').format(sql.Identifier(scope)))
                            native.execute(sql.SQL('INSERT INTO {}.direct_migration VALUES(1)').format(sql.Identifier(scope)))
                        if native.execute(sql.SQL('SELECT id FROM {}.direct_migration').format(sql.Identifier(scope))).fetchall()!=[(1,)]: raise ValueError('direct migration authority oracle differs')
                        native.execute(sql.SQL('DROP TABLE {}.direct_migration').format(sql.Identifier(scope)))
                        checks['cases'].append('separate-direct-native-ddl-and-advisory-lock')
                    finally:
                        if process.poll() is None:
                            os.killpg(process.pid,signal.SIGTERM)
                            try: process.wait(timeout=8)
                            except subprocess.TimeoutExpired: os.killpg(process.pid,signal.SIGKILL);process.wait()
                        save()
            for label,consumer in descriptor['clients'].items():
                if verify_artifacts(json.loads(Path(consumer['artifact_manifest']).read_text()),Path(consumer['artifact_root']))!=artifacts[label]: raise ValueError('installed artifacts changed')
        finally:
            if created_schema:
                marker=native.execute('SELECT pg_catalog.obj_description(oid,%s) FROM pg_catalog.pg_namespace WHERE nspname=%s',('pg_namespace',scope)).fetchone()
                if marker!=(performance.MARKER+token,): raise ValueError('owned schema cleanup refused')
                native.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(scope)))
            if created_role:
                # This unique role belongs to this fixture; terminate only its
                # native connections before revoking its owned temporary grants.
                native.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename=%s AND pid<>pg_backend_pid()',(role,))
                native.execute(sql.SQL('REVOKE CONNECT ON DATABASE {} FROM {}').format(sql.Identifier(database),sql.Identifier(role)))
                native.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))
            save()
            # Userlist/config contain credentials. Leave only redacted evidence.
            for path in work.iterdir(): path.unlink(missing_ok=True)
            work.rmdir()
    report['status']='pass';save()
    print(json.dumps({'status':report['status'],'result':str(report_path),'scope':report['scope']}))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,required=True);parser.add_argument('--binary-sha256',required=True);parser.add_argument('--version',required=True);parser.add_argument('--library-path',type=Path);parser.add_argument('--consumers',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    try: qualify(parser.parse_args())
    except Exception:
        print(json.dumps({'status':'fail','diagnostics':'pinned PgBouncer qualification failed; native diagnostics suppressed'}));raise SystemExit(1)
