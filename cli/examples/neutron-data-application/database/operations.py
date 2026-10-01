#!/usr/bin/env python3
"""Owned disposable PostgreSQL17 expand/restore drill; never a production backup command."""
import argparse
import asyncio
import base64
from datetime import date, datetime
from decimal import Decimal
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import unquote, urlsplit, urlunsplit
import urllib.error
import urllib.request
from uuid import UUID, uuid4

import asyncpg

HERE = Path(__file__).resolve().parent
TABLES = ('app_schema_revision', 'projects', 'documents', 'processing_requests', 'jobs', 'results', '_neutron_migrations')


def require(value, message):
    if not value:
        raise RuntimeError(message)


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def wire(value):
    if isinstance(value, (Decimal, datetime, date, UUID)):
        return str(value)
    if isinstance(value, bytes):
        return {'bytes_base64': base64.b64encode(value).decode()}
    raise TypeError(type(value).__name__)


def stable(value):
    return json.dumps(value, sort_keys=True, default=wire, ensure_ascii=False)


def private_json(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(json.dumps(value, indent=2) + '\n')


def token(secret, tenant):
    encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b'=')
    header = encode(b'{"alg":"HS256","typ":"JWT"}')
    claims = encode(json.dumps({'iss': 'neutron-data-reference', 'aud': 'neutron-data-reference-api',
                               'sub': 'operations-drill', 'tenant_id': tenant,
                               'exp': int(time.time()) + 600}, separators=(',', ':')).encode())
    payload = header + b'.' + claims
    return (payload + b'.' + encode(hmac.new(secret.encode(), payload, hashlib.sha256).digest())).decode()


async def command(argv, env, *, data=None, timeout=120):
    # Captured native diagnostics can include credentials; expose safe exit/category only.
    return await asyncio.to_thread(subprocess.run, argv, env=env, input=data,
                                   capture_output=True, timeout=timeout)


async def snapshot(db, name, roles):
    rows = {}
    for table in TABLES:
        values = [dict(row) for row in await db.fetch('SELECT * FROM public.' + ident(table))]
        rows[table] = sorted(values, key=stable)
    tables = [dict(row) for row in await db.fetch("""
        SELECT c.relname,pg_catalog.pg_get_userbyid(c.relowner) AS owner,
               c.relrowsecurity,c.relforcerowsecurity
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind='r' ORDER BY c.relname""")]
    columns = [dict(row) for row in await db.fetch("""
        SELECT c.relname,a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod) AS type,
               a.attnotnull,pg_catalog.pg_get_expr(d.adbin,d.adrelid) AS default_value
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
        WHERE n.nspname='public' AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped
        ORDER BY c.relname,a.attnum""")]
    policies = [dict(row) for row in await db.fetch("SELECT tablename,policyname,permissive,roles,cmd,qual,with_check FROM pg_catalog.pg_policies WHERE schemaname='public' ORDER BY tablename,policyname")]
    grants = [dict(row) for row in await db.fetch("SELECT table_name,grantee,privilege_type,is_grantable FROM information_schema.role_table_grants WHERE table_schema='public' ORDER BY table_name,grantee,privilege_type")]
    database = dict(await db.fetchrow("SELECT pg_catalog.pg_get_userbyid(datdba) AS owner,datacl::pg_catalog.text AS acl FROM pg_catalog.pg_database WHERE datname=$1", name))
    schema = dict(await db.fetchrow("SELECT pg_catalog.pg_get_userbyid(nspowner) AS owner,nspacl::pg_catalog.text AS acl FROM pg_catalog.pg_namespace WHERE nspname='public'"))
    identities = [dict(row) for row in await db.fetch("SELECT rolname,rolsuper,rolbypassrls,rolcreatedb,rolcreaterole,rolinherit,rolcanlogin FROM pg_catalog.pg_roles WHERE rolname=ANY($1::pg_catalog.text[]) ORDER BY rolname", roles)]
    return {'rows': rows, 'tables': tables, 'columns': columns, 'policies': policies,
            'grants': grants, 'database': database, 'schema': schema, 'identities': identities}


class API:
    def __init__(self, args, manifest, env, directory):
        self.args, self.manifest, self.env, self.directory = args, manifest, env, directory
        self.secret = secrets.token_urlsafe(32)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        self.process = None
        self.log = None

    async def start(self, admitted=True):
        roles = self.manifest['roles']
        name = self.manifest['database']
        mapping = {tenant: {'url': roles[name + '_api_' + tenant[-1]], 'role': name + '_api_' + tenant[-1]}
                   for tenant in ('tenant-a', 'tenant-b')}
        self.log = open(self.directory / 'api.stdout.local.log', 'ab')
        self.process = subprocess.Popen([str(self.args.api_bin.resolve())], stdout=self.log, stderr=self.log,
            env=dict(self.env, DATA_API_TENANTS=json.dumps(mapping), DATA_API_JWT_SECRET=self.secret,
                     DATA_API_ERROR_LOG=str(self.directory / 'api.errors.local.log'),
                     NEUTRON_HOST='127.0.0.1', NEUTRON_PORT=str(self.port)))
        if not admitted:
            code = await asyncio.to_thread(self.process.wait, 15)
            require(code != 0, 'incompatible revision was admitted by API')
            require('schema revision refused' in (self.directory / 'api.errors.local.log').read_text(),
                    'API failed without the expected schema-revision refusal')
            return
        for _ in range(150):
            require(self.process.poll() is None, 'API startup refused unexpectedly')
            try:
                await self.request('/health', authenticated=False)
                return
            except (OSError, urllib.error.URLError):
                await asyncio.sleep(.1)
        raise RuntimeError('API readiness timed out')

    async def stop(self):
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            await asyncio.to_thread(self.process.wait, 15)
        if self.log:
            self.log.close()
        self.process = self.log = None

    async def request(self, path, tenant='tenant-a', body=None, authenticated=True):
        headers = {'Content-Type': 'application/json'}
        if authenticated:
            headers['Authorization'] = 'Bearer ' + token(self.secret, tenant)
        request = urllib.request.Request('http://127.0.0.1:' + str(self.port) + path,
                    data=json.dumps(body).encode() if body is not None else None, headers=headers)
        def fetch():
            with urllib.request.urlopen(request, timeout=10) as response:
                return json.load(response)
        return await asyncio.to_thread(fetch)


async def run(args):
    admin_url = os.environ['ADMIN_DATABASE_URL']
    args.out.mkdir(parents=True, exist_ok=True, mode=0o700)
    name = 'v10_data_ops_' + uuid4().hex[:12]
    directory = Path(tempfile.mkdtemp(prefix=name + '_', dir=args.out))
    credentials = directory / 'credentials.local.json'
    archive = directory / 'database.dump'
    env = dict(os.environ)
    if args.python_sdk:
        env['PYTHONPATH'] = str(args.python_sdk.resolve())
    manifest = None
    admin = db = api = None
    checks = []
    def passed(label):
        checks.append(label)
        print('PASS ' + label, flush=True)
    try:
        result = await command([sys.executable, str(HERE / 'provision.py'), '--database', name,
                                '--cli', str(args.cli.resolve()), '--out', str(credentials)], env)
        require(result.returncode == 0, 'fresh provision failed; native diagnostics withheld')
        manifest = json.loads(credentials.read_text())
        admin = await asyncpg.connect(admin_url)
        database_url = urlunsplit(urlsplit(admin_url)._replace(path='/' + name))
        db = await asyncpg.connect(database_url)
        require((await db.fetchval("SELECT current_setting('server_version_num')::integer")) // 10000 == 17,
                'PostgreSQL17 required')
        owner = name + '_owner'
        require(await admin.fetchval('SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1', name) == owner,
                'fixture ownership mismatch')
        worker_env = dict(env, WORKER_DATABASE_URL=manifest['roles'][name + '_worker_a'], WORKER_TENANT='tenant-a')
        async def worker(tenant='tenant-a', admitted=True):
            result = await command([sys.executable, str(args.worker_script.resolve()), '--once'],
                dict(worker_env, WORKER_TENANT=tenant, WORKER_DATABASE_URL=manifest['roles'][name + '_worker_' + tenant[-1]]), timeout=30)
            require((result.returncode == 0) == admitted, 'worker admission/execution disagreed with expected profile')
            payload = json.loads(result.stdout)
            if not admitted:
                require(payload.get('failed') is True and payload.get('category') == 'ValueError',
                        'worker failed without the expected admission category')
                return None
            return payload
        api = API(args, manifest, env, directory)
        await api.start()
        project_id, document_id = str(uuid4()), str(uuid4())
        fixtures = {}
        for tenant in ('tenant-a', 'tenant-b'):
            await api.request('/api/projects', tenant, {'id': project_id, 'title': tenant})
            body = {'id': document_id, 'project_id': project_id, 'content': 'Exact β content ' + tenant,
                    'amount': '9999999999999999999999.123456789012345678' if tenant == 'tenant-a' else '-1.2',
                    'note': None if tenant == 'tenant-a' else '', 'payload': base64.b64encode(bytes(range(256))).decode(),
                    'idempotency_key': 'operations-' + tenant}
            fixtures[tenant] = await api.request('/api/documents', tenant, body)
            require((await worker(tenant))['outcome'] == 'done', 'actual worker did not process document')
        pending_id = str(uuid4())
        await api.request('/api/documents', body={**body, 'id': pending_id, 'content': 'Restored pending job β',
                                   'idempotency_key': 'operations-pending', 'amount': '0.000000000000000001', 'note': None})
        # Pin independently observed exact boundary values, then let the actual API read them.
        await db.execute("UPDATE public.documents SET version=9007199254740993,created_at='2026-09-30T22:34:56.123456Z'::timestamptz WHERE id=$1", UUID(document_id))
        before_detail = {tenant: await api.request('/api/documents/' + document_id, tenant) for tenant in fixtures}
        require(all(detail['tenant_id'] == tenant and detail['content'].endswith(tenant) for tenant, detail in before_detail.items()), 'actual API tenant identity differs')
        require(before_detail['tenant-a']['version'] == '9007199254740993' and before_detail['tenant-a']['created_at'] == '2026-09-30T22:34:56.123456Z', 'exact API version/timestamp lost')
        passed('original revision1 actual API and workers: exact values, both tenants, durable results')
        # Additive fixture only; no pretend new binary/schema release is introduced.
        await db.execute('ALTER TABLE public.documents ADD COLUMN operator_annotation pg_catalog.text')
        await api.stop()
        await api.start()
        require(await api.request('/api/documents/' + document_id) == before_detail['tenant-a'], 'old consumer failed additive expansion')
        require((await worker())['outcome'] == 'done', 'old worker failed additive expansion')
        passed('revision1 additive optional column remains compatible with existing API/worker')
        await api.stop()
        await db.execute('UPDATE public.app_schema_revision SET revision=2')
        unchanged = await snapshot(db, name, list(manifest['roles']) + [owner])
        await api.start(admitted=False)
        await api.stop()
        await worker(admitted=False)
        require(stable(await snapshot(db, name, list(manifest['roles']) + [owner])) == stable(unchanged), 'refused consumers mutated revision2 fixture')
        await db.execute('UPDATE public.app_schema_revision SET revision=1')
        await db.execute('ALTER TABLE public.documents DROP COLUMN operator_annotation')
        passed('revision2 explicitly refuses old API/worker without row changes; fixture marker reverted')
        # Leave an actual durable pending job for processing after restoration.
        pending_id = str(uuid4())
        await api.start()
        await api.request('/api/documents', body={**body, 'id': pending_id, 'content': 'Backup pending β work',
                          'idempotency_key': 'operations-restore-pending', 'amount': '0.000000000000000001', 'note': None})
        await api.stop()
        expected = await snapshot(db, name, list(manifest['roles']) + [owner])
        history = expected['rows']['_neutron_migrations']
        sql = (HERE / 'migrations/001_data.up.sql').read_bytes()
        require(len(history) == 1 and history[0]['checksum'] == hashlib.sha256(sql).hexdigest()
                and history[0]['owner'] == 'neutron-cli' and history[0]['format'] == 'v2', 'CLI history is not verified v2')
        parts = urlsplit(admin_url)
        tool_env = dict(env, PGPASSWORD=unquote(parts.password or ''))
        docker = ['docker', '--context', args.docker_context, 'exec', '-i', '--env', 'PGPASSWORD', args.container]
        connection = ['--host', '127.0.0.1', '--port', str(args.container_port), '--username', unquote(parts.username or 'postgres')]
        version = await command(docker + ['pg_dump', '--version'], tool_env)
        require(version.returncode == 0 and b' 17.' in version.stdout, 'container PostgreSQL17 tools required')
        cluster_id = str(await db.fetchval('SELECT system_identifier FROM pg_catalog.pg_control_system()'))
        container_identity = await command(docker + ['psql', *connection, '--dbname', parts.path.lstrip('/') or 'postgres',
                                         '--tuples-only', '--no-align', '--command',
                                         'SELECT system_identifier FROM pg_catalog.pg_control_system()'], tool_env)
        require(container_identity.returncode == 0 and container_identity.stdout.decode().strip() == cluster_id,
                'container backup tools point at a different PostgreSQL cluster')
        dump = await command(docker + ['pg_dump', *connection, '--format=custom', '--create', '--dbname', name], tool_env)
        require(dump.returncode == 0, 'pg_dump failed; diagnostics withheld')
        fd = os.open(archive, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(dump.stdout)
        require(archive.stat().st_size < 32 * 1024 * 1024, 'bounded fixture archive exceeded32MiB')
        archive_hash = hashlib.sha256(dump.stdout).hexdigest()
        await db.close()
        db = None
        require(await admin.fetchval('SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1', name) == owner, 'drop ownership changed')
        await admin.execute('DROP DATABASE ' + ident(name) + ' WITH (FORCE)')
        restore = await command(docker + ['pg_restore', *connection, '--exit-on-error', '--create', '--dbname', parts.path.lstrip('/') or 'postgres'], tool_env, data=dump.stdout)
        require(restore.returncode == 0, 'pg_restore failed; diagnostics withheld')
        db = await asyncpg.connect(database_url)
        observed = await snapshot(db, name, list(manifest['roles']) + [owner])
        require(stable(observed) == stable(expected), 'native restore catalog/ACL/RLS/history/exact data differ')
        passed('native PG17 custom dump/create restore preserves exact rows, owners, ACLs, policies, forced RLS and CLI history')
        noop = await command([str(args.cli.resolve()), 'migrate', '--dir', str(HERE / 'migrations')],
                             dict(env, DATABASE_URL=manifest['migration_owner_url']))
        require(noop.returncode == 0 and stable(await snapshot(db, name, list(manifest['roles']) + [owner])) == stable(expected), 'restored CLI migration was not a verified no-op')
        drift = directory / 'drift'
        shutil.copytree(HERE / 'migrations', drift)
        with (drift / '001_data.up.sql').open('a') as stream:
            stream.write('\n-- deliberate operator drill drift\n')
        refused = await command([str(args.cli.resolve()), 'migrate', '--dir', str(drift)], dict(env, DATABASE_URL=manifest['migration_owner_url']))
        require(refused.returncode != 0 and stable(await snapshot(db, name, list(manifest['roles']) + [owner])) == stable(expected), 'restored checksum drift did not refuse without effects')
        passed('restored CLI v2 checksum no-op and changed-source refusal without effects')
        for kind in ('api', 'worker', 'studio'):
            for tenant in ('tenant-a', 'tenant-b'):
                role = name + '_' + kind + '_' + tenant[-1]
                runtime = await asyncpg.connect(manifest['roles'][role])
                try:
                    rows = await runtime.fetch('SELECT tenant_id,id,amount::text,note,payload,version,created_at FROM public.documents')
                    require(rows and all(row['tenant_id'] == tenant for row in rows), 'restored role bypasses tenant RLS')
                    own = [row for row in rows if row['id'] == UUID(document_id)]
                    require(len(own) == 1 and own[0]['payload'] == bytes(range(256)) and own[0]['version'] == 9007199254740993, 'restored native values lost')
                    if kind == 'studio':
                        try:
                            await runtime.execute("UPDATE public.documents SET note='denied'")
                        except asyncpg.InsufficientPrivilegeError:
                            pass
                        else:
                            raise RuntimeError('restored Studio gained write privileges')
                finally:
                    await runtime.close()
        await api.start()
        for tenant in before_detail:
            require(await api.request('/api/documents/' + document_id, tenant) == before_detail[tenant], 'restored actual API response differs')
        try:
            await api.request('/api/documents/' + pending_id, 'tenant-b')
        except urllib.error.HTTPError as error:
            require(error.code == 404, 'cross-tenant document lookup did not hide inaccessible data')
        else:
            raise RuntimeError('actual restored API leaked other tenant document')
        require((await worker())['outcome'] == 'done', 'restored actual worker did not process pending job')
        detail = await api.request('/api/documents/' + pending_id)
        require(detail['result']['content_digest'] == hashlib.sha256('Backup pending β work'.encode()).hexdigest()
                and detail['result']['word_count'] == 4 and detail['job']['status'] == 'done', 'restored worker result differs')
        passed('restored API/worker/Studio identities, tenant isolation, exact reads and pending-job processing')
        python_identity = await command([sys.executable, '-c', 'import neutron; print(neutron.__file__)'], env)
        require(python_identity.returncode == 0, 'Python package identity unavailable')
        report = {'same_cluster_verified': True, 'python_package': python_identity.stdout.decode().strip(), 'python_executable': sys.executable, 'pg_dump_version': version.stdout.decode().strip(), 'result': 'PASS', 'checks': checks, 'archive_sha256': archive_hash,
                  'archive_bytes': archive.stat().st_size, 'database': name,
                  'identities': {key: {'path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
                      for key, path in [('operations_source', Path(__file__)), ('api_binary', args.api_bin), ('worker_source', args.worker_script), ('cli_binary', args.cli), ('up_sql', HERE / 'migrations/001_data.up.sql')]},
                  'limits': ['same-cluster roles retained; database archive does not back up cluster roles/passwords',
                             'existing revision1 binaries only; no artificial new consumer or rolling-upgrade guarantee',
                             'fixture-only drill; production quiescence, global-role recovery and storage retention are operator tasks']}
        private_json(args.out / ('report-' + name + '.json'), report)
    finally:
        if api:
            await api.stop()
        if db:
            await db.close()
        if manifest:
            if admin is None:
                admin = await asyncpg.connect(admin_url)
            # Only names created successfully by this invocation; never user-supplied targets.
            actual = await admin.fetchval('SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1', name)
            if actual is not None:
                require(actual in (name + '_owner', unquote(urlsplit(admin_url).username or 'postgres')), 'cleanup owner mismatch')
                await admin.execute('DROP DATABASE ' + ident(name) + ' WITH (FORCE)')
            for role in list(manifest['roles']) + [name + '_owner']:
                await admin.execute('DROP ROLE IF EXISTS ' + ident(role))
        if admin:
            await admin.close()
        # Fixture archives contain data; this bounded drill deliberately does not retain them.
        shutil.rmtree(directory)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api-bin', type=Path, required=True)
    parser.add_argument('--worker-script', type=Path, required=True)
    parser.add_argument('--cli', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--python-sdk', type=Path)
    parser.add_argument('--docker-context', default='podman')
    parser.add_argument('--container', required=True)
    parser.add_argument('--container-port', type=int, default=5432)
    try:
        asyncio.run(run(parser.parse_args()))
    except Exception as error:
        print('Operations drill failed: ' + (str(error) if type(error) is RuntimeError else type(error).__name__) + ' (native diagnostics withheld)', flush=True)
        raise SystemExit(1)
