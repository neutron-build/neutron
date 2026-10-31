#!/usr/bin/env python3
"""Real old/new reference consumers across schema1 ->2 ->1, owned fixtures only."""
import argparse
import asyncio
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from urllib.parse import urlsplit, urlunsplit
import urllib.error
from uuid import uuid4

import asyncpg
from operations import API, HERE, command, ident, private_json, require, runtime_env, snapshot, stable


async def business(db):
    state = {}
    for table in ('projects', 'documents', 'processing_requests', 'jobs', 'results'):
        rows = [dict(row) for row in await db.fetch('SELECT * FROM public.' + ident(table))]
        for row in rows:
            row.pop('content_octets', None)
        state[table] = sorted(rows, key=stable)
    return stable(state)


async def history(db, revision):
    rows = await db.fetch('SELECT version,checksum,owner,format FROM public._neutron_migrations ORDER BY version')
    require(len(rows) == revision, 'migration frontier differs')
    for row in rows:
        files = list((HERE / 'migrations').glob(str(row['version']) + '_*.up.sql'))
        require(len(files) == 1 and row['checksum'] == hashlib.sha256(files[0].read_bytes()).hexdigest()
                and row['owner'] == 'neutron-cli' and row['format'] == 'v2', 'migration history is not exact verified CLI source')
    return [dict(row) for row in rows]


async def run(args):
    require(shutil.disk_usage(args.out.parent).free >= 6 * 1024 ** 3, 'disk below6GiB guard')
    args.out.mkdir(parents=True, exist_ok=True, mode=0o700)
    name = 'v10_data_roll_' + uuid4().hex[:12]
    directory = Path(tempfile.mkdtemp(prefix=name + '_', dir=args.out))
    manifest_path = directory / 'credentials.local.json'
    env = dict(os.environ)
    if args.python_sdk:
        env['PYTHONPATH'] = str(args.python_sdk.resolve())
    manifest = None
    admin = db = old = new = None
    checks = []
    artifacts = {}
    def passed(label):
        checks.append(label)
        print('PASS ' + label, flush=True)
    try:
        provision = await command([sys.executable, str(HERE / 'provision.py'), '--database', name,
                                   '--schema-revision', '1', '--cli', str(args.cli.resolve()), '--out', str(manifest_path)], env)
        require(provision.returncode == 0, 'revision1 provision refused; diagnostics withheld')
        manifest = json.loads(manifest_path.read_text())
        admin = await asyncpg.connect(os.environ['ADMIN_DATABASE_URL'])
        db = await asyncpg.connect(urlunsplit(urlsplit(os.environ['ADMIN_DATABASE_URL'])._replace(path='/' + name)))
        owner = name + '_owner'
        require(await admin.fetchval('SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1', name) == owner,
                'fixture ownership mismatch')
        roles = list(manifest['roles']) + [owner]
        async def worker(path, admitted=True):
            result = await command([sys.executable, str(path.resolve()), '--once'],
                dict(runtime_env(env), WORKER_DATABASE_URL=manifest['roles'][name + '_worker_a'], WORKER_TENANT='tenant-a'), timeout=30)
            require((result.returncode == 0) == admitted, 'worker outcome differs from profile')
            payload = json.loads(result.stdout)
            if not admitted:
                require(payload.get('failed') is True and payload.get('category') == 'ValueError', 'worker refusal is not admission category')
            return payload
        for label in ('old', 'new'):
            (directory / label).mkdir()
        old = API(argparse.Namespace(**dict(vars(args), api_bin=args.old_api)), manifest, env, directory / 'old')
        new = API(argparse.Namespace(**dict(vars(args), api_bin=args.new_api)), manifest, env, directory / 'new')
        api_roles = [name + '_api_a', name + '_api_b']
        await old.start()
        old_pids = {row['pid'] for row in await db.fetch('SELECT pid FROM pg_stat_activity WHERE datname=$1 AND usename=ANY($2::text[])', name, api_roles)}
        require(old_pids, 'old physical sessions not observed')
        await new.start()
        project = str(uuid4())
        await old.request('/api/projects', body={'id': project, 'title': 'overlap project'})
        def document(content):
            return {'id': str(uuid4()), 'project_id': project, 'content': content,
                    'amount': '9999999999999999999999.123456789012345678', 'note': None,
                    'payload': base64.b64encode(bytes(range(256))).decode(), 'idempotency_key': 'roll-' + uuid4().hex}
        first = document('Old writer β content')
        await old.request('/api/documents', body=first)
        require((await worker(args.new_worker))['outcome'] == 'done', 'new worker did not process old writer')
        second = document('New writer β content')
        await new.request('/api/documents', body=second)
        require((await worker(args.old_worker))['outcome'] == 'done', 'old worker did not process new writer')
        require((await new.request('/api/documents/' + first['id']))['content'] == first['content'], 'new reader differs')
        require((await old.request('/api/documents/' + second['id']))['content'] == second['content'], 'old reader differs')
        await old.request('/api/documents/' + first['id'] + '/note', body={'expected_version': '1', 'note': 'old CAS'})
        result = await new.request('/api/documents/' + first['id'] + '/note', body={'expected_version': '2', 'note': 'new CAS'})
        require(result['version'] == '3' and result['note'] == 'new CAS', 'overlapping CAS differs')
        require(await db.fetchval('SELECT version FROM public.documents WHERE id=$1::uuid', first['id']) == 3, 'native CAS oracle differs')
        for body in (first, second):
            record = await db.fetchrow('SELECT content_digest,word_count FROM public.results WHERE document_id=$1::uuid', body['id'])
            require(record['content_digest'] == hashlib.sha256(body['content'].encode()).hexdigest() and record['word_count'] == 4, 'cross-generation worker result differs')
        await history(db, 1)
        passed('real old/new API and workers overlap on schema1: cross-generation writes/reads/results and CAS')
        prior = await business(db)
        await old.stop()
        await new.stop()
        for _ in range(100):
            live = {row['pid'] for row in await db.fetch('SELECT pid FROM pg_stat_activity WHERE datname=$1', name)}
            if not old_pids.intersection(live):
                break
            await asyncio.sleep(.05)
        require(not old_pids.intersection(live), 'old API physical sessions remain before revision2')
        require(await db.fetchval('SELECT count(*) FROM pg_stat_activity WHERE datname=$1 AND usename=$2', name, name + '_worker_a') == 0,
                'finite old/new worker session remains before expansion')
        owner_env = dict(runtime_env(env), DATABASE_URL=manifest['migration_owner_url'])
        migration = await command([str(args.cli.resolve()), 'migrate', '--dir', str(HERE / 'migrations')], owner_env)
        require(migration.returncode == 0, 'actual002 expansion refused; diagnostics withheld')
        require(await db.fetchval('SELECT revision FROM public.app_schema_revision') == 2 and await business(db) == prior,
                'expansion changed existing business state')
        await history(db, 2)
        await db.execute('SET search_path TO pg_catalog')
        column = await db.fetchrow("""SELECT a.atttypid::int AS oid,a.attgenerated::text AS generated,
               pg_get_expr(d.adbin,d.adrelid) AS expression
               FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
               WHERE a.attrelid='public.documents'::regclass AND a.attname='content_octets'""")
        require(dict(column) == {'oid':23,'generated':'s','expression':'octet_length(content)'}, 'actual002 stored generated native definition differs')
        await new.start()
        third = document('Schema two β derived content')
        created = await new.request('/api/documents', body=third)
        require('content_octets' not in created, 'expansion unexpectedly changed API wire')
        require((await worker(args.new_worker))['outcome'] == 'done', 'new worker failed schema2')
        await new.request('/api/documents/' + third['id'] + '/note', body={'expected_version':'1','note':''})
        current = await new.request('/api/documents/' + third['id'])
        require(current['amount'] == third['amount'] and current['payload'] == third['payload'] and current['note'] == '' and current['version'] == '2', 'new schema2 CRUD exact values differ')
        try:
            await new.request('/api/documents', body={**document('rejected'), 'content_octets': 1})
        except urllib.error.HTTPError as error:
            require(error.code == 400, 'generated wire-field refusal differs')
        else:
            raise RuntimeError('generated wire-field unexpectedly accepted')
        studio = await asyncpg.connect(manifest['roles'][name + '_studio_a'])
        try:
            derived = await studio.fetch('SELECT content,content_octets FROM public.documents')
            require(len(derived) == 3 and all(row['content_octets'] == len(row['content'].encode()) for row in derived), 'native Studio role derived-byte reads differ')
        finally:
            await studio.close()
        passed('actual002 schema2 generated nativeint4 byte-count, new CRUD/worker and Studio-role reads without wire change')
        before_noop = stable(await snapshot(db, name, roles))
        noop = await command([str(args.cli.resolve()), 'migrate', '--dir', str(HERE / 'migrations')], owner_env)
        require(noop.returncode == 0 and stable(await snapshot(db, name, roles)) == before_noop, 'schema2 verified migration not a no-op')
        artifact_dir = args.out / ('artifacts-' + name)
        artifact_dir.mkdir(mode=0o700)
        studio_env = dict(runtime_env(env), DATABASE_URL=manifest['roles'][name + '_studio_a'])
        for lang in ('go','ts','python'):
            generated = await command([str(args.cli.resolve()), 'generate', '--schema','public','--table','projects','--lang',lang,'--profile','lossless-read-v1'], studio_env)
            require(generated.returncode == 0 and b'title' in generated.stdout.lower(), 'actual scalar read artifact refused')
            path = artifact_dir / ('projects.' + {'go':'go','ts':'ts','python':'py'}[lang])
            path.write_bytes(generated.stdout)
            artifacts[str(path)] = hashlib.sha256(generated.stdout).hexdigest()
            unsupported = await command([str(args.cli.resolve()), 'generate','--schema','public','--table','documents','--lang',lang,'--profile','lossless-read-v1'], studio_env)
            require(unsupported.returncode != 0 and b'created_at' in unsupported.stderr + unsupported.stdout, 'temporal read profile was incorrectly admitted')
        schema = artifact_dir / 'schema2.json'
        pulled = await command([str(args.cli.resolve()), 'schema','pull','--out',str(schema)], owner_env)
        require(pulled.returncode == 0, 'actualschema pull refused; diagnostics withheld')
        artifacts[str(schema)] = hashlib.sha256(schema.read_bytes()).hexdigest()
        plan_dir = artifact_dir / 'no-op-plan'
        planned = await command([str(args.cli.resolve()), 'migrate','generate','--mode','live','--schema',str(schema),'--dir',str(plan_dir)], owner_env)
        require(planned.returncode == 0 and not list(plan_dir.glob('*.sql')) and stable(await snapshot(db,name,roles)) == before_noop,
                'pulledschema live plan was not a no-op')
        passed('schema2 exact CLI history/no-op, actualschema pull/live no-op and three scalar read artifacts with temporal refusal')
        await old.start(admitted=False)
        await old.stop()
        await worker(args.old_worker, admitted=False)
        require(stable(await snapshot(db,name,roles)) == before_noop, 'oldfresh revision2 refusals changed state')
        passed('retired old consumers fresh revision2 startup refuses actual admission causes without effects')
        await new.stop()
        await db.execute('UPDATE public.app_schema_revision SET revision=3')
        unsupported_state = stable(await snapshot(db,name,roles))
        await new.start(admitted=False)
        await new.stop()
        await worker(args.new_worker, admitted=False)
        require(stable(await snapshot(db,name,roles)) == unsupported_state, 'new revision3 refusal changed state')
        await db.execute('UPDATE public.app_schema_revision SET revision=2')
        passed('new consumers refuse unsupported revision3 without effects; fixture marker restored')
        preserved = await business(db)
        down = await command([str(args.cli.resolve()),'migrate','down','1','--dir',str(HERE/'migrations')],owner_env)
        require(down.returncode == 0 and await db.fetchval('SELECT revision FROM public.app_schema_revision') == 1,
                'actual002 rollback refused; diagnostics withheld')
        require(await db.fetchval("SELECT count(*) FROM pg_attribute WHERE attrelid='public.documents'::regclass AND attname='content_octets' AND NOT attisdropped") == 0,
                'rollback left generated column')
        require(await business(db) == preserved, 'derived-only rollback changed business state')
        await history(db,1)
        await old.start()
        await new.start()
        require((await old.request('/api/documents/'+third['id'])) == (await new.request('/api/documents/'+third['id'])), 'generations disagree after rollback')
        require((await worker(args.old_worker))['outcome']=='idle' and (await worker(args.new_worker))['outcome']=='idle', 'both workers failed rollback admission')
        passed('actual002 derived-only rollback preserves business state/history001 and both generations admit schema1')
        inputs={'rollout':Path(__file__),'api_source':HERE.parent/'api/main.go','new_worker':args.new_worker,'old_worker':args.old_worker,
                'old_api_binary':args.old_api,'new_api_binary':args.new_api,'cli_binary':args.cli}
        inputs.update({file.name:file for file in (HERE/'migrations').glob('*.sql')})
        private_json(args.out/('report-'+name+'.json'),{'result':'PASS','checks':checks,'artifacts':artifacts,
            'identities':{label:{'path':str(path.resolve()),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()} for label,path in inputs.items()},
            'retired_old_api_pids':sorted(old_pids),'limits':['controlled stop/restart; no zero-downtime or oldphysicalsession revision2 guarantee',
                'documents lossless temporal model unsupported; scalar artifacts generated but not compiled by this drill',
                'exact fixture/schema release only; no arbitrary rolling-schema compatibility']})
    finally:
        for api in (old,new):
            if api:
                await api.stop()
        if db:
            await db.close()
        if manifest:
            if admin is None:
                admin = await asyncpg.connect(os.environ['ADMIN_DATABASE_URL'])
            actual = await admin.fetchval('SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1',name)
            if actual is not None:
                require(actual == name+'_owner','cleanup owner mismatch')
                await admin.execute('DROP DATABASE '+ident(name)+' WITH(FORCE)')
            for role in list(manifest['roles'])+[name+'_owner']:
                await admin.execute('DROP ROLE IF EXISTS '+ident(role))
        if admin:
            await admin.close()
        shutil.rmtree(directory)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('old-api','new-api','old-worker','new-worker','cli','out'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--python-sdk',type=Path)
    try:
        asyncio.run(run(parser.parse_args()))
    except Exception as error:
        print('Rollout drill failed: '+(str(error) if type(error) is RuntimeError else type(error).__name__)+' (native diagnostics withheld)',flush=True)
        raise SystemExit(1)
