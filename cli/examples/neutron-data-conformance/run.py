"""Disposable PostgreSQL native read conformance. Never print connection URLs."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4
import asyncpg

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def command(args, cwd, env, log, expect_failure=False):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True, timeout=180)
    # Raw diagnostics can include a DSN: store privately, never echo them.
    log.write_text(result.stdout + result.stderr)
    if (result.returncode == 0) == expect_failure:
        raise RuntimeError(f'command outcome unexpected; inspect private {log.name}')
    return result.stdout

async def main():
    admin_url = os.environ['ADMIN_DATABASE_URL']
    cli = Path(os.environ['NEUTRON_CLI']).resolve()
    runtime = Path(os.environ['CONFORMANCE_RUNTIME']).resolve()
    ts_source = Path(os.environ.get('CONFORMANCE_TS_SOURCE', str(ROOT/'typescript/packages/neutron-nucleus/src'))).resolve()
    deps = Path(os.environ['CONFORMANCE_TS_DEPS']).resolve()
    tsc = Path(os.environ['CONFORMANCE_TSC']).resolve()
    if runtime == ROOT or ROOT in runtime.parents or runtime.exists():
        raise RuntimeError('runtime must be a new private directory outside the candidate source')
    if shutil.disk_usage(ROOT).free < 6 * 1024**3:
        raise RuntimeError('6 GiB disk guard')
    runtime.mkdir(mode=0o700, parents=True)
    os.umask(0o077)
    name = 'v10_conformance_' + uuid4().hex[:16]
    parts = urlsplit(admin_url)
    url = urlunsplit(parts._replace(path='/' + name))
    env = dict(os.environ, DATABASE_URL=url, PYTHONPATH=str(ROOT / 'python'), GOPROXY='off', GOSUMDB='off', CONFORMANCE_PY_SOURCE=str(ROOT/'python/neutron/nucleus/client.py'))
    env.pop('ADMIN_DATABASE_URL', None)
    manifest = {'source_commit': subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(), 'cli_sha256':digest(cli), 'sources':{}, 'outcomes':{}, 'database_cleaned':False}
    for base in (ROOT/'go/nucleus',ROOT/'python/neutron/nucleus',ROOT/'cli/internal/studio',HERE):
        for path in sorted(base.rglob('*')):
            if path.is_file() and path.suffix in ('.go','.py','.ts','.tmpl'):
                manifest['sources'][str(path.relative_to(ROOT))] = digest(path)
    manifest['tools']={name:subprocess.check_output(args,text=True).strip() for name,args in {'python':[sys.executable,'--version'],'go':['go','version'],'node':['node','--version'],'typescript':[str(tsc),'--version']}.items()}
    manifest['typescript_source']={str(p.relative_to(ts_source)):digest(p) for p in sorted(ts_source.rglob('*.ts'))}
    admin = await asyncpg.connect(admin_url)
    created = False
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        created = True
        conn = await asyncpg.connect(url)
        try:
            await conn.execute('''CREATE SCHEMA fixture;
CREATE TABLE fixture.scalars(row_id int4 PRIMARY KEY,i2 int2,i4 int4,i8 int8,amount numeric(40,18),flag bool,label text,short_label varchar(30),fixed_label bpchar(4),ident uuid,payload bytea);
CREATE TABLE fixture.temporal(happened timestamptz NOT NULL);
INSERT INTO fixture.scalars VALUES
(1,-32768,-2147483648,-9223372036854775808,-9999999999999999999999.123456789012345678,true,'quotes '' Unicode λ','short','ABCD','00000000-0000-0000-0000-000000000001',decode('00017f80ff','hex')),
(2,32767,2147483647,9223372036854775807,9999999999999999999999.999999999999999999,false,'','', 'WXYZ','ffffffff-ffff-ffff-ffff-ffffffffffff',decode('','hex')),
(3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO fixture.temporal VALUES ('2024-03-01T01:59:59.123456+02:00');''')
            await conn.execute('UPDATE fixture.scalars SET payload=$1 WHERE row_id=1', bytes(range(256)))
            # Independent SQL oracle, not generated/native model serialization.
            oracle = [dict(r) for r in await conn.fetch("SELECT row_id,i2,i4,i8::text AS i8,amount::text AS amount,flag,label,short_label,fixed_label::text AS fixed_label,ident::text AS ident,encode(payload,'hex') AS payload FROM fixture.scalars ORDER BY row_id")]
            temporal = await conn.fetchval("SELECT to_char(happened AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US')||'+00:00' FROM fixture.temporal")
            manifest['oracle']={'rows':oracle,'temporal':temporal}
            for lang in ('go','ts','python'):
                directory = runtime / lang
                directory.mkdir()
                command([str(cli),'generate','--profile','lossless-read-v1','--lang',lang,'--schema','fixture','--table','scalars','--out',str(directory)],runtime,env,runtime/f'generate-{lang}.log')
            manifest['generated_sources']={str(p.relative_to(runtime)):digest(p) for p in runtime.rglob('scalars.*')}
            # --all must not overwrite even an admitted first-table sentinel.
            await conn.execute('CREATE SCHEMA refused; CREATE TABLE refused.a_valid(id int4); CREATE TABLE refused.z_temporal(happened timestamptz)')
            for lang,ext in [('go','go'),('ts','ts'),('python','py')]:
                target=runtime/f'refusal-{lang}';target.mkdir();sentinel=target/f'a_valid.{ext}';sentinel.write_text('preexisting sentinel\n')
                command([str(cli),'generate','--profile','lossless-read-v1','--lang',lang,'--schema','refused','--all','--out',str(target)],runtime,env,runtime/f'refusal-{lang}.log',True)
                if 'unsupported identity for lossless-read-v1' not in (runtime/f'refusal-{lang}.log').read_text():
                    raise RuntimeError('unexpected unsupported-table refusal')
                command([str(cli),'generate','--profile','lossless-read-v999','--lang',lang,'--schema','fixture','--table','scalars','--out',str(target)],runtime,env,runtime/f'unknown-profile-{lang}.log',True)
                if 'unknown codegen profile' not in (runtime/f'unknown-profile-{lang}.log').read_text():
                    raise RuntimeError('unexpected unknown-profile refusal')
                if sentinel.read_text() != 'preexisting sentinel\n' or sorted(p.name for p in target.iterdir()) != [sentinel.name]:
                    raise RuntimeError('partial output on refused profile')
            manifest['outcomes']['unsupported_batch']='refused without partial output in all three languages'
        finally:
            await conn.close()
        py=runtime/'python';shutil.copy(HERE/'python_consumer.py',py/'consumer.py')
        result=command([sys.executable,'consumer.py'],py,env,runtime/'python-runtime.log')
        manifest['outcomes']['python']=json.loads(result)
        if manifest['outcomes']['python'] != manifest['oracle']:
            raise RuntimeError('Python differs from independent SQL oracle')
        go=runtime/'go';generated=go/'scalars.go';generated.write_text('package main\n'+generated.read_text());shutil.copy(HERE/'go_consumer.go.tmpl',go/'main.go')
        # Reuse the example's pinned module dependency set; replace only local source.
        module=(ROOT/'cli/examples/neutron-data-application/api/go.mod').read_text().replace('example.local/neutron-data-api','example.local/neutron-data-conformance').replace('../../../../go',json.dumps(str(ROOT/'go')))
        (go/'go.mod').write_text(module);shutil.copy(ROOT/'cli/examples/neutron-data-application/api/go.sum',go/'go.sum')
        command(['go','build','-o',str(go/'consumer'),'.'],go,env,runtime/'go-build.log')
        manifest['go_binary_sha256']=digest(go/'consumer')
        manifest['go_binary_build_info']=subprocess.check_output(['go','version','-m',str(go/'consumer')],text=True)
        manifest['outcomes']['go']=json.loads(command([str(go/'consumer')],go,env,runtime/'go-runtime.log'))
        if manifest['outcomes']['go'] != manifest['oracle']:
            raise RuntimeError('Go differs from independent SQL oracle')
        ts=runtime/'ts';shutil.copytree(ts_source,ts/'sdk');shutil.copy(HERE/'ts_consumer.ts',ts/'consumer.ts');(ts/'node_modules').symlink_to(deps,target_is_directory=True)
        (ts/'package.json').write_text('{"type":"module"}')
        (ts/'tsconfig.json').write_text(json.dumps({'compilerOptions':{'target':'ES2022','module':'NodeNext','moduleResolution':'NodeNext','strict':True,'skipLibCheck':True,'esModuleInterop':True,'outDir':'build'},'include':['consumer.ts','scalars.ts','required-nullable.ts','sdk/**/*.ts'],'exclude':['sdk/**/*.test.ts']}))
        command([str(tsc),'-p','tsconfig.json'],ts,env,runtime/'ts-typecheck.log')
        (ts/'required-nullable.ts').write_text("import type { Scalars } from './scalars.js';\nconst missing: Scalars = {row_id:3};\n")
        command([str(tsc),'-p','tsconfig.json','--noEmit'],ts,env,runtime/'ts-required-nullable.log',True)
        if 'required-nullable.ts' not in (runtime/'ts-required-nullable.log').read_text() or 'missing' not in (runtime/'ts-required-nullable.log').read_text():
            raise RuntimeError('unexpected required-nullable compile failure')
        (ts/'required-nullable.ts').unlink()
        manifest['outcomes']['typescript']=json.loads(command(['node','build/consumer.js'],ts,env,runtime/'ts-runtime.log'))
        for lang in ('python','go','typescript'):
            actual=manifest['outcomes'][lang]
            if actual['rows'] != oracle or actual['temporal'] != temporal:
                raise RuntimeError(f'{lang} differs from independent SQL oracle')
        manifest['oracle']={'rows':oracle,'temporal':temporal}
        for path in runtime.rglob('*'):
            if path.is_file() and path.suffix in ('.go','.ts','.py') and 'node_modules' not in path.parts:
                manifest.setdefault('runtime_sources',{})[str(path.relative_to(runtime))]=digest(path)
        manifest['build_configuration']={str(p.relative_to(runtime)):digest(p) for p in [go/'go.mod',go/'go.sum',ts/'tsconfig.json',ts/'package.json']}
    finally:
        try:
            if created:
                await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
                manifest['database_cleaned']=True
        finally:
            await admin.close()
            (runtime/'provenance.json').write_text(json.dumps(manifest,indent=2))
    print('PASS: three native scalar consumers, separate temporal checks, batch refusal, database cleaned')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as exc:
        runtime = Path(os.environ.get('CONFORMANCE_RUNTIME', ''))
        if runtime.is_dir():
            detail = traceback.format_exc()
            for key in ('ADMIN_DATABASE_URL', 'DATABASE_URL'):
                if os.environ.get(key):
                    detail = detail.replace(os.environ[key], '[private URL]')
            (runtime/'failure.log').write_text(detail)
        print(f'FAIL: {type(exc).__name__}; inspect private runtime diagnostics',file=sys.stderr)
        raise SystemExit(1)
