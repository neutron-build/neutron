"""Unreleased candidate archive consumers. No publish or dependency downloads."""
import asyncio
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
from urllib.parse import urlsplit,urlunsplit,unquote
from uuid import uuid4
import asyncpg

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
TEMPLATES=HERE.parent/'neutron-data-conformance'
OWNED=None

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def guard():
    if shutil.disk_usage(ROOT).free<6*1024**3:
        raise RuntimeError('6 GiB disk guard')

def run(args,cwd,env,log):
    result=subprocess.run(args,cwd=cwd,env=env,capture_output=True,text=True,timeout=180)
    log.write_text(result.stdout+result.stderr)
    if result.returncode:
        raise RuntimeError(f'command failed: inspect private {log.name}')
    return result.stdout

def unpack(archive,directory):
    with tarfile.open(fileobj=io.BytesIO(archive)) as tf:
        # Source/package archives must not introduce symlink escapes.
        if any(member.issym() or member.islnk() for member in tf.getmembers()):
            raise RuntimeError('archive links unsupported')
        tf.extractall(directory,filter='data')

def replace_once(text,old,new):
    if text.count(old)!=1:
        raise RuntimeError('consumer template contract changed')
    return text.replace(old,new)

async def main():
    global OWNED
    runtime=Path(os.environ['ARTIFACT_RUNTIME']).resolve()
    if runtime.exists() or runtime==ROOT or ROOT in runtime.parents:
        raise RuntimeError('new runtime outside candidate checkout required')
    guard();os.umask(0o077);runtime.mkdir(mode=0o700,parents=True);OWNED=runtime
    env=dict(os.environ,GOPROXY='off',GOSUMDB='off')
    admin_url=env.pop('ADMIN_DATABASE_URL')
    env.pop('PYTHONPATH',None);env.pop('CONFORMANCE_WRITE_PHASE',None)
    cli=Path(os.environ['NEUTRON_CLI']).resolve()
    deps=Path(os.environ['ARTIFACT_TS_DEPS']).resolve()
    tsc=Path(os.environ['ARTIFACT_TSC']).resolve()
    build_python=Path(os.environ['ARTIFACT_BUILD_PYTHON']).absolute()
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    manifest={'kind':'unreleased-candidate-artifact-consumers','revision':revision,'cli_sha256':sha(cli),'artifacts':{},'checks':{},'database_cleaned':False}
    manifest['tools']={name:subprocess.check_output(args,text=True).strip() for name,args in {'python':[sys.executable,'--version'],'build_python':[str(build_python),'--version'],'go':['go','version'],'node':['node','--version'],'typescript':[str(tsc),'--version'],'pnpm':['pnpm','--version'],'uv':['uv','--version']}.items()}
    manifest['source_hashes']={str(p.relative_to(ROOT)):sha(p) for base in (HERE,TEMPLATES,ROOT/'python',ROOT/'go',ROOT/'typescript/packages/neutron-nucleus') for p in sorted(base.rglob('*')) if p.is_file() and 'node_modules' not in p.parts and 'dist' not in p.parts and '__pycache__' not in p.parts and p.suffix in ('.py','.ts','.tmpl','.toml','.json','.mod','.sum','.sql')}
    manifest['build_tools']=json.loads(subprocess.check_output([str(build_python),'-c',"import importlib.metadata as m,json;print(json.dumps({d.metadata['Name']:d.version for d in m.distributions()}))"],text=True))
    manifest['native_dependencies']=json.loads(subprocess.check_output([sys.executable,'-c',"import importlib.metadata as m,json;print(json.dumps({name:m.version(name) for name in ['asyncpg','pydantic','pydantic-settings','starlette']}))"],text=True))
    manifest['typescript_compiler_sha256']=sha(tsc)
    build=runtime/'build';build.mkdir();artifacts=runtime/'artifacts';artifacts.mkdir()
    consumer=runtime/'consumer';consumer.mkdir();python=consumer/'python';python.mkdir();go=consumer/'go';go.mkdir();ts=consumer/'ts';ts.mkdir()
    try:
        archive=subprocess.check_output(['git','archive',revision,'python','typescript/packages/neutron-nucleus'],cwd=ROOT)
        manifest['build_source_archive_sha256']=hashlib.sha256(archive).hexdigest()
        unpack(archive,build)
        package=build/'typescript/packages/neutron-nucleus'
        (package/'node_modules').symlink_to(deps,target_is_directory=True)
        run([str(tsc),'-p','tsconfig.json'],package,env,runtime/'typescript-package-build.log')
        run(['pnpm','pack','--pack-destination',str(artifacts)],package,env,runtime/'typescript-pack.log')
        tarball=next(artifacts.glob('*.tgz'))
        target=ts/'node_modules/@neutron-build/nucleus';target.parent.mkdir(parents=True)
        package_extract=runtime/'unpacked-ts';package_extract.mkdir();unpack(tarball.read_bytes(),package_extract)
        shutil.move(str(package_extract/'package'),target)
        # Reuse only third-party dependencies. The SDK itself is physically unpacked.
        for dependency in deps.iterdir():
            if dependency.name.startswith('.') or dependency.name=='@neutron-build':continue
            (ts/'node_modules'/dependency.name).symlink_to(dependency.resolve(),target_is_directory=dependency.is_dir())
        packed=json.loads((target/'package.json').read_text())
        manifest['typescript_dependencies']={name:json.loads((ts/'node_modules'/name/'package.json').read_text())['version'] for name in ('pg','@types/pg','@types/node')}
        manifest['typescript_installed_files']={str(p.relative_to(target)):sha(p) for p in sorted(target.rglob('*')) if p.is_file()}
        manifest['artifacts']['typescript']={'file':str(tarball),'sha256':sha(tarball),'name':packed['name'],'version':packed['version'],'manifest_sha256':sha(target/'package.json'),'exports':packed['exports'],'source_lock_sha256':sha(package/'package-lock.json'),'installation':'tarball unpack; third-party dependencies reused, no npm registry install'}
        # Build the real declared Hatchling wheel with supplied existing build tools.
        run([str(build_python),'-m','build','--wheel','--no-isolation','--outdir',str(artifacts)],build/'python',env,runtime/'python-wheel-build.log')
        wheel=next(artifacts.glob('*.whl'));py_target=consumer/'python-target'
        run(['uv','pip','install','--offline','--no-deps','--target',str(py_target),'--python',sys.executable,str(wheel)],runtime,env,runtime/'python-wheel-install.log')
        manifest['artifacts']['python']={'file':str(wheel),'sha256':sha(wheel),'installation':'actual uv pip --offline --no-deps --target','target':str(py_target)}
        go_archive=artifacts/'go-sdk.tar.gz';go_archive.write_bytes(subprocess.check_output(['git','archive','--format=tar.gz',revision,'go'],cwd=ROOT))
        go_target=consumer/'go-sdk';go_target.mkdir();unpack(go_archive.read_bytes(),go_target);go_sdk=go_target/'go'
        manifest['artifacts']['go']={'file':str(go_archive),'sha256':sha(go_archive),'kind':'candidate source archive, not Go proxy release','module':'github.com/neutron-build/neutron/go','go_mod_sha256':sha(go_sdk/'go.mod'),'go_sum_sha256':sha(go_sdk/'go.sum'),'target':str(go_sdk)}
        guard()
        name='v10_artifact_'+uuid4().hex[:16];parts=urlsplit(admin_url);url=urlunsplit(parts._replace(path='/'+name));env['DATABASE_URL']=url
        admin=await asyncpg.connect(admin_url);created=False
        try:
            await admin.execute(f'CREATE DATABASE "{name}"');created=True
            connection=await asyncpg.connect(url)
            try:
                await connection.execute((HERE/'fixture.sql').read_text())
                await connection.execute('UPDATE fixture.scalars SET payload=$1 WHERE row_id=1',bytes(range(256)))
                oracle_sql="SELECT row_id,i2,i4,i8::text AS i8,amount::text AS amount,flag,label,short_label,fixed_label::text AS fixed_label,ident::text AS ident,encode(payload,'hex') AS payload FROM fixture.scalars ORDER BY row_id"
                oracle=[dict(r) for r in await connection.fetch(oracle_sql)]
                temporal=await connection.fetchval("SELECT to_char(happened AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US')||'+00:00' FROM fixture.temporal")
                for language,directory in [('python',python),('go',go),('ts',ts)]:
                    for table in ('scalars','writes'):
                        run([str(cli),'generate','--profile','lossless-read-v1','--lang',language,'--schema','fixture','--table',table,'--out',str(directory)],runtime,env,runtime/f'generate-{language}-{table}.log')
                manifest['generated_hashes']={str(p.relative_to(consumer)):sha(p) for directory in (python,go,ts) for p in directory.glob('*') if p.is_file()}
                for file in ('python_consumer.py','write_conformance.py'):
                    shutil.copy(TEMPLATES/file,python/('consumer.py' if file=='python_consumer.py' else file))
                py_env=dict(env,PYTHONPATH=str(py_target),CONFORMANCE_PY_SOURCE=str(py_target/'neutron/nucleus/client.py'))
                identity=run([sys.executable,'-c',"import neutron.nucleus.client as c,importlib.metadata as m,json,pathlib;print(json.dumps({'origin':str(pathlib.Path(c.__file__).resolve()),'version':m.version('neutron-framework')}))"],python,py_env,runtime/'python-import.log')
                manifest['artifacts']['python']['identity']=json.loads(identity)
                if Path(json.loads(identity)['origin'])!=py_target/'neutron/nucleus/client.py':raise RuntimeError('Python imported source instead of wheel')
                for table in ('scalars','writes'):
                    p=go/f'{table}.go';p.write_text('package main\n'+p.read_text())
                shutil.copy(TEMPLATES/'go_consumer.go.tmpl',go/'main.go');shutil.copy(TEMPLATES/'write_conformance.go.tmpl',go/'write_conformance.go')
                module=(ROOT/'cli/examples/neutron-data-application/api/go.mod').read_text().replace('example.local/neutron-data-api','example.local/neutron-data-artifact').replace('../../../../go',json.dumps(str(go_sdk)))
                (go/'go.mod').write_text(module);shutil.copy(ROOT/'cli/examples/neutron-data-application/api/go.sum',go/'go.sum')
                origin=json.loads(run(['go','list','-m','-json','github.com/neutron-build/neutron/go'],go,env,runtime/'go-module.log'))
                if Path(origin['Dir']).resolve()!=go_sdk.resolve():raise RuntimeError('Go module did not resolve archive')
                manifest['artifacts']['go']['resolved_module']=origin
                run(['go','build','-o',str(go/'consumer'),'.'],go,env,runtime/'go-consumer-build.log');guard()
                manifest['artifacts']['go']['consumer_sha256']=sha(go/'consumer')
                manifest['artifacts']['go']['build_info']=subprocess.check_output(['go','version','-m',str(go/'consumer')],text=True)
                source=(TEMPLATES/'ts_consumer.ts').read_text()
                for old,new in [("'./sdk/client.js'","'@neutron-build/nucleus'"),("'./sdk/transport.js'","'@neutron-build/nucleus'"),("'./sdk/sql/index.js'","'@neutron-build/nucleus/sql'")]:source=replace_once(source,old,new)
                (ts/'consumer.ts').write_text(source)
                helper=replace_once((TEMPLATES/'write_conformance.ts').read_text(),"'./sdk/sql/index.js'","'@neutron-build/nucleus/sql'");(ts/'write_conformance.ts').write_text(helper)
                (ts/'package.json').write_text(json.dumps({'private':True,'type':'module','dependencies':{'@neutron-build/nucleus':'file:'+str(tarball)}}))
                (ts/'tsconfig.json').write_text(json.dumps({'compilerOptions':{'target':'ES2022','module':'NodeNext','moduleResolution':'NodeNext','strict':True,'skipLibCheck':True,'esModuleInterop':True,'outDir':'build'},'include':['*.ts']}))
                run([str(tsc),'-p','tsconfig.json'],ts,env,runtime/'typescript-consumer-typecheck.log')
                resolved=json.loads(run(['node','--input-type=module','-e',"console.log(JSON.stringify({root:import.meta.resolve('@neutron-build/nucleus'),sql:import.meta.resolve('@neutron-build/nucleus/sql')}))"],ts,env,runtime/'typescript-exports.log'))
                paths={key:Path(unquote(urlsplit(value).path)).resolve() for key,value in resolved.items()}
                if paths!={'root':target/'dist/index.js','sql':target/'dist/sql/index.js'} or target.is_symlink():
                    raise RuntimeError('TypeScript did not resolve tarball exports')
                manifest['artifacts']['typescript']['resolved_exports']=resolved
                native={'python':([sys.executable,'consumer.py'],python,py_env,100),'go':([str(go/'consumer')],go,env,200),'typescript':(['node','build/consumer.js'],ts,env,300)}
                expected={};version=9007199254740993
                columns=('i2','i4','i8','amount','flag','label','short_label','fixed_label','ident','payload')
                high=(32767,2147483647,'9223372036854775807','9999999999999999999999.999999999999999999',False,'','','WXYZ','ffffffff-ffff-ffff-ffff-ffffffffffff','')
                for phase in ('write','update','read'):
                    for language,(args,directory,lang_env,base) in native.items():
                        if phase=='write':
                            expected[base]=dict(row_id=base,**dict(zip(columns,high)),revision=str(version+1));expected[base+1]=dict(row_id=base+1,**dict.fromkeys(columns),revision=str(version))
                        elif phase=='update':
                            for key,row in expected.items():
                                if key not in (base,base+1):row['revision']=str(int(row['revision'])+1)
                        actual=json.loads(run(args,directory,dict(lang_env,CONFORMANCE_WRITE_PHASE=phase),runtime/f'{language}-{phase}.log'))
                        wanted=[expected[k] for k in sorted(expected)]
                        observed=[dict(r) for r in await connection.fetch(oracle_sql.replace('FROM fixture.scalars',',revision::text AS revision FROM fixture.writes'))]
                        if actual['rows']!=oracle or actual['temporal']!=temporal or actual['writes']!=wanted or observed!=wanted:
                            raise RuntimeError(f'{language}/{phase} differs from independent artifact oracle')
                        manifest['checks'][f'{language}/{phase}']={'oracle_match':True,'rows':observed}
                manifest['matrix']={'native_writers':3,'cross_writer_pairs':6,'cross_row_CAS_updates':12,'final_cross_readers':3,'final_rows':6,'rollback_leaks':0}
            finally:await connection.close()
        finally:
            try:
                if created:await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)');manifest['database_cleaned']=True
            finally:await admin.close()
        manifest['consumer_hashes']={str(p.relative_to(consumer)):sha(p) for base in (python,go,ts) for p in sorted(base.rglob('*')) if p.is_file() and 'node_modules' not in p.parts and '__pycache__' not in p.parts}
        guard();manifest['result']='PASS'
    finally:
        (runtime/'provenance.json').write_text(json.dumps(manifest,indent=2))
    print('PASS: unreleased TS tarball exports, installed Python wheel and Go source-archive native consumers; exact oracle; database cleaned')

if __name__=='__main__':
    try:asyncio.run(main())
    except Exception as exc:
        if OWNED is not None:
            import traceback
            detail=traceback.format_exc()
            for key in ('ADMIN_DATABASE_URL','DATABASE_URL'):
                if os.environ.get(key):detail=detail.replace(os.environ[key],'[private URL]')
            (OWNED/'failure.log').write_text(detail)
        print(f'FAIL: {type(exc).__name__}; inspect private artifact runtime',file=sys.stderr)
        raise SystemExit(1)
