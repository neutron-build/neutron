"""Build/install a real wheel, then qualify isolated sync and async adapters.

Requires host pip, venv and psycopg for the independent native oracle. No source
editable installs, skip-as-pass, publication, or automatic work-directory removal.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
# Explicit trusted tooling directory works with Python's isolated (-I) entry.
sys.path.insert(0,str(Path(__file__).resolve().parent))
from protocol import PROTOCOL, redact

ROOT=Path(__file__).resolve().parent


def command(argv: list[str], cwd: Path, timeout: float, capture: bool=False, live: bool=False) -> str:
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('finite positive subprocess timeout required')
    env={name:value for name,value in os.environ.items()
         if not name.startswith('PG') and not name.endswith('DATABASE_URL')
         and not name.endswith('DB_URL') and name != 'NEUTRON_SQL_TEST_URL'}
    for name in ('PYTHONPATH','PYTHONHOME'): env.pop(name,None)
    if live:
        url=os.environ.get('NEUTRON_TEST_DATABASE_URL')
        if not url: raise ValueError('required live PostgreSQL URL missing')
        env['NEUTRON_TEST_DATABASE_URL']=url
    env['PIP_DISABLE_PIP_VERSION_CHECK']='1'
    proc=subprocess.Popen(argv,cwd=cwd,env=env,stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
    try: stdout,stderr=proc.communicate(timeout=timeout)
    except BaseException as exc:
        try: os.killpg(proc.pid,signal.SIGKILL)
        except ProcessLookupError: pass
        # Repeated terminal interrupts must not abandon a still-unreaped child.
        while True:
            try:
                proc.wait()
                break
            except KeyboardInterrupt: continue
        # Discard pipe data rather than retrying the operation that failed.
        for pipe in (proc.stdout,proc.stderr):
            if pipe is not None: pipe.close()
        message='qualification subprocess timed out' if isinstance(exc,subprocess.TimeoutExpired) else 'qualification subprocess interrupted'
        raise ValueError(message) from exc
    if proc.returncode:
        # pip errors can contain private indexes, credentials and user parameters.
        # Preserve only bounded command identity and code, never arbitrary stderr.
        raise ValueError(f'qualification subprocess failed ({Path(argv[0]).name}, exit {proc.returncode})')
    return stdout if capture else ''


def qualify(source: Path, work: Path, timeout: float) -> dict:
    source=source.resolve();work=work.resolve()
    if not os.environ.get('NEUTRON_TEST_DATABASE_URL'):
        raise ValueError('required live PostgreSQL URL missing')
    if not (source/'pyproject.toml').is_file() or not (source/'neutron'/'orm').is_dir():
        raise ValueError('Python ORM package source required')
    repo=source.parent
    if work.is_relative_to(repo) or repo.is_relative_to(work):
        raise ValueError('owned work directory must be outside the package repository')
    # Never claim a preexisting environment/directory.
    work.mkdir(parents=True,exist_ok=False)
    wheels=work/'wheels';wheels.mkdir()
    environment=work/'client-env'
    command([sys.executable,'-I','-m','venv',str(environment)],work,timeout)
    python=environment/'bin'/'python'
    # pip wheel resolves the actual orm optional dependency and all package deps.
    command([str(python),'-I','-m','pip','wheel','--wheel-dir',str(wheels),str(source)+'[orm]'],work,timeout)
    clients=list(wheels.glob('neutron_framework-*.whl'))
    if len(clients)!=1: raise ValueError('exactly one built client wheel required')
    command([str(python),'-I','-m','pip','install','--no-index','--find-links',str(wheels),str(clients[0])+'[orm]'],work,timeout)
    runtime_program="import hashlib,json,pathlib,sys;binary=pathlib.Path(sys.executable).resolve();print(json.dumps({'version':sys.version,'implementation':sys.implementation.name,'executable':str(binary),'executable_sha256':hashlib.sha256(binary.read_bytes()).hexdigest()}))"
    runtime=command([str(python),'-I','-B','-c',runtime_program],work,timeout,True)
    (work/'runtime-identity.json').write_text(runtime)
    freeze=command([str(python),'-I','-m','pip','freeze','--all'],work,timeout,True)
    (work/'resolved-requirements.txt').write_text(freeze)
    shutil.copy2(source/'pyproject.toml',work/'direct-requirements.toml')
    tooling=work/'tooling';tooling.mkdir();(tooling/'adapters').mkdir()
    for filename in ('protocol.py','oracle.py','runner.py'):
        shutil.copy2(ROOT/filename,tooling/filename)
    shutil.copy2(ROOT/'adapters'/'python.py',tooling/'adapters'/'python.py')
    manifests=[]
    for mode in ('sync','async'):
        manifest={'protocol':PROTOCOL,'cases':[{'id':'scalar-extremes','kind':'adapter-read','command':[
            str(python),'-I','-B',str(tooling/'adapters'/'python.py'),'--mode',mode,
            '--prefix',str(environment),'--artifact-root',str(work)]}]}
        path=work/f'{mode}-manifest.json';path.write_text(json.dumps(manifest,indent=2));manifests.append(path)
    # Bind wheelhouse (including actual native driver binaries), installed files,
    # tooling, direct requirements, resolved lock, and separate mode manifests.
    files=[]
    for path in sorted(work.rglob('*')):
        if path.is_file() and not path.is_symlink() and '__pycache__' not in path.parts and path.suffix!='.pyc':
            files.append({'path':str(path.relative_to(work)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    artifact={'protocol':PROTOCOL,'files':files}
    (work/'artifacts.json').write_text(json.dumps(artifact,indent=2))
    results={}
    for mode,path in zip(('sync','async'),manifests):
        bootstrap="import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_path(sys.argv.pop(1),run_name='__main__')"
        output=command([sys.executable,'-I','-B','-c',bootstrap,str(tooling),str(tooling/'runner.py'),'--manifest',str(path),
                        '--artifact-manifest',str(work/'artifacts.json'),'--artifact-root',str(work),
                        '--required','--timeout',str(timeout)],work,timeout*5,True,live=True)
        result=json.loads(output)
        if result.get('status')!='pass' or result.get('executed')!=1 or result.get('kind')!='adapter-conformance':
            raise ValueError('required packaged Python adapter failed')
        results[mode]=result
        (work/f'{mode}-result.json').write_text(json.dumps(result,indent=2))
    return {'status':'pass','qualification':'installed-python-scalar-read-v1','work_directory':str(work),
            'wheel':clients[0].name,'modes':results,'scope':'scalar-extremes only; no wider ORM parity certification'}


def main() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--work-directory',type=Path,required=True)
    parser.add_argument('--timeout',type=float,default=180)
    args=parser.parse_args()
    try:
        if not math.isfinite(args.timeout) or args.timeout<=0:
            raise ValueError('finite positive subprocess timeout required')
        print(json.dumps(qualify(args.source,args.work_directory,args.timeout)))
        return 0
    except Exception as exc:
        print(json.dumps({'status':'fail','diagnostics':redact(str(exc),os.environ.get('NEUTRON_TEST_DATABASE_URL',''))}))
        return 1

if __name__=='__main__':
    raise SystemExit(main())
