"""Attach performance consumers to three already qualified outside-origin installs.

This preserves existing artifacts.json and scalar runners. A separate manifest
binds original installed bytes, new performance source/binary, and build identity.
Go compilation must run on the timing host, under coordinator serialization.
"""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import signal
import subprocess
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from protocol import verify_artifacts

def command(argv,cwd,env,timeout):
    process=subprocess.Popen(argv,cwd=cwd,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
    try: output,_=process.communicate(timeout=timeout)
    except BaseException:
        try: os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError: pass
        process.wait();raise
    if process.returncode: raise ValueError('performance preparation subprocess failed (diagnostics suppressed)')
    return output

def prepare(args):
    origin=args.source.resolve()
    revision_file=origin/'source-revision'
    if revision_file.is_file(): revision=revision_file.read_text().strip()
    else:
        revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=origin,text=True).strip()
    if args.revision != revision: raise ValueError('source revision differs from archived/worktree identity')
    clients={}
    def extend(root, files):
        original=json.loads((root/'artifacts.json').read_text())
        verify_artifacts(original,root)
        entries={item['path']:item for item in original['files']}
        for path in files:
            entries[str(path.relative_to(root))]={'path':str(path.relative_to(root)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
        manifest=root/'performance-artifacts.json'
        manifest.write_text(json.dumps({'files':list(entries.values())},indent=2)+'\n')
        return str(manifest)
    for kind,value in [('python',args.python),('typescript',args.typescript),('go',args.go)]:
        root=value.resolve()
        if root.is_relative_to(origin) or root==origin: raise ValueError('outside-origin consumers required')
        verify_artifacts(json.loads((root/'artifacts.json').read_text()),root)
        if kind=='go': installed_revision=(root/'provenance/source-revision').read_text().strip()
        else: installed_revision=json.loads((root/'source-identity.json').read_text())['source_revision']
        if installed_revision!=args.revision: raise ValueError('installed source provenance differs from campaign revision')
        directory=root/'performance'
        directory.mkdir(exist_ok=False)
        profile=directory/'profile.json';profile.write_bytes(Path(__file__).with_name('profile.json').read_bytes())
        files=[profile]
        if kind=='python':
            adapter=directory/'python.py';adapter.write_bytes(Path(__file__).with_name('python.py').read_bytes());files.append(adapter)
            prefix=root/'client-env'
            if not (prefix/'bin/python').is_file(): raise ValueError('qualified Python venv required')
            identity=directory/'runtime-identity.json'
            executable=(prefix/'bin/python').resolve()
            identity.write_text(json.dumps({'executable':str(executable),'sha256':hashlib.sha256(executable.read_bytes()).hexdigest(),'host':platform.uname()._asdict()}))
            files.append(identity)
            manifest=extend(root,files)
            for mode in ('sync','async'):
                clients['python-'+mode]={'command':[str(prefix/'bin/python'),'-I','-B',str(adapter),'--execution',mode,'--prefix',str(prefix)],'artifact_root':str(root),'artifact_manifest':manifest}
        elif kind=='typescript':
            adapter=directory/'typescript.mjs';adapter.write_bytes(Path(__file__).with_name('typescript.mjs').read_bytes());files.append(adapter)
            entry=root/'node_modules/@neutron-build/sql/dist/index.js'
            if not entry.is_file(): raise ValueError('qualified installed TS package required')
            identity=directory/'runtime-identity.json'
            executable=Path(shutil.which('node')).resolve()
            identity.write_text(json.dumps({'executable':str(executable),'sha256':hashlib.sha256(executable.read_bytes()).hexdigest(),'host':platform.uname()._asdict()}))
            files.append(identity)
            manifest=extend(root,files)
            for driver in ('pg','postgres'):
                command=[str(executable),str(adapter),'--module',str(entry)]
                if driver=='postgres': command.append('--postgres-js')
                clients['typescript-'+driver]={'command':command,'artifact_root':str(root),'artifact_manifest':manifest}
        else:
            source=directory/'main.go';source.write_bytes((Path(__file__).with_name('go')/'main.go').read_bytes())
            for name in ('go.mod','go.sum'): (directory/name).write_bytes((root/'app'/name).read_bytes())
            # The scalar app's sibling module replacement remains correct.
            binary=directory/'adapter'
            env={k:v for k,v in os.environ.items() if not k.startswith('PG') and not k.endswith('DATABASE_URL') and k not in ('DB_URL','PYTHONPATH','PYTHONHOME')}
            env.update({'GOWORK':'off','GOTOOLCHAIN':'local','GOFLAGS':''})
            command(['go','build','-mod=readonly','-trimpath','-o',str(binary),'.'],directory,env,180)
            toolchain=directory/'toolchain.txt'
            toolchain.write_bytes(command(['go','version','-m',str(binary)],directory,env,30))
            files.extend(directory/name for name in ('main.go','go.mod','go.sum','adapter','toolchain.txt'))
            manifest=extend(root,files)
            clients['go-pgx']={'command':[str(binary)],'artifact_root':str(root),'artifact_manifest':manifest}
    args.output.write_text(json.dumps({'source_revision':args.revision,'clients':clients},indent=2)+'\n')

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--revision',required=True)
    for label in ('python','typescript','go'): parser.add_argument('--'+label,type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    try: prepare(parser.parse_args())
    except Exception:
        print('performance consumer preparation failed; diagnostics suppressed',file=sys.stderr)
        raise SystemExit(1)
