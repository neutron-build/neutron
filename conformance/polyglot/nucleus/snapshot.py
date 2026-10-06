"""Bind an unenabled Nucleus candidate assessment to source/artifact bytes.

Read-only source collection: never launches binaries, modifies engine or
generates certification. Optional binary provenance must name its engine tree.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

HERE=Path(__file__).resolve().parent

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

def snapshot(args):
    root=args.source.resolve()
    profile=json.loads((HERE/'profile.json').read_text())
    fixtures=json.loads((HERE/'fixtures.json').read_text())
    if profile['status']!='source-assessment-only' or profile['package_enabled'] or not profile['mandatory_program_requirements_retained']:
        raise ValueError('assessment cannot enable or certify package support')
    if fixtures['executed'] or fixtures['profile']!=profile['profile']:
        raise ValueError('fixture examples cannot be relabeled execution evidence')
    if (root/'source-revision').exists():
        revision=(root/'source-revision').read_text().strip()
        tree=args.nucleus_tree
        if not tree: raise ValueError('archive requires coordinator-captured nucleus tree')
    else:
        if subprocess.check_output(['git','status','--porcelain','--','nucleus'],cwd=root,text=True).strip():
            raise ValueError('dirty engine source cannot claim the assessed committed tree')
        revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
        tree=subprocess.check_output(['git','rev-parse','HEAD:nucleus'],cwd=root,text=True).strip()
    if not re.fullmatch('[0-9a-f]{40}',revision) or not re.fullmatch('[0-9a-f]{40}',tree): raise ValueError('exact source/tree identity required')
    if tree!=profile['assessed_nucleus_tree']: raise ValueError('engine tree changed; independent reassessment required')
    sources={}
    for name in profile['source_evidence']:
        source=root/name
        if source.is_symlink() or not source.resolve().is_relative_to(root) or not source.is_file(): raise ValueError('regular contained source evidence required')
        sources[name]=sha(source)
    historical=json.loads((root/'conformance/live/orm/capabilities.nucleus.json').read_text())
    report={'status':'source-assessment-only','package_enabled':False,'profile':profile['profile'],
        'source_revision':revision,'nucleus_tree':tree,'source_sha256':sources,
        'assessment_sha256':{name:sha(HERE/name) for name in ('profile.json','fixtures.json','README.md','snapshot.py')},
        'historical_recording':{'source':historical['source'],'engine':historical['engine'],'drivers':historical['drivers']},
        'historical_recording_matches_current_engine_tree':historical['source']['nucleusTree']==tree,
        'current_native_gate':'not-run','installed_orm_qualification':'not-run','independent_review':'required',
        'binary':None,'claims':profile['claims']}
    if args.binary:
        if not args.binary_provenance: raise ValueError('binary requires coordinator build provenance')
        provenance=json.loads(args.binary_provenance.read_text())
        binary=args.binary.resolve();digest=sha(binary)
        if provenance.get('nucleus_tree')!=tree or provenance.get('binary_sha256')!=digest or not re.fullmatch('[0-9a-f]{40}',provenance.get('source_revision','')):
            raise ValueError('binary/provenance does not bind assessed engine tree')
        fields=('source_revision','nucleus_tree','binary_sha256','toolchain_sha256','build_config_sha256')
        report['binary']={'sha256':digest,'provenance_sha256':sha(args.binary_provenance),'provenance':{name:provenance[name] for name in fields if name in provenance}}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'status':report['status'],'output':str(args.output),'profile':profile['profile'],'package_enabled':False}))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--nucleus-tree');parser.add_argument('--binary',type=Path);parser.add_argument('--binary-provenance',type=Path)
    try: snapshot(parser.parse_args())
    except Exception:
        print(json.dumps({'status':'fail','diagnostics':'Nucleus assessment source/artifact binding refused'}));raise SystemExit(1)
