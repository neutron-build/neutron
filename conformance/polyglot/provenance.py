"""Bind real installed archives to exact source revision and source-file bytes."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

def capture(root: Path, package: Path, destination: Path) -> None:
    root=root.resolve();package=package.resolve()
    if not package.is_relative_to(root): raise ValueError('source package escapes repository')
    revision_file=root/'source-revision'
    archive_manifest=root/'source-manifest.json'
    if revision_file.exists() or archive_manifest.exists():
        if not revision_file.is_file() or not archive_manifest.is_file(): raise ValueError('complete archive provenance required')
        revision=revision_file.read_text().strip()
        names=json.loads(archive_manifest.read_text())
    else:
        revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
        names=subprocess.check_output(['git','ls-files','-z','--',str(package.relative_to(root))],cwd=root).decode().split('\0')
    if not re.fullmatch('[0-9a-f]{40}',revision): raise ValueError('exact source revision required')
    files=[]
    for name in names:
        if not isinstance(name,str): raise ValueError('invalid source manifest')
        if not name: continue
        path=(root/name).resolve()
        if not path.is_relative_to(root): raise ValueError('source path escapes repository')
        if not path.is_relative_to(package): continue
        if not path.is_file() or (root/name).is_symlink(): raise ValueError('regular source files required')
        files.append({'path':name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    if not files: raise ValueError('nonempty source snapshot required')
    destination.write_text(json.dumps({'source_revision':revision,'files':sorted(files,key=lambda item:item['path'])},indent=2)+'\n')
