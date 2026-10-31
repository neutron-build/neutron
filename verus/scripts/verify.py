"""Fail closed on missing/wrong tool, missing targets, or zero verified obligations."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
ROOT=Path(__file__).resolve().parents[1]
def verify():
    manifest=json.loads((ROOT/'manifest.json').read_text())
    binary=shutil.which('verus')
    if not binary: raise RuntimeError('Verus unavailable: no proof checked')
    version=subprocess.run([binary,'--version'],capture_output=True,text=True,check=True,timeout=30).stdout
    if not re.search(r'(?<![\w.])'+re.escape(manifest['release'])+r'(?![\w.])',version): raise RuntimeError('Verus release does not match pinned manifest')
    targets=manifest['targets']
    if not targets or len({t['path'] for t in targets}) != len(targets): raise ValueError('Missing/duplicate checked targets')
    mapping=manifest['productionMapping']
    if hashlib.sha256((ROOT.parent/mapping['source']).read_bytes()).hexdigest()!=mapping['sha256']:
        raise ValueError('Production algorithm mapping changed; review source before repinning')
    total=0
    receipts=[]
    for entry in targets:
        path=ROOT/entry['path']; text=path.read_text()
        if 'verus!' not in text or re.search(r'\b(admit|assume)\s*\(|external_body',text): raise ValueError('Unchecked assumptions in active target')
        functions=re.findall(r'pub (?:open spec |proof )?fn (\w+)',text)
        if {p['function'] for p in entry['properties']} - set(functions):
            raise ValueError('Manifest refers to absent checked functions')
        result=subprocess.run([binary,str(path)],capture_output=True,text=True,timeout=120)
        output=result.stdout+result.stderr
        print(output)
        matches=re.findall(r'verification results::\s*(\d+) verified,\s*(\d+) errors',output)
        match=matches[0] if len(matches)==1 else None
        if result.returncode != 0 or not match or int(match[0]) < entry['minimumVerified'] or int(match[1]) != 0:
            raise RuntimeError('Verus did not report the required checked obligations')
        total+=int(match[0])
        receipts.append({'target':entry['path'],'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'properties':entry['properties'],'verified':int(match[0])})
    print(json.dumps({'release':manifest['release'],'checkedObligations':total,'targets':receipts,'productionMapping':mapping}))
if __name__=='__main__': verify()
