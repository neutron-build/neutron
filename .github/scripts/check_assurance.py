"""Source-only policy/registry lint. It never certifies reporting delivery or proofs."""
import json
from pathlib import Path
import re
import subprocess
ROOT=Path(__file__).resolve().parents[2]

def check():
    policy=(ROOT/'SECURITY.md').read_text()
    if re.search(r'\b(?:SOON|TODO|FIXME)\b|security@|72.hour',policy,re.I):
        raise ValueError('Placeholder or unprovisioned security promise')
    if 'currently disabled' not in policy:
        if not (ROOT/'contracts/security/reporting-readiness.json').exists():
            raise ValueError('Enabled policy requires actual maintainer route/support/delivery receipt')
        receipt=json.loads((ROOT/'contracts/security/reporting-readiness.json').read_text())
        for key in ['route','primary','backup','supportedVersions','acknowledgmentTarget','deliveryReceipt']:
            if not receipt.get(key):raise ValueError('Missing reporting readiness field: '+key)
    for name in re.findall(r'`([a-z][a-z0-9_-]*/)`',policy):
        if not (ROOT/name).is_dir() and not subprocess.check_output(['git','ls-tree','HEAD',name.rstrip('/')],cwd=ROOT,text=True).strip():raise ValueError('Stale policy path '+name)
    manifest=json.loads((ROOT/'verus/manifest.json').read_text())
    for entry in manifest['targets']:
        source=(ROOT/'verus'/entry['path']).read_text()
        for prop in entry['properties']:
            if not re.search(r'\bfn '+re.escape(prop['function'])+r'\b',source):raise ValueError('Stale Verus property')
    for name in ['lean4/README.md','quint/README.md']:
        text=(ROOT/name).read_text()
        if 'all 92 theorems' in text or 'direct Rust-code verification' in text:raise ValueError('Stale assurance headline')
    print('Policy paths and property references checked; no runtime/proof claim')
if __name__=='__main__':check()
