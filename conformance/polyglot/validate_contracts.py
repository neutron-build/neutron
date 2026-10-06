"""Validate versioned fixture structure without implying runtime compliance."""
import json
from pathlib import Path


def validate(root: Path):
    manifest = json.loads((root/'values/v1/manifest.json').read_text())
    if manifest.get('format') != 'neutron-value-manifest' or manifest.get('version') != 1:
        raise ValueError('value manifest version mismatch')
    files = manifest.get('files')
    if not isinstance(files, list) or not files or len(files) != len(set(files)):
        raise ValueError('invalid fixture file list')
    seen = set()
    writes = set()
    for name in files:
        path = (root/'values/v1'/name).resolve()
        if not path.is_relative_to((root/'values/v1').resolve()):
            raise ValueError('fixture escapes root')
        data = json.loads(path.read_text())
        if data.get('format') != 'neutron-value-cases' or data.get('version') != 1 or not isinstance(data.get('cases'), list) or not data['cases']:
            raise ValueError('invalid value cases')
        for case in data['cases']:
            if not isinstance(case.get('id'), str) or case['id'] in seen:
                raise ValueError('missing or duplicate case identity')
            seen.add(case['id'])
            if 'operation' in case:
                operation = case['operation']
                if operation not in {'insert','update'} or [v.get('state') for v in case.get('inputs',[])] != ['omitted','default','null','present'] or len(case.get('expected',[])) != 4:
                    raise ValueError('write states incomplete')
                writes.add(operation)
    if writes != {'insert','update'}:
        raise ValueError('insert and update four-state cases required')
    for name in ['transitions.json','profiles.json']:
        data = json.loads((root/'execution/v1'/name).read_text())
        if data.get('format') != 'neutron-execution-cases' or data.get('version') != 1 or not data.get('cases'):
            raise ValueError('invalid execution fixture')
    transitions = json.loads((root/'execution/v1/transitions.json').read_text())['cases']
    ownership = [c for c in transitions if c.get('id') == 'concurrent-session']
    if len(ownership) != 1 or ownership[0].get('to') != 'rejected':
        raise ValueError('concurrent active session use must reject by default')
    return {'status':'validated','runtime_compliance':'unverified','value_cases':len(seen)}

if __name__ == '__main__':
    print(json.dumps(validate(Path(__file__).resolve().parents[2]/'contracts/data')))
