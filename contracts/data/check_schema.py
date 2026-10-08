"""Validate total fixture classification, then check actual JSON Schema results."""
import copy
import tempfile
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent

def classifications(manifest, root=ROOT):
    entries = manifest['invalid']
    names = [entry['name'] for entry in entries]
    if len(names) != len(set(names)):
        raise ValueError('Duplicate invalid fixture names')
    classified = {'schema_rejectable': [], 'validator_only': []}
    documents = set()
    for entry in entries:
        expectation = entry.get('schemaExpectation')
        if expectation not in classified:
            raise ValueError(f"Unclassified fixture: {entry['name']}")
        path = Path(entry['document'])
        if path != Path('invalid') / (entry['name'] + '.json') or not (root / path).is_file():
            raise ValueError(f"Invalid fixture path: {path}")
        if path in documents:
            raise ValueError(f'Duplicate fixture document: {path}')
        documents.add(path)
        classified[expectation].append(path)
    actual = {path.relative_to(root) for path in (root / 'invalid').glob('*.json')}
    if documents != actual:
        raise ValueError(f'Manifest/fixture mismatch: {documents ^ actual}')
    return classified

def check_result(expectation, result, path):
    output = result.stdout + result.stderr
    if expectation == 'validator_only':
        if result.returncode != 0: raise RuntimeError(output)
    elif result.returncode != 1 or ' invalid' not in output or 'schema is invalid' in output:
        raise RuntimeError(f'Expected schema rejection of {path}, got: {output}')

def semantic_controls(command):
    original=json.loads((ROOT/'schema-v2.json').read_text())
    for fixture in ('index-with-non-integer','index-opclass-expression'):
        schema=copy.deepcopy(original)
        index=next(v for v in schema['$defs'].values() if isinstance(v,dict) and 'with' in v.get('properties',{}))
        if fixture=='index-with-non-integer':
            index['properties']['with']['additionalProperties']['type']=['integer','string']
        else:
            expression=next(v for v in index['properties']['key']['items']['oneOf'] if 'expression' in v['properties'])
            expression['properties']['opclass']={'$ref':'#/$defs/name'}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'schema.json';path.write_text(json.dumps(schema))
            weakened=command.copy();weakened[weakened.index('-s')+1]=str(path)
            result=subprocess.run(weakened+['-d',str(ROOT/'golden/invalid'/(fixture+'.json'))],capture_output=True,text=True,timeout=60)
            if result.returncode!=0:raise RuntimeError('Weakened-schema positive control did not execute: '+result.stdout+result.stderr)
            try:check_result('schema_rejectable',result,fixture)
            except RuntimeError:print('Semantic schema relaxation rejected: '+fixture)
            else:raise RuntimeError('Gate accepted weakened schema')

def main():
    groups = classifications(json.loads((ROOT / 'golden/manifest.json').read_text()), ROOT / 'golden')
    # Positive control distinguishes a working validator from startup/schema failures.
    command = ['npx', '--yes', 'ajv-cli@5', 'validate', '--spec=draft2020', '-s', str(ROOT / 'schema-v2.json')]
    for path in sorted((ROOT / 'golden/valid').glob('*.json')):
        subprocess.run(command + ['-d', str(path)], check=True)
    count = 0
    for expectation, paths in groups.items():
        for path in paths:
            result = subprocess.run(command + ['-d', str(ROOT / 'golden' / path)], capture_output=True, text=True)
            check_result(expectation, result, path)
            count += 1
    semantic_controls(command)
    print(f'Checked {count} classified invalid fixtures')

if __name__ == '__main__':
    main()
