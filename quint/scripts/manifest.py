"""Load-bearing Quint inventory: all files and all scenario modules must be declared."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
ROOT = Path(__file__).resolve().parents[1]

def inventory(manifest=None):
    data = (manifest or json.loads((ROOT / 'manifest.json').read_text()))['files']
    paths = [e['path'] for e in data]
    actual = {str(p.relative_to(ROOT)) for directory in ('specs','tests','conformance') for p in (ROOT / directory).rglob('*.qnt')}
    if len(paths) != len(set(paths)) or set(paths) != actual:
        raise ValueError('Quint manifest must cover all files exactly once')
    for entry in data:
        text = (ROOT / entry['path']).read_text()
        declarations = list(re.finditer(r'^module\s+(\w+)\s*\{', text, re.M))
        modules = entry['modules']
        runnable = re.search(r'action init\b',text) and re.search(r'action step\b',text)
        if bool(runnable) != ('invariant' in entry):
            raise ValueError(f"Runnable safety model must declare its invariant: {entry['path']}")
        if [m['module'] for m in modules] != [m.group(1) for m in declarations]:
            raise ValueError(f"Missing/duplicate module: {entry['path']}")
        for i, module in enumerate(modules):
            end = declarations[i+1].start() if i+1 < len(declarations) else len(text)
            count = len(re.findall(r'^\s*run \w+_test\b',text[declarations[i].start():end],re.M))
            if module['role'] not in ('scenario','typecheck') or count != module['expectedTests'] or (count > 0) != (module['role'] == 'scenario'):
                raise ValueError(f"Invalid role/count: {entry['path']}:{module['module']}")
    return data

def run(mode):
    total = 0
    for entry in inventory():
        for module in entry['modules']:
            if mode == 'test' and module['role'] != 'scenario': continue
            if mode == 'run' and ('invariant' not in entry or module != entry['modules'][0]): continue
            command = ['quint', mode, str(ROOT / entry['path'])]
            if mode == 'typecheck' and module != entry['modules'][0]: continue
            if mode == 'test': command += ['--main', module['module']]
            if mode == 'test': command += ['--match', '.*_test$', '--seed', '20261007', '--max-samples', '1', '--backend', 'typescript']
            if mode == 'run': command += ['--seed','20261007','--backend','typescript','--max-samples',os.environ.get('MAX_SAMPLES','500'),'--max-steps',os.environ.get('MAX_STEPS','40'),'--invariant',entry['invariant']]
            result = subprocess.run(command, text=True, capture_output=True, timeout=180)
            output = result.stdout + result.stderr
            print(f"{entry['path']}:{module['module']}\n{output}", flush=True)
            if result.returncode != 0: raise RuntimeError('Quint invocation failed')
            if mode == 'run':
                if 'No violation found' not in output or 'An invariant was violated' in output: raise RuntimeError('Simulation did not confirm invariant checking')
                total += 1
            if mode == 'test':
                matched = re.search(r'(\d+) passing', output)
                count = int(matched.group(1)) if matched else 0
                if count != module['expectedTests'] or count == 0:
                    raise RuntimeError(f"Executed {count}, expected {module['expectedTests']}")
                total += count
    if mode == 'test' and total == 0: raise RuntimeError('No scenarios executed')
    print(f'test: {total} executed scenarios' if mode == 'test' else f'run: {total} checked safety models' if mode == 'run' else 'All declared modules typechecked')

if __name__ == '__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('mode',choices=['typecheck','test','run','inventory']); mode=parser.parse_args().mode
    if mode=='inventory': print(json.dumps(inventory(),indent=2))
    else: run(mode)
