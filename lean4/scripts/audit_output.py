"""Validate axiom-audit transport; zero-theorem roots are legal, zero aggregate is not."""
import json
from pathlib import Path
import re
import sys

MARKER = re.compile(r'^AXIOM AUDIT OK: ([0-9]+) theorems, none depending on an unlisted axiom$')
THEOREM = re.compile(r'^AXIOM THEOREM: (Nucleus\.[A-Za-z0-9_.]+)$')

def parse(output):
    # Lean logInfo includes file/line prefixes; strip only its diagnostic prefix.
    lines = [re.sub(r'^(?:.*?:\d+:\d+: )?(?:info: )?', '', line) for line in output.splitlines()]
    markers = [line for line in lines if 'AXIOM AUDIT OK' in line]
    if len(markers) != 1 or not MARKER.fullmatch(markers[0]):
        raise ValueError('Expected exactly one well-formed axiom audit success marker')
    if re.search(r'unlisted axiom(?!$)|AXIOM AUDIT FAILED|\berror:', output):
        # The normal marker contains "an unlisted axiom" at end of its line.
        diagnostics = [line for line in lines if not MARKER.fullmatch(line)]
        if any('unlisted axiom' in line or 'AXIOM AUDIT FAILED' in line or 'error:' in line for line in diagnostics):
            raise ValueError('Axiom audit emitted a failure diagnostic')
    names = [THEOREM.fullmatch(line).group(1) for line in lines if THEOREM.fullmatch(line)]
    count = int(MARKER.fullmatch(markers[0]).group(1))
    if len(names) != count or len(set(names)) != count:
        raise ValueError('Theorem inventory disagrees with audit marker')
    return names

def summarize(directory):
    modules = {}
    unique = set()
    references = 0
    for path in sorted(Path(directory).glob('Nucleus.*.log')):
        names = parse(path.read_text())
        module = path.stem
        modules[module] = {'importedReferences': len(names), 'declaredTheorems': sum(n.startswith(module + '.') for n in names)}
        references += len(names)
        unique.update(names)
    if not modules or references == 0 or not unique:
        raise ValueError('Axiom audit checked no theorems')
    return {'modules': modules, 'importedReferences': references, 'uniqueTheorems': sorted(unique)}

if __name__ == '__main__':
    print(json.dumps(summarize(sys.argv[1]), indent=2))
