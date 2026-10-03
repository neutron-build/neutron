"""Dependency-free envelope checks shared by the runner and native oracle."""
from __future__ import annotations
import hashlib
import re
from pathlib import Path

PROTOCOL = 'polyglot-conformance-v1'
SCOPE = re.compile(r'^neutron_polyglot_[0-9a-f]{32}$')
DIGEST = re.compile(r'^[0-9a-f]{64}$')

def redact(text: str, secret: str = '') -> str:
    if secret:
        text = text.replace(secret, '[REDACTED]')
    text = re.sub(r'(?i)(?:postgres(?:ql)?://)[^\s"\']+', '[REDACTED_DATABASE_URL]', text)
    text = re.sub(r'(?i)(password\s*[=:]\s*)[^\s,;]+', r'\1[REDACTED]', text)
    return text

def validate_request(request: dict) -> None:
    if not isinstance(request, dict) or request.get('protocol') != PROTOCOL:
        raise ValueError('unsupported envelope protocol')
    if not isinstance(request.get('case_id'), str) or not request['case_id']:
        raise ValueError('case_id required')
    if not SCOPE.fullmatch(request.get('schema_scope', '')):
        raise ValueError('invalid runner-owned schema scope')
    if request.get('profile') != 'postgres-direct':
        raise ValueError('unsupported oracle profile')
    if request.get('action') not in {'setup', 'observe', 'cleanup'}:
        raise ValueError('unsupported oracle action')

def verify_artifacts(manifest: dict, root: Path) -> dict:
    files = manifest.get('files')
    if not isinstance(files, list) or not files:
        raise ValueError('artifact manifest needs nonempty files')
    result = {}
    for item in files:
        name, expected = item['path'], item['sha256']
        if not isinstance(name, str) or not DIGEST.fullmatch(expected):
            raise ValueError('invalid artifact identity')
        path = (root / name).resolve()
        if not path.is_relative_to(root.resolve()) or not path.is_file():
            raise ValueError('artifact escapes root or is missing')
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f'artifact hash mismatch: {name}')
        if name in result:
            raise ValueError('duplicate artifact path')
        result[name] = actual
    return result
