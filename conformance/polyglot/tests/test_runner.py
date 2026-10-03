import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from protocol import PROTOCOL, redact, verify_artifacts, validate_request
from runner import EXPECTED, invoke, run, validate_manifest

MANIFEST = {'protocol': PROTOCOL, 'cases': [{'id': 'scalar-extremes', 'kind': 'oracle-self-check'}]}

class RunnerTests(unittest.TestCase):
    def test_zero_cases_fail(self):
        with self.assertRaises(ValueError): validate_manifest({'protocol': PROTOCOL, 'cases': []})
    def test_required_missing_database_fails(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(ValueError): run(MANIFEST, {}, True, 1)
    def test_optional_missing_is_not_pass(self):
        with patch.dict(os.environ, {}, clear=True): self.assertEqual(run(MANIFEST, {}, False, 1)['status'], 'not-run')
    def test_artifact_hash_and_escape(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); (root/'artifact').write_bytes(b'actual')
            good = {'files': [{'path':'artifact','sha256':hashlib.sha256(b'actual').hexdigest()}]}
            self.assertIn('artifact', verify_artifacts(good, root))
            with self.assertRaises(ValueError): verify_artifacts({'files':[{'path':'artifact','sha256':'0'*64}]},root)
            with self.assertRaises(ValueError): verify_artifacts({'files':[{'path':'../missing','sha256':'0'*64}]},root)
    def test_redaction(self):
        text = redact('postgresql://user:secret@localhost/db password=secret', 'secret')
        self.assertNotIn('secret', text); self.assertNotIn('user', text)
    def test_timeout(self):
        with self.assertRaisesRegex(ValueError, 'timeout'): invoke([sys.executable,'-c','import time; time.sleep(5)'], {}, .01)
    def test_unsupported_not_pass(self):
        command = [sys.executable,'-c', 'print(\'{"protocol":"polyglot-conformance-v1","case_id":"scalar-extremes","status":"unsupported"}\')']
        with self.assertRaises(ValueError): invoke(command, {'case_id':'scalar-extremes'}, 1)
    def test_swapped_scope_or_profile_fails(self):
        request = {'case_id':'scalar-extremes','profile':'postgres-direct','schema_scope':'neutron_polyglot_'+'a'*32}
        for changed in [{'profile':'other'}, {'schema_scope':'neutron_polyglot_'+'b'*32}]:
            response = {'protocol':PROTOCOL, 'status':'pass', **request, **changed}
            command = [sys.executable, '-c', 'print('+repr(json.dumps(response))+')']
            with self.assertRaisesRegex(ValueError, 'identity mismatch'): invoke(command, request, 1)
    def test_native_mismatch_cleans(self):
        actions=[]
        def fake(command, request, timeout):
            actions.append(request['action']); return {'rows':[['corrupt']]}
        with patch.dict(os.environ, {'NEUTRON_TEST_DATABASE_URL':'private'}), patch('runner.invoke',side_effect=fake), self.assertRaises(ValueError): run(MANIFEST, {}, True, 1)
        self.assertEqual(actions,['setup','observe','cleanup'])
    def test_ambiguous_setup_cleans(self):
        actions=[]
        def fake(command, request, timeout):
            actions.append(request['action'])
            if request['action']=='setup': raise ValueError('timeout')
            return {'rows':[]}
        with patch.dict(os.environ, {'NEUTRON_TEST_DATABASE_URL':'private'}), patch('runner.invoke',side_effect=fake), self.assertRaises(ValueError): run(MANIFEST, {}, True, 1)
        self.assertEqual(actions,['setup','cleanup'])
    def test_self_check_labeled(self):
        with patch.dict(os.environ, {'NEUTRON_TEST_DATABASE_URL':'private'}), patch('runner.invoke',return_value={'rows':EXPECTED}):
            self.assertEqual(run(MANIFEST, {}, True, 1)['kind'],'oracle-self-check')
    def test_unsafe_scope_refuses(self):
        with self.assertRaises(ValueError): validate_request({'protocol':PROTOCOL,'case_id':'scalar-extremes','profile':'postgres-direct','schema_scope':'public','action':'cleanup'})

if __name__ == '__main__': unittest.main()
