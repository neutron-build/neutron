"""Gate transport regressions: unrelated tool failures must never certify rejection."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
ROOT=Path(__file__).resolve().parents[2]
STUB='''#!/usr/bin/env python3
import os,sys
name=os.path.basename(sys.argv[0]); mode=os.environ.get('GATE_STUB','pass')
args=' '.join(sys.argv[1:])
if name=='verus':
 if '--version' in args: print('Verus '+('wrong' if mode=='version' else '0.2026.10.04.426d8b0'));sys.exit(0)
 if mode=='multiple': print('verification results:: 6 verified, 0 errors\\nverification results:: 6 verified, 0 errors');sys.exit(0)
 if mode=='malformed': print('verification results:: nope');sys.exit(0)
 if mode=='errors': print('verification results:: 6 verified, 1 errors');sys.exit(0)
 if mode=='zero': print('verification results:: 0 verified, 0 errors');sys.exit(0)
 if mode=='startup': print('solver unavailable');sys.exit(1)
 print('verification results:: 6 verified, 0 errors');sys.exit(0)
negative='Negative.lean' in args or '--invariant=bad' in args
if mode=='startup': print('startup failed');sys.exit(2)
if negative:
 if mode=='accept':sys.exit(0)
 if mode=='import': print('unknown module; QNT404 import not found');sys.exit(1)
 if mode=='timeout':
  import time
  print("error: tactic 'assumption' failed\\n⊢ False; Invariant violated",flush=True);time.sleep(2)
 if mode=='syntax': print('unexpected token; parsing failed');sys.exit(1)
 if name=='lake': print("error: tactic 'assumption' failed\\n⊢ False")
 else: print('error: Invariant violated')
 sys.exit(1)
if name=='lake': print('POSITIVE_CONTROL_PASS audited=6')
elif 'test' in args: print('1 passing')
else: print('No violation')
'''
class FormalGates(unittest.TestCase):
    def run_gate(self, name, mode):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)
            for tool in ('quint','lake','verus'):
                binary=path/tool;binary.write_text(STUB);binary.chmod(0o755)
            env={**os.environ,'PATH':str(path)+os.pathsep+os.environ['PATH'],'GATE_STUB':mode,'NEUTRON_GATE_TIMEOUT_SECONDS':'0.5'}
            return subprocess.run(['bash',str(ROOT/name/'scripts'/('verify.sh' if name=='verus' else 'canary.sh'))],env=env,capture_output=True,text=True,timeout=15)
    def test_controls_pass(self):
        for name in ('lean4','quint','verus'):
            with self.subTest(name=name): self.assertEqual(self.run_gate(name,'pass').returncode,0)
    def test_startup_failure(self):
        for name in ('lean4','quint','verus'):
            with self.subTest(name=name): self.assertNotEqual(self.run_gate(name,'startup').returncode,0)
    def test_unrelated_syntax_failure(self):
        for name in ('lean4','quint'):
            with self.subTest(name=name): self.assertNotEqual(self.run_gate(name,'syntax').returncode,0)
    def test_missing_import_failure(self):
        for name in ('lean4','quint'):
            with self.subTest(name=name): self.assertNotEqual(self.run_gate(name,'import').returncode,0)
    def test_timeout_after_semantic_error_is_not_rejection(self):
        for name in ('lean4','quint'):
            with self.subTest(name=name): self.assertNotEqual(self.run_gate(name,'timeout').returncode,0)
    def test_false_control_accepted(self):
        for name in ('lean4','quint'):
            with self.subTest(name=name): self.assertNotEqual(self.run_gate(name,'accept').returncode,0)
    def test_zero_verifications(self): self.assertNotEqual(self.run_gate('verus','zero').returncode,0)
    def test_verus_version_and_summaries(self):
        for mode in ['version','multiple','malformed','errors']:
            with self.subTest(mode=mode):self.assertNotEqual(self.run_gate('verus',mode).returncode,0)
if __name__=='__main__':unittest.main()
