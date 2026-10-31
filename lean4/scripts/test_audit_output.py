import os
import subprocess
import tempfile
from pathlib import Path
import unittest
from audit_output import parse, summarize

def output(names):
    return '\n'.join('AXIOM THEOREM: '+n for n in names)+'\nAXIOM AUDIT OK: '+str(len(names))+' theorems, none depending on an unlisted axiom\n'
class AuditOutput(unittest.TestCase):
    def test_zero_root_and_deduplication(self):
        with tempfile.TemporaryDirectory() as d:
            for m,names in [('Nucleus.Empty',[]),('Nucleus.One',['Nucleus.One.t']),('Nucleus.Two',['Nucleus.One.t','Nucleus.Two.t'])]:
                (Path(d)/(m+'.log')).write_text(output(names))
            result=summarize(d)
            self.assertEqual(result['importedReferences'],3)
            self.assertEqual(len(result['uniqueTheorems']),2)
            self.assertEqual(result['modules']['Nucleus.Empty']['declaredTheorems'],0)
    def test_zero_aggregate(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d)/'Nucleus.Empty.log').write_text(output([]))
            with self.assertRaises(ValueError): summarize(d)
    def test_bad_transports(self):
        good=output(['Nucleus.One.t'])
        for bad in ['', 'AXIOM AUDIT OK: nope',good+good,good.replace('1 theorems','2 theorems'),good+'error: Nucleus.One.t depends on unlisted axiom bad',good.replace('AXIOM AUDIT OK:', 'AXIOM AUDIT OK: garbage')]:
            with self.subTest(output=bad),self.assertRaises(ValueError):parse(bad)
    def test_axiom_shell_transport(self):
        stub = """#!/usr/bin/env python3
import os,sys
from pathlib import Path
if sys.argv[1:] == ['build']:sys.exit(0)
mode=os.environ['AUDIT_STUB']
module=Path(sys.argv[-1]).read_text().splitlines()[0].split()[1]
if mode=='missing':sys.exit(0)
count=0 if mode=='zero' or module=='Nucleus.Helpers.Tactics' else 1
if count:print('AXIOM THEOREM: '+module+'.fixture')
marker='AXIOM AUDIT OK: '+str(count)+' theorems, none depending on an unlisted axiom'
print(marker)
if mode=='multiple':print(marker)
if mode=='malformed':print('AXIOM AUDIT OK: garbage')
if mode=='unlisted':print('error: theorem depends on unlisted axiom bad')
"""
        with tempfile.TemporaryDirectory() as d:
            binary=Path(d)/'lake';binary.write_text(stub);binary.chmod(0o755)
            for mode in ['pass','missing','zero','multiple','malformed','unlisted']:
                result=subprocess.run(['bash',str(Path(__file__).parent/'axioms.sh')],
                    env={**os.environ,'PATH':d+os.pathsep+os.environ['PATH'],'AUDIT_STUB':mode},
                    capture_output=True,text=True,timeout=30)
                with self.subTest(mode=mode):self.assertEqual(result.returncode==0,mode=='pass',result.stdout+result.stderr)
if __name__=='__main__':unittest.main()
