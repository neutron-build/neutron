"""Prepare and run actual Lean model mutations after the source freeze.
None of these controls is certified by a transport stub or missing tool.
"""
from pathlib import Path
import shutil
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[1]

MUTANTS = [
 ('unrelated_lookup','Nucleus/Aeneas/Btree.lean',
  'entries.filter (fun e => e.key != k)','[]'),
 ('vote_overwrite','Nucleus/Spec/RaftSpec.lean',
  '(free : s.votes t v = none) : Step s (grant s t v c)',
  '(free : True) : Step s (grant s t v c)'),
 ('uncommitted_table_recovery','Nucleus/Spec/WalSpec.lean',
  'wal.isCommitted r.txId &&','true &&'),
]
def run():
    if not shutil.which('lake'):raise RuntimeError('Lean unavailable; no model control checked')
    subprocess.run(['bash',str(ROOT/'scripts/canary.sh')],check=True,timeout=180,cwd=ROOT)
    for name,relative,old,new in MUTANTS:
        with tempfile.TemporaryDirectory(prefix='lean-model-mutant-') as d:
            target=Path(d)/'Nucleus'
            shutil.copytree(ROOT/'Nucleus',target,ignore=shutil.ignore_patterns('.lake'))
            source=target/relative;text=source.read_text()
            if old not in text:raise ValueError('Mutation anchor changed')
            source.write_text(text.replace(old,new))
            result=subprocess.run(['lake','build'],cwd=target,capture_output=True,text=True,timeout=180)
            output=result.stdout+result.stderr;print(name+'\n'+output)
            if result.returncode==0 or 'unsolved goals' not in output and 'type mismatch' not in output:
                raise RuntimeError('Model mutant did not reject at a proof obligation')
            if any(bad in output.lower() for bad in ['unknown module','unknown identifier','unexpected token','failed to download']):
                raise RuntimeError('Unrelated Lean failure')
if __name__=='__main__':run()
