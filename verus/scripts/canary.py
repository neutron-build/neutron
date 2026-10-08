"""Real Verus semantic controls; transport tests do not substitute for these."""
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from verify import verify, ROOT

def controls():
    verify()
    binary=shutil.which('verus')
    text=(ROOT/'checked/model_obligations.rs').read_text()
    mutants={'conflicting_commits':('if first_committed && first_key == second_key { false }','if first_committed && first_key == second_key { true }'),
        'early_mismatch':('diff = diff | (l ^ r);','if l != r { return (false,index+1); }\n        diff = diff | (l ^ r);')}
    with tempfile.TemporaryDirectory() as d:
        for name,(old,new) in mutants.items():
            if text.count(old)!=1:raise ValueError('Mutation anchor changed')
            path=Path(d)/(name+'.rs');path.write_text(text.replace(old,new))
            result=subprocess.run([binary,str(path)],capture_output=True,text=True,timeout=120)
            output=result.stdout+result.stderr;print(output)
            summaries=re.findall(r'verification results::\s*(\d+) verified,\s*(\d+) errors',output)
            if result.returncode==0 or len(summaries)!=1 or int(summaries[0][1])==0 or not re.search(r'postcondition not satisfied|assertion failed',output):
                raise RuntimeError('Mutant did not fail the intended proof obligation: '+name)
            if re.search(r'cannot find|unexpected token|failed to parse|solver.*unavailable',output,re.I):
                raise RuntimeError('Unrelated mutant failure: '+name)
    print(json.dumps({'semanticControls':'real-verus-rejected','mutants':list(mutants)}))
if __name__=='__main__':controls()
