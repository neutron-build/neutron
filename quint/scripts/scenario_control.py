"""Top-level scenario propagation control, executed only with the actual tool."""
from pathlib import Path
import shutil
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[1]
def run():
    if not shutil.which('quint'):raise RuntimeError('Quint unavailable; no scenario mutation executed')
    with tempfile.TemporaryDirectory(prefix='quint-scenario-mutant-') as d:
        copy=Path(d)/'quint';shutil.copytree(ROOT,copy,ignore=shutil.ignore_patterns('target'))
        source=copy/'conformance/predicate_history_test.qnt';text=source.read_text()
        old='run conflicting_interval_serial_history_test = serializable(serial,rows,scans)'
        if text.count(old)!=1:raise ValueError('Scenario mutation anchor changed')
        source.write_text(text.replace(old,'run conflicting_interval_serial_history_test = not(serializable(serial,rows,scans))'))
        result=subprocess.run(['python3',str(copy/'scripts/manifest.py'),'test'],capture_output=True,text=True,timeout=600)
        output=result.stdout+result.stderr;print(output)
        if result.returncode==0 or 'conflicting_interval_serial_history_test' not in output or 'failed' not in output.lower():
            raise RuntimeError('Top-level scenario failure did not propagate')
        if any(x in output for x in ['QNT','Traceback (most recent call last):\n  File']) and 'Quint invocation failed' not in output:
            raise RuntimeError('Unrelated scenario-control failure')
if __name__=='__main__':run()
