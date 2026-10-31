"""Real production control runner. Never invoked in the source-only repair round.
The negative copies only the production crate and trace harness; the mutant must
compile and fail the exact independent oracle assertion, not merely fail startup.
"""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[2]

def run():
    pin=json.loads((ROOT/'quint/production-trace.json').read_text())
    for name,digest in pin['sources'].items():
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=digest:
            raise RuntimeError('Production source pin changed: '+name+'; review and repin explicitly')
    command=['cargo','test','--manifest-path',str(ROOT/'quint/conformance/Cargo.toml'),'--features','production-trace','public_wal_append_sync_reopen_trace','--','--exact']
    # --exact needs the complete qualified test name.
    command[-1:]=['--exact']
    command[-3]='production_wal_trace::tests::public_wal_append_sync_reopen_trace'
    positive=subprocess.run(command,text=True,capture_output=True,timeout=900)
    print(positive.stdout+positive.stderr)
    if positive.returncode or '1 passed' not in positive.stdout:raise RuntimeError('Production positive trace did not execute')
    with tempfile.TemporaryDirectory(prefix='nucleus-wal-mutant-') as d:
        copy=Path(d)
        shutil.copytree(ROOT/'nucleus',copy/'nucleus',ignore=shutil.ignore_patterns('target','.git'))
        shutil.copytree(ROOT/'quint/conformance',copy/'quint/conformance',ignore=shutil.ignore_patterns('target'))
        source=copy/'nucleus/src/storage/wal.rs'
        text=source.read_text(); old='self.log_control(RECORD_ABORT, txn_id, None)'
        if text.count(old)!=2:raise RuntimeError('Mutation anchor changed')
        source.write_text(text.replace(old,'self.log_control(RECORD_COMMIT, txn_id, None)'))
        mutant=command.copy();mutant[3]=str(copy/'quint/conformance/Cargo.toml')
        result=subprocess.run(mutant,text=True,capture_output=True,timeout=900)
        output=result.stdout+result.stderr;print(output)
        if result.returncode==0 or 'test result: FAILED' not in output or 'assertion `left == right` failed' not in output or 'could not compile' in output:
            raise RuntimeError('Real production mutant did not fail the oracle assertion')
    print(json.dumps({'productionSources':pin['sources'],'positive':'executed','abortAsCommitMutant':'oracle-rejected'}))
if __name__=='__main__':run()
