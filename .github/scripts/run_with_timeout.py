"""Bound checker invocations and distinguish timeout from semantic rejection."""
import os
import signal
import subprocess
import sys

seconds = float(os.environ.get('NEUTRON_GATE_TIMEOUT_SECONDS', '120'))
if not 0 < seconds <= 120 or len(sys.argv) < 2:
    sys.exit('Invalid checker invocation or timeout')
process = subprocess.Popen(sys.argv[1:], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           start_new_session=True)
try:
    output, _ = process.communicate(timeout=seconds)
except subprocess.TimeoutExpired:
    os.killpg(process.pid, signal.SIGKILL)
    output, _ = process.communicate()
    sys.stdout.buffer.write(output)
    print('TOOL_TIMEOUT: checker did not complete', flush=True)
    sys.exit(124)
sys.stdout.buffer.write(output)
sys.exit(process.returncode)
