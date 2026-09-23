"""Exercise the compiled CLI and native Go/TypeScript processes outside the repo.

Usage: python3 smoke.py /absolute/path/to/neutron
Requires Python 3.11+, Go 1.23+, Node 22.6+, and macOS or Linux.
"""
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request


def available_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for(predicate, process, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"CLI exited early: {process.returncode}")
        try:
            result = predicate()
            if result:
                return result
        except (OSError, ValueError):
            pass
        time.sleep(0.1)
    raise AssertionError("Timed out")


def request(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1) as response:
        return json.load(response)


def closed(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.1):
            return False
    except OSError:
        return True


binary = str(Path(sys.argv[1]).resolve())
source = Path(__file__).resolve().parent
with tempfile.TemporaryDirectory(prefix="neutron-app-smoke-") as directory:
    root = Path(directory) / "application"
    shutil.copytree(source, root, ignore=shutil.ignore_patterns(".neutron", "__pycache__"))
    api_port, web_port = available_port(), available_port()
    while web_port == api_port:
        web_port = available_port()
    manifest = root / "neutron.toml"
    manifest.write_text(manifest.read_text().replace("4301", str(api_port)).replace("4300", str(web_port)))
    plan = subprocess.run([binary, "project", "plan", "--json", "--service", "web"], cwd=root / "web", check=True, capture_output=True, text=True)
    assert [c["name"] for c in json.loads(plan.stdout)["services"]] == ["api", "web"]
    assert not (root / ".neutron").exists(), "planning changed project files"
    print("PASS: planning from a subdirectory without execution", flush=True)
    for scenario in ["interrupt", "api_failure", "hangup", "coordinator_killed"]:
        log_path = Path(directory) / f"{scenario}.log"
        with log_path.open("w") as log:
            process = subprocess.Popen([binary, "dev", "--service", "web"], cwd=root / "web", stdout=log, stderr=subprocess.STDOUT)
            try:
                data = wait_for(lambda: request(web_port), process)
                assert data == {"from": "TypeScript", "api": "Hello from Go"}, data
                text = log_path.read_text()
                assert text.index("[api] ready") < text.index("[web] running")
                print("PASS: native Go API + TypeScript service, ordered readiness", flush=True)
                if scenario == "interrupt":
                    # Confirm Node's own watcher remains responsible for hot reload.
                    web_source = root / "web/server.ts"
                    web_source.write_text(web_source.read_text().replace('from: "TypeScript"', 'from: "TypeScript reloaded"'))
                    wait_for(lambda: request(web_port).get("from") == "TypeScript reloaded", process)
                    print("PASS: native TypeScript watch/reload", flush=True)
                    process.send_signal(signal.SIGINT)
                elif scenario == "hangup":
                    # A closed terminal delivers SIGHUP.
                    process.send_signal(signal.SIGHUP)
                elif scenario == "coordinator_killed":
                    # No cleanup code runs; the per-service lifeline must fire.
                    process.kill()
                else:
                    pid = int(re.search(r"api pid=(\d+)", text).group(1))
                    os.kill(pid, signal.SIGKILL)
                assert process.wait(timeout=12) != 0
                deadline = time.monotonic() + 10
                while not (closed(api_port) and closed(web_port)):
                    assert time.monotonic() < deadline, "orphaned service"
                    time.sleep(0.1)
                print(f"PASS: {scenario} stops both process trees", flush=True)
            except BaseException:
                print(log_path.read_text(), file=sys.stderr)
                raise
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    process.wait(timeout=12)
        web_source = root / "web/server.ts"
        web_source.write_text(web_source.read_text().replace('from: "TypeScript reloaded"', 'from: "TypeScript"'))
