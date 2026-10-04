#!/usr/bin/env python3
"""Run cancellation_authority.py against an engine this script starts itself.

Linux only. Starts the exact Nucleus binary on a free loopback port with a
temporary data directory and a random bootstrap password, proves the running
process image is that binary (SHA256 of /proc/<pid>/exe equals the file hash),
runs the finite authority facts, then stops the engine and removes its data.
The password exists only in this process environment and is never printed or
written to the report. No timing, workload or parity claim is made.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    digest = sha256(binary)
    data = Path(tempfile.mkdtemp(prefix="neutron-np02-"))
    password = secrets.token_urlsafe(32)
    port, resp_port = free_port(), free_port()
    env = {key: value for key, value in os.environ.items() if not key.startswith(("NUCLEUS_", "NEUTRON_"))}
    env.update(NUCLEUS_ALLOW_INSECURE_AUTH="1", NUCLEUS_PASSWORD=password)
    server = subprocess.Popen(
        [str(binary), "start", "--host", "127.0.0.1", "--port", str(port), "--data", str(data / "db"),
         "--resp-port", str(resp_port), "--no-tls"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    report: dict[str, object] = {"status": "fail", "binarySha256": digest, "runnerSha256": sha256(Path(__file__))}
    code = 1
    try:
        deadline = time.monotonic() + 90
        while True:
            if server.poll() is not None:
                raise RuntimeError("engine exited before accepting connections")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError("engine did not accept connections before the deadline")
                time.sleep(0.5)
        running = sha256(Path(f"/proc/{server.pid}/exe"))
        report["runningImageSha256"] = running
        if running != digest:
            raise RuntimeError("running engine image does not match the supplied binary")
        child_env = dict(os.environ)
        child_env["NEUTRON_NP02_NUCLEUS_URL"] = f"postgresql://nucleus:{password}@127.0.0.1:{port}/nucleus?sslmode=disable"
        facts = data / "facts.json"
        result = subprocess.run(
            [sys.executable, "-I", str(HERE / "cancellation_authority.py"), "--engine", "nucleus",
             "--admin-url-env", "NEUTRON_NP02_NUCLEUS_URL", "--binary-sha256", digest, "--report", str(facts)],
            env=child_env, capture_output=True, text=True,
        )
        report["authorityExit"] = result.returncode
        if facts.exists():
            report["authority"] = json.loads(facts.read_text())
        # Child output can echo connection details; keep only a bounded, redacted tail.
        tail = (result.stdout + result.stderr).replace(password, "[redacted]")[-1500:]
        report["authorityLogTail"] = tail
        if result.returncode == 0 and report.get("authority", {}).get("status") == "pass":
            report["status"] = "pass"
            code = 0
    except Exception as error:
        report["runnerFailure"] = type(error).__name__ + ": " + str(error).replace(password, "[redacted]")
    finally:
        try:
            os.killpg(server.pid, signal.SIGTERM)
            server.wait(timeout=30)
        except Exception:
            try:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait(timeout=10)
            except Exception:
                report["engineStopFailed"] = True
                report["status"] = "fail"
                code = 1
        shutil.rmtree(data, ignore_errors=True)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "binarySha256": digest}))
    return code


if __name__ == "__main__":
    sys.exit(main())
