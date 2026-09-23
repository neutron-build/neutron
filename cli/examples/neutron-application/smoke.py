"""Run the Neutron SDK example outside the repository under the coordinator.

Usage: python3 smoke.py /absolute/path/to/neutron
Requires Go 1.23+, Node 22+, npm, pnpm, and the TypeScript packages built
(`pnpm --filter "@neutron-build/cli..." build` in typescript/). The SDKs are
packed from this checkout, so the example runs against unreleased changes.
"""
import glob
import json
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

binary = str(Path(sys.argv[1]).resolve())
example = Path(__file__).resolve().parent
repo = example.parents[2]


def wait_for(pattern, log_path, process, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        text = log_path.read_text()
        match = re.search(pattern, text)
        if match:
            return match
        if process.poll() is not None:
            raise AssertionError(f"coordinator exited early:\n{text}")
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {pattern}:\n{log_path.read_text()}")


def closed(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return False
    except OSError:
        return True


def start(root, log_path):
    log = log_path.open("w")
    process = subprocess.Popen([binary, "dev"], cwd=root, stdout=log, stderr=subprocess.STDOUT)
    wait_for(r"\[web\] ready", log_path, process)
    text = log_path.read_text()
    ports = {name: int(re.search(rf"\[{name}\] port (\d+) \(assigned\)", text).group(1)) for name in ("api", "web")}
    return process, ports


def stop(process, ports, log_path):
    process.send_signal(signal.SIGINT)
    process.wait(timeout=60)
    deadline = time.monotonic() + 10
    while not all(closed(p) for p in ports.values()):
        assert time.monotonic() < deadline, f"orphaned service:\n{log_path.read_text()}"
        time.sleep(0.1)


with tempfile.TemporaryDirectory(prefix="neutron-sdk-example-") as directory:
    work = Path(directory)
    packs = work / "packs"
    packs.mkdir()
    for package in ("neutron", "neutron-cli"):
        subprocess.run(["pnpm", "pack", "--pack-destination", str(packs)], cwd=repo / "typescript/packages" / package, check=True, capture_output=True)
    root = work / "app"
    shutil.copytree(example, root, ignore=shutil.ignore_patterns("node_modules", ".neutron", "__pycache__", "smoke.py"))
    go_mod = root / "api/go.mod"
    go_mod.write_text(go_mod.read_text().replace("=> ../../../../go", f"=> {repo / 'go'}"))
    manifest = root / "web/package.json"
    package = json.loads(manifest.read_text())
    package["dependencies"]["@neutron-build/core"] = "file:" + glob.glob(str(packs / "neutron-build-core-*.tgz"))[0]
    package["dependencies"]["@neutron-build/cli"] = "file:" + glob.glob(str(packs / "neutron-build-cli-*.tgz"))[0]
    manifest.write_text(json.dumps(package, indent=2))
    subprocess.run(["npm", "install", "--no-audit", "--no-fund"], cwd=root / "web", check=True, capture_output=True)
    subprocess.run(["go", "mod", "download"], cwd=root / "api", check=True, capture_output=True)
    print("PASS: example installed outside the repository against packed SDKs", flush=True)

    log_path = work / "dev.log"
    process, ports = start(root, log_path)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{ports['web']}/", timeout=10) as response:
            page = response.read().decode()
        assert 'data-item="Hydrogen"' in page, page[:2000]
        print("PASS: web renders API data reached only through NEUTRON_SERVICE_API_URL", flush=True)

        request = urllib.request.Request(f"http://127.0.0.1:{ports['api']}/api/items", data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
        try:
            urllib.request.urlopen(request, timeout=5)
            raise AssertionError("invalid item accepted")
        except urllib.error.HTTPError as error:
            body = json.loads(error.read())
            assert error.code == 422 and error.headers.get_content_type() == "application/problem+json", (error.code, error.headers)
            assert body["errors"][0]["field"] == "name", body
        print("PASS: validation failure is RFC 7807 problem+json", flush=True)

        notes = re.findall(r"\[(\w+)\] contract: (.*)", log_path.read_text())
        assert not notes, f"contract deviations reported: {notes}"
        print("PASS: both services meet the checked framework contract", flush=True)

        second_log = work / "second.log"
        second_root = work / "app-copy"
        shutil.copytree(root, second_root, symlinks=True, ignore=shutil.ignore_patterns(".neutron"))
        second, second_ports = start(second_root, second_log)
        assert not set(ports.values()) & set(second_ports.values()), (ports, second_ports)
        stop(second, second_ports, second_log)
        print("PASS: two copies run at once on assigned ports", flush=True)

        result = {}
        def slow():
            with urllib.request.urlopen(f"http://127.0.0.1:{ports['api']}/api/slow?seconds=2", timeout=20) as response:
                result["status"] = response.status
        thread = threading.Thread(target=slow)
        thread.start()
        time.sleep(0.5)
        stop(process, ports, log_path)
        thread.join(timeout=20)
        assert result.get("status") == 200, (result, log_path.read_text())
        print("PASS: in-flight request completes during shutdown; no process left", flush=True)
    except BaseException:
        print(log_path.read_text(), file=sys.stderr)
        raise
    finally:
        if process.poll() is None:
            process.kill()
