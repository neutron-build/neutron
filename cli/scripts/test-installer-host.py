#!/usr/bin/env python3
"""Build and verify the real Caddy installer image locally, without deployment."""
import argparse
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid

parser = argparse.ArgumentParser()
parser.add_argument("--engine", default="docker", choices=["docker", "podman"])
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
host = root / "cli/installer-host"
name = "neutron-installer-test-" + uuid.uuid4().hex[:10]
image = "localhost/" + name + ":test"
engine = args.engine
try:
    subprocess.run(["sh", str(host / "prepare.sh")], check=True)
    subprocess.run([engine, "build", "-t", image, str(host)], check=True)
    subprocess.run([engine, "run", "-d", "--name", name, "-p", "127.0.0.1::80", image], check=True)
    address = subprocess.check_output([engine, "port", name, "80/tcp"], text=True).strip().splitlines()[0]
    base = "http://" + address
    for attempt in range(30):
        try:
            with urllib.request.urlopen(base + "/", timeout=2) as response:
                body = response.read()
            break
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.2)
    else:
        raise RuntimeError("installer container did not become reachable")
    expected = (root / "scripts/install.sh").read_bytes()
    assert body == expected, "root response differs from canonical installer"
    with urllib.request.urlopen(base + "/install.sh", timeout=2) as response:
        assert response.read() == expected, "install.sh response differs"
        assert response.headers.get_content_type() == "text/plain"
        assert response.headers.get("Cache-Control") == "no-cache"
        assert response.headers.get("X-Content-Type-Options") == "nosniff"
    try:
        urllib.request.urlopen(base + "/missing", timeout=2)
    except urllib.error.HTTPError as error:
        assert error.code == 404
    else:
        raise AssertionError("unknown path must return 404")
    print("PASS: real installer container, both paths exact, required headers, unknown path 404")
finally:
    subprocess.run([engine, "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run([engine, "rmi", image], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
