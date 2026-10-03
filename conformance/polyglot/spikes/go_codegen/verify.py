#!/usr/bin/env python3
"""Execute this isolated API spike; no databases or production SDKs are used."""
import hashlib
import pathlib
import subprocess
import sys

root = pathlib.Path(__file__).resolve().parent

def run(args):
    result = subprocess.run(args, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("$", " ".join(args), "exit", result.returncode)
    print(result.stdout, end="")
    return result

for _ in range(2):
    result = run(["go", "generate", "./model"])
    if result.returncode:
        sys.exit(result.returncode)
    current = hashlib.sha256((root / "model/model_gen.go").read_bytes()).hexdigest()
    if _ == 0:
        expected = current
    elif current != expected:
        sys.exit("non-deterministic generated source")

result = run(["go", "test", "./..."])
if result.returncode:
    sys.exit(result.returncode)
result = run(["go", "test", "./testdata/positive"])
if result.returncode:
    sys.exit(result.returncode)
for name, diagnostic in [
    ("wrong_predicate", "cannot use"),
    ("wrong_projection", "cannot use"),
    ("wrong_write", "cannot use"),
    ("null_nonnullable", "cannot use"),
]:
    result = run(["go", "test", "./testdata/" + name])
    if result.returncode == 0 or diagnostic not in result.stdout:
        sys.exit("negative consumer did not fail with expected type error: " + name)
print("PASS: deterministic generator, runtime contracts, four negative compile consumers")
