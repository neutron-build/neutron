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
for name, target_type in [
    ("wrong_predicate", "as int64 value"),
    ("wrong_projection", "as []int value"),
    ("wrong_write", "as typed.Optional[bool] value"),
    ("null_nonnullable", "as typed.Optional[string] value"),
]:
    result = run(["go", "test", "./testdata/" + name])
    source = root / "testdata" / name / "main.go"
    line = next(i for i, text in enumerate(source.read_text().splitlines(), 1) if "var invalid" in text)
    location = f"testdata/{name}/main.go:{line}:"
    if result.returncode == 0 or "cannot use" not in result.stdout or target_type not in result.stdout or location not in result.stdout:
        sys.exit("negative consumer did not fail with expected type error: " + name)
print("PASS: deterministic generator, runtime contracts, four negative compile consumers")
