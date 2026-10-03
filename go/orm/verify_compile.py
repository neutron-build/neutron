#!/usr/bin/env python3
"""Check positive/negative Go consumer types; run from the Go module root."""
import pathlib
import subprocess
import sys

root = pathlib.Path(__file__).resolve().parent.parent

def run(package):
    command = ["go", "test", package]
    result = subprocess.run(command, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("$", " ".join(command), "exit", result.returncode)
    print(result.stdout, end="")
    return result

for package in ["./orm", "./orm/testdata/positive"]:
    result = run(package)
    if result.returncode:
        sys.exit(result.returncode)

for name, expected in [
    ("wrong_predicate", "as int64 value"),
    ("wrong_write", "does not match inferred type"),
    ("wrong_projection", "as []int value"),
    ("wrong_model", "as orm.Predicate[Project] value"),
]:
    result = run("./orm/testdata/" + name)
    source = root / "orm/testdata" / name / "main.go"
    needle = "result, _ =" if name == "wrong_projection" else "var invalid"
    line = next(i for i, text in enumerate(source.read_text().splitlines(), 1) if needle in text)
    location = f"orm/testdata/{name}/main.go:{line}:"
    if result.returncode == 0 or expected not in result.stdout or location not in result.stdout:
        sys.exit("intended type-check failure absent: " + name)
print("PASS: public Go ORM core positive and four negative compile consumers")
