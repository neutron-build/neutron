#!/usr/bin/env python3
"""Compile-iterate a Mojo file and add `raises` to defs the compiler points at.

Usage: python3 fix_raises.py <file.mojo>

Runs `mojo run -I src <file>` repeatedly; on each
"note: or mark surrounding function as 'raises'" it rewrites that def's
signature (handling multi-line parameter lists). Stops on the first
non-raises error (prints it) or when the file compiles/runs.
"""
import re
import subprocess
import sys
import os

CORE = os.path.dirname(os.path.abspath(__file__))


def run_mojo(path):
    env = dict(os.environ)
    env["CONDA_PREFIX"] = os.path.join(CORE, ".pixi/envs/default")
    activate = os.path.join(env["CONDA_PREFIX"], "etc/conda/activate.d/10-activate-max.sh")
    # activate script only sets MODULAR_HOME; replicate it directly
    env["MODULAR_HOME"] = os.path.join(env["CONDA_PREFIX"], "share/max")
    mojo = os.path.join(env["CONDA_PREFIX"], "bin/mojo")
    p = subprocess.run(
        [mojo, "run", "-I", "src", path],
        cwd=CORE, env=env, capture_output=True, text=True, timeout=300,
    )
    return p.returncode, p.stdout + p.stderr


def find_note_func(out):
    # pattern: file:line:col: note: or mark surrounding function as 'raises'
    # followed by quoted source of the def line
    m = re.search(
        r"note: or mark surrounding function as 'raises'\n\s*(?:.*\n)??\s*def\s+(\w+)",
        out,
    )
    return m.group(1) if m else None


def add_raises(src_text, func_name):
    lines = src_text.split("\n")
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith(f"def {func_name}("):
            # find end of parameter list starting from this line
            depth = 0
            j = i
            close_line, close_col = None, None
            started = False
            while j < len(lines):
                for k, ch in enumerate(lines[j]):
                    if ch == "#":  # rest of line is comment
                        break
                    if ch == "(":
                        depth += 1
                        started = True
                    elif ch == ")":
                        depth -= 1
                        if started and depth == 0:
                            close_line, close_col = j, k
                            break
                if close_line is not None:
                    break
                j += 1
            if close_line is None:
                return None, f"could not find closing paren for {func_name}"
            # find what follows the closing paren
            rest = lines[close_line][close_col + 1:]
            rest_stripped = rest.lstrip()
            if rest_stripped.startswith("raises"):
                return src_text, f"{func_name} already raises"
            if rest_stripped.startswith("->"):
                insert_at = close_col + 1 + (len(rest) - len(rest_stripped))
                newline = lines[close_line][:insert_at] + " raises" + lines[close_line][insert_at:]
                lines[close_line] = newline
                return "\n".join(lines), f"raises added to {func_name} (before ->) line {close_line+1}"
            # find the terminating ':' of the signature (may be after -> on same/later line)
            # scan forward from close paren for first ':' outside parens/brackets
            depth = 0
            jj = close_line
            kk = close_col + 1
            while jj < len(lines):
                while kk < len(lines[jj]):
                    ch = lines[jj][kk]
                    if ch in "([{":
                        depth += 1
                    elif ch in ")]}":
                        depth -= 1
                    elif ch == ":" and depth == 0:
                        lines[jj] = lines[jj][:kk] + " raises" + lines[jj][kk:]
                        return "\n".join(lines), f"raises added to {func_name} (before :) line {jj+1}"
                    kk += 1
                jj += 1
                kk = 0
            return None, f"could not find signature colon for {func_name}"
    return None, f"def {func_name} not found"


def main():
    path = sys.argv[1]
    for iteration in range(200):
        rc, out = run_mojo(path)
        if rc == 0:
            print(f"OK: {path} runs clean")
            return 0
        func = find_note_func(out)
        if not func:
            print("NON-RAISES ERROR (manual fix needed):")
            err = [l for l in out.split("\n") if "error:" in l]
            print("\n".join(err[:6]) if err else out[-3000:])
            return 1
        with open(path) as f:
            src_text = f.read()
        new_text, msg = add_raises(src_text, func)
        print(f"[{iteration}] {msg}")
        if new_text is None:
            print("FAILED:", msg)
            return 1
        if new_text == src_text:
            print("no change made; stopping")
            return 1
        with open(path, "w") as f:
            f.write(new_text)
    print("iteration limit hit")
    return 1


if __name__ == "__main__":
    sys.exit(main())
