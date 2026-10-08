#!/usr/bin/env python3
"""Run one nucleus v2 task card on GLM (opencode headless) in an isolated worktree, gate it, log it.

Usage: nucleus/v2/tools/run_card.py nucleus/v2/cards/<id>.local.md [--base <branch>] [--effort high|max]
       [--attempts 3] [--model zai-coding-plan/glm-5.3] [--keep]

Flow: worktree on branch card/v2-<id> from --base -> copy card + worker prompt in (they are local-only)
-> opencode v2 `run --standalone`, cwd = worktree, stdin closed (denied: rm, sudo, push, reset, checkout, curl, wget, brew) -> scope guard (only the
card's "Touch only" globs) -> acceptance commands -> up to N attempts with failure output fed back ->
RUNLOG row. Every attempt first checks free disk. Nothing is merged here; the frontier reviews the branch.
"""
import argparse
import fnmatch
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time

V2 = pathlib.Path(__file__).resolve().parent.parent
ROOT = V2.parent.parent
CARDS = V2 / "cards"
WT_ROOT = pathlib.Path("/tmp/nv2-cards")
TARGET_DIR = pathlib.Path("/tmp/nv2-target")
MIN_FREE_GB = 10
PROTECTED = ["nucleus/v2/docs/", "nucleus/v2/cards/", "nucleus/v2/tools/", "nucleus/src/"]
OPENCODE_CONFIG = {
    "$schema": "https://opencode.ai/config.json",
    "permission": {
        "edit": "allow",
        "webfetch": "deny",
        "bash": {
            "*": "allow",
            "rm *": "deny",
            "sudo *": "deny",
            "git push*": "deny",
            "git reset*": "deny",
            "git checkout*": "deny",
            "git stash*": "deny",
            "curl *": "deny",
            "wget *": "deny",
            "brew *": "deny",
        },
    },
}


def run(cmd, cwd, env=None, timeout=None):
    p = subprocess.run(cmd, cwd=cwd, env=env, shell=isinstance(cmd, str), capture_output=True, text=True, timeout=timeout)
    return p.returncode, (p.stdout + p.stderr)


def free_gb(path):
    return shutil.disk_usage(path).free / 1e9


def parse_card(path):
    text = path.read_text()
    title = re.search(r"^# CARD ([^:]+):", text, re.M)
    card_id = title.group(1).strip() if title else path.stem.split(".")[0]

    def section(name):
        m = re.search(r"^## %s\n(.*?)(?=^## |\Z)" % re.escape(name), text, re.M | re.S)
        return m.group(1) if m else ""

    touch = [m.strip().strip("`") for m in re.findall(r"^- (.+)$", section("Touch only"), re.M)]
    accept = re.findall(r"^- `([^`]+)`", section("Acceptance"), re.M)
    return card_id, touch, accept


def in_scope(path, touch):
    return any(fnmatch.fnmatch(path, pat) or path.startswith(pat.rstrip("*")) for pat in touch)


def changed_files(wt, base):
    _, committed = run(["git", "diff", "--name-only", f"{base}...HEAD"], wt)
    _, out = run("git status --porcelain --untracked-files=all", wt)
    files = set(committed.split())
    for line in out.splitlines():
        f = line[3:].strip().strip('"')
        if " -> " in f:
            f = f.split(" -> ")[1]
        files.add(f)
    return sorted(f for f in files if f and f not in ("opencode.json",) and not f.endswith("Cargo.lock"))


def scope_violations(wt, base, touch):
    bad = []
    for f in changed_files(wt, base):
        if any(f.startswith(p) for p in PROTECTED):
            bad.append(f + " (protected)")
        elif not in_scope(f, touch):
            bad.append(f + " (outside Touch only)")
    return bad


def parse_tokens(log_path):
    tot = {"input": 0, "output": 0, "reasoning": 0}
    session = None
    for line in log_path.read_text().splitlines():
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        session = session or ev.get("sessionID")
        part = ev.get("part") or {}
        toks = part.get("tokens")
        if ev.get("type") == "step_finish" and isinstance(toks, dict):
            for k in tot:
                tot[k] += int(toks.get(k, 0) or 0)
    return tot, session


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("card")
    ap.add_argument("--base", default="nucleus/v2-scaffold")
    ap.add_argument("--effort", default="high")
    ap.add_argument("--attempts", type=int, default=3)
    ap.add_argument("--model", default="zai-coding-plan/glm-5.3")
    ap.add_argument("--keep", action="store_true", help="reuse an existing worktree and branch")
    args = ap.parse_args()

    card_path = pathlib.Path(args.card)
    card_path = card_path if card_path.is_absolute() else (pathlib.Path.cwd() / card_path).resolve()
    card_id, touch, accept = parse_card(card_path)
    if not touch or not accept:
        sys.exit(f"card {card_id}: needs Touch only and Acceptance sections")

    wt = WT_ROOT / card_id
    branch = f"card/v2-{card_id}"
    WT_ROOT.mkdir(parents=True, exist_ok=True)
    if wt.exists() and not args.keep:
        run(["git", "worktree", "remove", "--force", str(wt)], ROOT)
        shutil.rmtree(wt, ignore_errors=True)
    if not wt.exists():
        rc, out = run(["git", "worktree", "add", "-B", branch, str(wt), args.base], ROOT)
        if rc:
            sys.exit(out)
    env = dict(os.environ, CARGO_TARGET_DIR=str(TARGET_DIR))
    rc, out = run("cd nucleus/v2 && cargo fmt --all -- --check", wt, env=env)
    if rc:
        sys.exit("preflight: base is not rustfmt-clean; fix it before issuing a card\n" + out[-800:])

    wt_cards = wt / "nucleus/v2/cards"
    wt_cards.mkdir(parents=True, exist_ok=True)
    shutil.copy(card_path, wt_cards / card_path.name)
    shutil.copy(CARDS / "WORKER-PROMPT.local.md", wt_cards / "WORKER-PROMPT.local.md")
    for extra in ("PLAN.local.md", "HANDOFF.md"):
        src = V2 / "docs" / extra
        if src.exists() and not (wt / "nucleus/v2/docs" / extra).exists():
            shutil.copy(src, wt / "nucleus/v2/docs" / extra)
    (wt / "opencode.json").write_text(json.dumps(OPENCODE_CONFIG, indent=2))
    log_dir = CARDS / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    prompt = (CARDS / "WORKER-PROMPT.local.md").read_text()
    prompt += f"\n\nYour card is `nucleus/v2/cards/{card_path.name}`. Base branch: `{args.base}`. Begin.\n"

    session = None
    result = {"pass": False, "attempts": 0, "in": 0, "out": 0, "reasoning": 0, "scope": "n/a", "seconds": 0}
    t0 = time.time()
    failure = ""
    for attempt in range(1, args.attempts + 1):
        if free_gb(WT_ROOT) < MIN_FREE_GB:
            sys.exit(f"[{card_id}] only {free_gb(WT_ROOT):.1f} GB free (< {MIN_FREE_GB}); stopping before attempt {attempt}")
        result["attempts"] = attempt
        effort = "max" if attempt == args.attempts else args.effort
        log = log_dir / f"{card_id}.attempt{attempt}.jsonl"
        cmd = ["opencode", "run", "--standalone", "-m", f"{args.model}#{effort}", "--format", "json", "--auto", "--thinking"]
        if session:
            cmd += ["-s", session]
        msg = prompt if (attempt == 1 or not session) else f"Acceptance or scope failed. Fix it within the same card. Failure output:\n\n{failure}\n\nRe-run the acceptance commands, commit, then end with the RESULT block."
        cmd.append(msg)
        print(f"[{card_id}] attempt {attempt}: opencode {args.model} effort {effort}", flush=True)
        finished = False
        for start in range(3):
            with open(log, "w") as fh:
                proc = subprocess.Popen(cmd, cwd=wt, env=env, stdin=subprocess.DEVNULL, stdout=fh, stderr=subprocess.STDOUT)
                t_start = time.time()
                while proc.poll() is None:
                    time.sleep(5)
                    waited = time.time() - t_start
                    if waited > 600 and log.stat().st_size == 0:
                        proc.kill()
                        print(f"[{card_id}] no output after 10 minutes, restarting opencode (start {start + 1})", flush=True)
                        break
                    if waited > 60 * 60 or free_gb(WT_ROOT) < MIN_FREE_GB / 2:
                        proc.kill()
                        failure = "opencode run killed: timed out after 60 minutes or disk nearly full"
                        break
                else:
                    finished = True
            if finished or failure.startswith("opencode run killed"):
                break
        if not finished:
            continue
        toks, sess = parse_tokens(log)
        session = sess or session
        for k, key in (("in", "input"), ("out", "output"), ("reasoning", "reasoning")):
            result[k] += toks[key]
        bad = scope_violations(wt, args.base, touch)
        if bad:
            result["scope"] = "FAIL"
            failure = "Scope violations (revert them):\n" + "\n".join(bad)
            print(failure, flush=True)
            continue
        result["scope"] = "ok"
        failures = []
        for c in accept:
            rc, out = run(c, wt, env=env, timeout=60 * 60)
            if rc == 0 and "cargo test" in c and sum(int(n) for n in re.findall(r"test result: ok\. (\d+) passed", out)) == 0:
                rc, out = 1, out + "\nno tests ran: a cargo test acceptance command must run at least one test"
            print(f"[{card_id}] {'PASS' if rc == 0 else 'FAIL'}: {c}", flush=True)
            if rc:
                failures.append(f"$ {c}\n" + "\n".join(out.splitlines()[-60:]))
        if not failures:
            result["pass"] = True
            break
        failure = "\n\n".join(failures)
    result["seconds"] = int(time.time() - t0)

    row = f"| {time.strftime('%Y-%m-%d %H:%M')} | {card_id} | {args.model.split('/')[-1]} | {result['attempts']} | {'yes' if result['pass'] else 'NO'} | {result['scope']} | {result['in']} | {result['out'] + result['reasoning']} | {result['seconds']}s |\n"
    runlog = CARDS / "RUNLOG.local.md"
    if not runlog.exists():
        runlog.write_text("| when | card | model | attempts | pass | scope | in tok | out tok | time |\n|---|---|---|---|---|---|---|---|---|\n")
    with open(runlog, "a") as fh:
        fh.write(row)
    print(json.dumps(result))
    print(f"worktree: {wt} (branch {branch}); review with: git -C {wt} diff {args.base}...HEAD --stat")
    sys.exit(0 if result["pass"] else 1)


if __name__ == "__main__":
    main()
