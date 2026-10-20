#!/usr/bin/env python3
"""Run one nucleus v3 task card on GLM (opencode headless) in an isolated worktree, gate it, log it.

Usage: nucleus/v3/tools/run_card.py nucleus/v3/cards/<id>.local.md [--base <branch>] [--effort high|max]
       [--attempts 3] [--model zai-coding-plan/glm-5.3] [--keep [--trust-existing]]

Flow: worktree on branch card/v3-<id> from --base -> copy card + worker prompt in (they are local-only)
-> opencode v2 `run --standalone`, cwd = worktree, stdin closed (denied: rm, sudo, curl, wget, brew, git push/reset/checkout/stash, history rewrites, git-state edits and git global options) -> scope guard (only the
card's "Touch only" globs) -> acceptance commands -> up to N attempts with failure output fed back ->
RUNLOG row. Every attempt first checks free disk. Nothing is merged here: a gate writes
verdicts/<id>.md and scripts/merge-card.sh merges (PLAN-v3 §7.3, §7.6).

Every run appends a start and an end record to the card's trust ledger, cards/logs/<id>.ledger.jsonl,
outside the worktree (worker_commits()); the end is written on every exit, after the opencode child
is killed. A commit made while a worker ran never vouches for verdicts/<id>.md. A fresh run moves the
old ledger aside; --keep on a worktree with no ledger whose branch has a verdict commit refuses to start
unless --trust-existing records that the operator inspected it. A killed attempt's tokens count.

v3 rules on top of v2: the last Acceptance command must be the full gate (`scripts/gate.sh`),
whose pass requires a literal `GATE EXIT=0` line, not just exit code 0 (§7.6.4); workers may
not touch docs/, cards/, tools/, scripts/, verdicts/, CORE_PATHS, or anything in nucleus/v2.
"""
import argparse
import fnmatch
import json
import os
import pathlib
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid

V3 = pathlib.Path(__file__).resolve().parent.parent
ROOT = V3.parent.parent
CARDS = V3 / "cards"
# Under ~/.cache, not /tmp: a reboot wipes /tmp and with it any uncommitted card work.
WT_ROOT = pathlib.Path.home() / ".cache" / "nv3-cards"
TARGET_DIR = pathlib.Path.home() / ".cache" / "nv3-target"
MIN_FREE_GB = 10
# Attempt timing, in seconds; module constants so the e2e scenarios can shrink them.
NO_OUTPUT_S = 600  # a log still empty after this is a provider stall: restart opencode
STALL_S = 30 * 60  # a log silent for this long: kill the attempt
ATTEMPT_S = 60 * 60  # an attempt running this long: kill it
POLL_S = 5
# Where the per-card trust ledgers live (ledger_path()); the scenarios repoint it.
LEDGER_DIR = CARDS / "logs"
PROTECTED = [
    "nucleus/v3/docs/",
    "nucleus/v3/cards/",
    "nucleus/v3/tools/",
    "nucleus/v3/scripts/",
    "nucleus/v3/verdicts/",
    "nucleus/v3/CORE_PATHS",
    "nucleus/v2/",
    "nucleus/src/",
]
GATE_CMD = "cd nucleus/v3 && bash scripts/gate.sh"
GATE_PASS = "GATE EXIT=0"
OPENCODE_CONFIG = {
    "$schema": "https://opencode.ai/config.json",
    "permission": {
        "edit": "allow",
        "webfetch": "deny",
        "external_directory": {
            "*": "deny",
            str(pathlib.Path.home() / ".cargo" / "registry") + "/*": "allow",
            str(TARGET_DIR) + "/*": "allow",
        },
        # How opencode 2.0.32 applies this map, read from the installed binary
        # (~/.opencode/bin/opencode; [names] are its bundled JS functions):
        # - each key becomes a rule {action "shell", resource <key>}, in key order [yP, L$];
        # - the command line is parsed (tree-sitter-bash) and every `command` node,
        #   including those inside lists, pipes and $(...), is checked on its own text.
        #   cd/pushd/popd/chdir nodes are not checked. One denied command denies the
        #   whole call [wL, Nw, V];
        # - a pattern matches the whole command text: `*` is any run of characters
        #   (newlines too), `?` one character, and a trailing " *" is optional, so
        #   "rm *" also matches a bare "rm" [rb];
        # - the last matching rule wins [UC: findLast], so "*" must come first.
        # bash_permission() models this for the scenarios. Each denial is listed at the
        # command's start, after a space ("GIT_DIR=x git reset", "xargs rm") and after
        # a "/" ("/usr/bin/git reset"). Workers run git from their cwd, so any global
        # option before the verb (-C, -c, --git-dir, --work-tree) is denied outright.
        # A guard-rail, not the enforcement: interpreters, `sh -c '...'`, eval, words
        # built from quotes or variables (g"it" reset, $G reset), combined short flags
        # (git branch -qf) and scripts that run git all pass it. The post-hoc scope
        # check, which diffs against the tree as it was before the run, catches them.
        "bash": {
            "*": "allow",
            **{
                pre + pat: "deny"
                for pat in (
                    "rm *", "sudo *", "curl *", "wget *", "brew *",
                    "git -*",
                    "git push*", "git reset*", "git checkout*", "git stash*",
                    "git rebase*", "git commit *--amend*", "git update-ref*", "git switch*", "git restore*",
                    "git replace*", "git update-index*", "git config*", "git filter-branch*",
                    "git worktree*", "git symbolic-ref*", "git notes*",
                    *(f"git branch{sep}{flag}*" for flag in ("-f", "-m", "-M", "-d", "-D", "-c", "-C") for sep in (" ", " * ")),
                    "git branch *--force*", "git branch *--move*", "git branch *--delete*", "git branch *--copy*",
                )
                for pre in ("", "* ", "*/")
            },
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


def validate_card(touch, accept):
    """The card-shape rules (PLAN-v3 §7.2). Returns an error string, or None."""
    if not touch:
        return "needs a Touch only list"
    for t in touch:
        # A note after the path makes the entry match nothing, so every edit
        # would be reported as outside Touch only.
        if re.search(r"[\s(]", t):
            return f"Touch only entry '{t}' is not a bare path or glob (put notes under Limits:)"
    if not accept:
        return "needs backticked Acceptance commands"
    if accept[-1].strip() != GATE_CMD:
        return f"the last Acceptance command must be exactly `{GATE_CMD}`"
    return None


def bash_permission(cmd, rules=None):
    """opencode's decision on a bash call, by the semantics recorded above
    OPENCODE_CONFIG's bash map (rules defaults to that map). Only the scenarios
    call it, so they test the real map. tree-sitter is approximated by a
    quote-aware cut at control operators, parentheses and command substitutions;
    the patterns also match after a space, so a part it leaves joined is still
    denied. Returns "allow", "deny", or "ask" if no rule matches a part."""
    rules = OPENCODE_CONFIG["permission"]["bash"] if rules is None else rules

    def glob(pat):
        rx = re.sub(r"[.+^${}()|[\]\\]", lambda m: "\\" + m.group(0), pat.replace("\\", "/"))
        rx = rx.replace("*", ".*").replace("?", ".")
        return rx[:-3] + "( .*)?" if rx.endswith(" .*") else rx

    parts, cur, q, i = [], "", None, 0
    while i < len(cmd):
        c, nxt = cmd[i], cmd[i + 1 : i + 2]
        redirect = (c in "&|" and cmd[i - 1 : i] in ("<", ">")) or c + nxt == "&>"  # 2>&1, >|, &>
        if q == "'":  # nothing is special inside '...'
            q = None if c == "'" else q
        elif c == "\\":  # an escaped character never cuts
            c, i = cmd[i : i + 2], i + 1
        elif c + nxt == "$(" or c == "`" or (q is None and c in ";|&()\n" and not redirect):
            parts.append(cur)
            cur, i = "", i + (2 if c == "$" else 1)
            continue
        elif c in "'\"":
            q = None if q == c else q or c
        cur, i = cur + c, i + 1
    parts.append(cur)
    cmds = [p.strip().replace("\\", "/") for p in parts]
    cmds = [p for p in cmds if p and p.split()[0] not in ("cd", "chdir", "popd", "pushd")]
    found = [next((e for p, e in reversed(list(rules.items())) if re.fullmatch(glob(p), c, re.S)), "ask") for c in cmds]
    return "deny" if "deny" in found else "ask" if "ask" in found else "allow"


def gate_passed(rc, out):
    """A gate run passes only with exit 0 AND a last line of exactly `GATE EXIT=0` (§7.6.4)."""
    lines = out.rstrip("\n").splitlines()
    return rc == 0 and bool(lines) and lines[-1].strip() == GATE_PASS


def in_scope(path, touch):
    return any(fnmatch.fnmatch(path, pat) or path.startswith(pat.rstrip("*")) for pat in touch)


def git_paths(args, wt):
    """NUL-separated git path output. Without -z git C-quotes non-ASCII paths
    ("nucleus/v3/docs/\\303\\251.md"), which then match no PROTECTED prefix.
    A failed git raises CalledProcessError: an empty listing would read as
    "nothing changed" and pass every scope rule."""
    cmd = ["git", args[0], "-z", *args[1:]]
    p = subprocess.run(cmd, cwd=wt, capture_output=True, text=True, encoding="utf-8", errors="surrogateescape")
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, cmd, p.stdout, p.stderr)
    return [f for f in p.stdout.split("\0") if f]


def changed_files(wt, base):
    # Raises (from git_paths) if either listing fails; never a partial set.
    # --no-renames: a rename must report its source path, or a protected file
    # could be moved out of a protected directory unnoticed.
    files = set(git_paths(["diff", "--no-renames", "--name-only", f"{base}...HEAD"], wt))
    # Porcelain -z entries are "XY <path>"; --no-renames means no "orig path" entries.
    files.update(e[3:] for e in git_paths(["status", "--porcelain", "--no-renames", "--untracked-files=all"], wt))
    return sorted(f for f in files if f and f not in ("opencode.json",) and not f.endswith("Cargo.lock"))


def changed_since(wt, rev, f):
    """True unless f in the worktree is exactly as in rev's tree (committed,
    staged, unstaged or untracked). Diffs rev itself, not a merge-base, so an
    amend or rebase that drops rev's commits cannot hide the change; any git
    error counts as changed."""
    rc, diff = run(["git", "diff", "--no-renames", "--name-only", rev, "--", f], wt)
    rc2, untracked = run(["git", "ls-files", "--others", "--", f], wt)
    return bool(rc or rc2 or diff.strip() or untracked.strip())


def ledger_path(card_id):
    return LEDGER_DIR / f"{card_id}.ledger.jsonl"


def ledger_append(path, rec):
    """One JSON line, on disk before the caller goes on: the start record must
    exist before the worker can commit, the end record before the runner exits."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(json.dumps(rec) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def worker_commits(wt, base, path):
    """The commits made while a worker ran, read from the trust ledger at path,
    or None when they cannot be listed (a malformed record, a failed git), which
    callers treat as "trust nothing".

    Each run appends a start record (HEAD before its first attempt) and, on every
    exit from main(), an end record (HEAD once the child is dead). A closed
    range's worker commits are rev-list base..end minus base..start, which also
    covers commits that do not descend from start (an amend, a rebase or reset,
    then new commits). A range with no end (the runner was SIGKILLed, the power
    went) fails closed: every commit in base..HEAD outside base..start counts.
    That includes the current run, whose range is open until it exits. Commits
    made between runs, where the gate's verdict commit lands, are in no range.

    The ledger is outside the worktree, in cards/logs/, which the worker's file
    tools cannot reach (external_directory: deny); its bash still can. So this
    closes the accidental and rationalized paths (a worker "fixing" a scope
    report, a --keep rerun of a branch an earlier run forged on), not a
    determined adversary with a shell."""
    try:
        recs = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
        starts, ends = {}, {}
        for r in recs:
            if "start" in r:
                starts.setdefault(r["run"], r["start"])  # the earliest start is the widest range
            elif "end" in r:
                ends[r["run"]] = r["end"]
    except (OSError, ValueError, TypeError, KeyError):
        return None
    worker = set()
    for run_id, start in starts.items():
        if not start:
            return None
        # rev-list END --not START BASE is base..END minus base..START.
        p = subprocess.run(["git", "rev-list", ends.get(run_id) or "HEAD", "--not", start, base, "--"], cwd=wt, capture_output=True, text=True)
        if p.returncode != 0:
            return None
        worker.update(p.stdout.split())
    return worker


def gate_committed(wt, base, f, card_id, pre_head, ledger=None):
    """True if the last commit before this run (pre_head) to touch f is the
    gate's verdict commit for this card. A subject alone proves nothing (a
    worker can write one), so the commit must also be no worker commit in the
    trust ledger, and the caller requires f untouched since pre_head. ledger
    None checks the subject only: the unit scenarios, which have no runs.
    main() always passes one."""
    _, out = run(["git", "log", "-1", "--format=%H %s", f"{base}..{pre_head}", "--", f], wt)
    sha, _, subject = out.strip().partition(" ")
    if not subject.startswith(f"nucleus v3: {card_id} verdict "):
        return False
    if ledger is None:
        return True
    worker = worker_commits(wt, base, ledger)
    return worker is not None and sha not in worker


def scope_violations(wt, base, touch, card_id=None, pre_head=None, ledger=None):
    """Changed files outside the card's Touch only, or in a protected path.
    The gate commits verdicts/<ID>.md on the card branch between rounds, so a
    base...HEAD diff shows it, and X-6's round-2 worker reverted it to clear
    this check. That one file is exempt while it is exactly as the gate left
    it at pre_head (HEAD before this run's first attempt) and the ledger shows
    no worker made the commit that wrote it (gate_committed)."""
    bad = []
    try:
        files = changed_files(wt, base)
    except subprocess.CalledProcessError as e:
        # A check that cannot see the diff refuses (PLAN-v3 §7.6).
        why = ((e.stderr or "").strip().splitlines() or ["no stderr"])[0]
        return [f"{' '.join(e.cmd)} failed (exit {e.returncode}): {why} (cannot list changed files)"]
    own = f"nucleus/v3/verdicts/{card_id}.md"
    exempt = False
    if pre_head and card_id and gate_committed(wt, base, own, card_id, pre_head, ledger):
        exempt = not changed_since(wt, pre_head, own)
        if not exempt and own not in files:
            # Deleting it, or restoring the base version, leaves no base...HEAD change.
            bad.append(own + " (protected)")
    for f in files:
        if f == own and exempt:
            continue
        if any(f.startswith(p) for p in PROTECTED):
            bad.append(f + " (protected)")
        elif not in_scope(f, touch):
            bad.append(f + " (outside Touch only)")
    return bad


def stray_commits(card_id, branch, since):
    """Commits naming this card on any branch but its own: the worker wrote outside its worktree.
    A failed git log is one entry naming the failure, never []: an empty listing
    would read as "no stray commits" and the run would go on. stderr stays out
    of the parsed lines (run() folds it in, which only by accident made an
    "unknown revision 'card/v3-<ID>'" error match the card id)."""
    cmd = ["git", "log", "--branches", "--not", branch, f"--since=@{int(since)}", "--format=%h %s"]
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if p.returncode != 0:
        why = (p.stderr.strip().splitlines() or ["no stderr"])[0]
        return [f"{' '.join(cmd)} failed (exit {p.returncode}): {why} (cannot list commits; treating as stray)"]
    return [l for l in p.stdout.splitlines() if card_id in l]


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


def kill_child(proc):
    """Kill opencode and everything it started (its bash tool's git and cargo),
    then reap it: a process that outlives the kill can still commit."""
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def _sigterm(signum, frame):
    # SIGTERM's default action skips main()'s finally, and with it the kill
    # and the ledger's end record.
    raise SystemExit(128 + signum)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("card")
    ap.add_argument("--base", default="nucleus/v3-scaffold")
    ap.add_argument("--effort", default="high")
    ap.add_argument("--attempts", type=int, default=3)
    ap.add_argument("--model", default="zai-coding-plan/glm-5.3")
    ap.add_argument("--min-free-gb", type=float, default=MIN_FREE_GB, help="refuse to start an attempt below this much free disk")
    ap.add_argument("--keep", action="store_true", help="reuse an existing worktree and branch")
    ap.add_argument("--trust-existing", action="store_true", help="with --keep and no trust ledger: trust the branch as it is (after inspecting it)")
    args = ap.parse_args()

    card_path = pathlib.Path(args.card)
    card_path = card_path if card_path.is_absolute() else (pathlib.Path.cwd() / card_path).resolve()
    card_id, touch, accept = parse_card(card_path)
    err = validate_card(touch, accept)
    if err:
        sys.exit(f"card {card_id}: {err} (PLAN-v3 §7.2)")

    wt = WT_ROOT / card_id
    branch = f"card/v3-{card_id}"
    ledger = ledger_path(card_id)
    WT_ROOT.mkdir(parents=True, exist_ok=True)
    if wt.exists() and not args.keep:
        run(["git", "worktree", "remove", "--force", str(wt)], ROOT)
        shutil.rmtree(wt, ignore_errors=True)
    if not wt.exists():
        rc, out = run(["git", "worktree", "add", "-B", branch, str(wt), args.base], ROOT)
        if rc:
            sys.exit(out)
        # A recreated branch starts a new ledger; the old one is kept for the record.
        if ledger.exists():
            stamp = time.strftime("%Y%m%d-%H%M%S")
            aside = ledger.with_name(f"{card_id}.ledger.{stamp}.jsonl")
            n = 1
            while aside.exists():
                n += 1
                aside = ledger.with_name(f"{card_id}.ledger.{stamp}-{n}.jsonl")
            ledger.rename(aside)
    elif not ledger.exists():
        # A worktree from before the ledger: nothing says which of its commits a
        # worker made, so a verdict-subject commit on it cannot be told from a forgery.
        rc, subjects = run(["git", "log", "--format=%s", f"{args.base}..HEAD"], wt)
        verdict_subject = rc or any(l.startswith(f"nucleus v3: {card_id} verdict ") for l in subjects.splitlines())
        if args.trust_existing:
            _, head = run(["git", "rev-parse", "HEAD"], wt)
            trusted = uuid.uuid4().hex
            ledger_append(ledger, {"run": trusted, "start": head.strip(), "t": time.time(), "trust-existing": True})
            ledger_append(ledger, {"run": trusted, "end": head.strip(), "t": time.time()})
            print(f"[{card_id}] --trust-existing: {branch} at {head.strip()} trusted as is (recorded in {ledger})", flush=True)
        elif verdict_subject:
            sys.exit(f"no trust ledger for {card_id}; rerun without --keep, or pass --trust-existing after inspecting the branch")
    # opencode takes its session directory from PWD, not the process cwd; without this the
    # worker runs in the checkout the runner was started from.
    # one target per card: concurrent cards on a shared target serialize on its lock
    env = dict(os.environ, CARGO_TARGET_DIR=str(TARGET_DIR / card_id), PWD=str(wt))
    # --keep resumes unfinished work, which need not be formatted yet
    rc, out = (0, "") if args.keep else run("cd nucleus/v3 && cargo fmt --all -- --check", wt, env=env)
    if rc:
        sys.exit("preflight: base is not rustfmt-clean; fix it before issuing a card\n" + out[-800:])

    wt_cards = wt / "nucleus/v3/cards"
    wt_cards.mkdir(parents=True, exist_ok=True)
    shutil.copy(card_path, wt_cards / card_path.name)
    shutil.copy(CARDS / "WORKER-PROMPT.local.md", wt_cards / "WORKER-PROMPT.local.md")
    for extra in ("PLAN-v3.local.md",):
        src = CARDS / extra
        if src.exists():
            shutil.copy(src, wt_cards / extra)
    (wt / "opencode.json").write_text(json.dumps(OPENCODE_CONFIG, indent=2))
    log_dir = CARDS / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    prompt = (CARDS / "WORKER-PROMPT.local.md").read_text()
    prompt += f"\n\nYour card is `nucleus/v3/cards/{card_path.name}`. Base branch: `{args.base}`. Begin.\n"

    # Anything on the branch before the first attempt was not written by this run's worker.
    _, pre_head = run(["git", "rev-parse", "HEAD"], wt)
    pre_head = pre_head.strip()
    session = None
    result = {"pass": False, "attempts": 0, "in": 0, "out": 0, "reasoning": 0, "scope": "n/a", "seconds": 0, "killed": []}
    t0 = time.time()
    failure = ""
    child = None
    run_id = uuid.uuid4().hex
    old_term = signal.signal(signal.SIGTERM, _sigterm)
    ledger_append(ledger, {"run": run_id, "start": pre_head, "t": t0})
    try:
        for attempt in range(1, args.attempts + 1):
            if free_gb(WT_ROOT) < args.min_free_gb:
                sys.exit(f"[{card_id}] only {free_gb(WT_ROOT):.1f} GB free (< {args.min_free_gb}); stopping before attempt {attempt}")
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
            killed = None  # "silence" or "timeout"; a no-output restart is not a kill of the attempt
            for start in range(3):
                with open(log, "w") as fh:
                    # Its own process group, so kill_child reaches its children too.
                    child = subprocess.Popen(cmd, cwd=wt, env=env, stdin=subprocess.DEVNULL, stdout=fh, stderr=subprocess.STDOUT, process_group=0)
                    t_start = time.time()
                    while child.poll() is None:
                        time.sleep(POLL_S)
                        waited = time.time() - t_start
                        st = log.stat()
                        silence = int(time.time() - st.st_mtime) if st.st_size else int(waited)
                        if waited > NO_OUTPUT_S and st.st_size == 0:
                            kill_child(child)
                            result["killed"].append({"attempt": attempt, "reason": "no-output", "silence_s": silence})
                            print(f"[{card_id}] attempt {attempt}: no output at all after {waited / 60:.0f} min (provider stall?); restarting opencode (start {start + 1})", flush=True)
                            break
                        if st.st_size and silence > STALL_S:
                            kill_child(child)
                            killed = "silence"
                            failure = f"opencode run killed: no output for {STALL_S // 60} minutes (stalled model call); continue the card"
                            break
                        if waited > ATTEMPT_S or free_gb(WT_ROOT) < args.min_free_gb / 2:
                            kill_child(child)
                            killed = "timeout"
                            failure = f"opencode run killed: timed out after {ATTEMPT_S // 60} minutes or disk nearly full"
                            break
                    else:
                        finished = True
                if finished or killed:
                    break
            if '"provider.quota"' in log.read_text(errors="replace"):
                sys.exit(f"[{card_id}] provider usage limit reached; stopping (rerun with --keep after the reset)")
            # A killed attempt's partial log counts too: hours of real output must not read as a 0-token stall.
            toks, sess = parse_tokens(log)
            session = sess or session
            for k, key in (("in", "input"), ("out", "output"), ("reasoning", "reasoning")):
                result[k] += toks[key]
            if killed:
                result["killed"].append({"attempt": attempt, "reason": killed, "silence_s": silence})
                why = "of silence" if killed == "silence" else "(timed out or disk nearly full)"
                print(f"[{card_id}] attempt {attempt}: opencode killed after {(silence if killed == 'silence' else waited) / 60:.0f} min {why} ({silence} s since its last output; {toks['input']}/{toks['output']} tokens this attempt)", flush=True)
            if not finished:
                continue
            stray = stray_commits(card_id, branch, t0)
            if stray:
                result["scope"] = "OUTSIDE"
                # Listed commits start with a hash; only the failure entry starts with "git log ".
                what = "cannot list commits to check for stray ones" if stray[0].startswith("git log ") else "worker committed outside its worktree"
                print(f"[{card_id}] {what}; stopping:\n" + "\n".join(stray), flush=True)
                break
            bad = scope_violations(wt, args.base, touch, card_id, pre_head, ledger)
            if bad:
                result["scope"] = "FAIL"
                failure = "Scope violations (revert them):\n" + "\n".join(bad)
                print(failure, flush=True)
                continue
            result["scope"] = "ok"
            failures = []
            for c in accept:
                rc, out = run(c, wt, env=env, timeout=60 * 60)
                if c.strip() == GATE_CMD and not gate_passed(rc, out):
                    rc, out = 1, out + "\nthe gate's last line was not GATE EXIT=0"
                if rc == 0 and "cargo test" in c and sum(int(n) for n in re.findall(r"test result: ok\. (\d+) passed", out)) == 0:
                    rc, out = 1, out + "\nno tests ran: a cargo test acceptance command must run at least one test"
                print(f"[{card_id}] {'PASS' if rc == 0 else 'FAIL'}: {c}", flush=True)
                if rc:
                    failures.append(f"$ {c}\n" + "\n".join(out.splitlines()[-60:]))
            _, dirty = run("git status --porcelain --untracked-files=all -- . ':!opencode.json'", wt)
            if dirty.strip():
                failures.append("Uncommitted changes (commit the card's work):\n" + dirty.strip())
                print(f"[{card_id}] FAIL: uncommitted changes", flush=True)
            if not failures:
                result["pass"] = True
                break
            failure = "\n\n".join(failures)
        result["seconds"] = int(time.time() - t0)

        # The row reports the final tree, however the loop ended: a killed or
        # exhausted last attempt used to leave "scope n/a".
        bad = scope_violations(wt, args.base, touch, card_id, pre_head, ledger)
        result["scope"] = "OUTSIDE" if stray_commits(card_id, branch, t0) else "FAIL" if bad else "ok"
        if bad:
            print(f"[{card_id}] final scope FAIL: " + "; ".join(bad), flush=True)
        row = f"| {time.strftime('%Y-%m-%d %H:%M')} | {card_id} | {args.model.split('/')[-1]} | {result['attempts']} | {'yes' if result['pass'] else 'NO'} | {result['scope']} | {result['in']} | {result['out'] + result['reasoning']} | {result['seconds']}s |\n"
        runlog = CARDS / "RUNLOG.local.md"
        if not runlog.exists():
            runlog.write_text("| when | card | model | attempts | pass | scope | in tok | out tok | time |\n|---|---|---|---|---|---|---|---|---|\n")
        with open(runlog, "a") as fh:
            fh.write(row)
        print(json.dumps(result))
        print(f"worktree: {wt} (branch {branch}); review with: git -C {wt} diff {args.base}...HEAD --stat")
    finally:
        # Every exit (finish, exhausted, quota, disk, stray, exception, SIGINT,
        # SIGTERM) closes this run's range. The child dies first: one that
        # outlives the runner keeps committing after the end record.
        old_int = signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        if child is not None and child.poll() is None:
            kill_child(child)
        _, head = run(["git", "rev-parse", "HEAD"], wt)
        head = head.strip()
        ledger_append(ledger, {"run": run_id, "end": head if re.fullmatch(r"[0-9a-f]{40}", head) else None, "t": time.time()})
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)
    sys.exit(0 if result["pass"] else 1)


if __name__ == "__main__":
    main()
