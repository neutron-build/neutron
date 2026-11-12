#!/usr/bin/env python3
"""The E4 failure-class matcher (card E4-CLASS-1).

This script owns three invariants, and nothing else:

1. `match` reads one E4 campaign results file and the frozen class table
   (`e4-class-table.json`, next to this script) and assigns every
   failing record to a class of that table. A record no class takes is
   **new**: it is printed as unmatched and the exit code is 1. The table
   is frozen by hash (`TABLE_SHA256`); a table with any other bytes is
   never read.
2. It fails closed. A field this script reads that is missing or wrongly
   shaped is never read as false, zero, null or a non-match: the run
   stops with exit 2 and one `E4_CLASS_CANNOT_CHECK` line. So does a
   results file whose sha256 is not the one the caller named, and a
   failing round that two classes both take.
3. `self-test` runs literal copies of three baseline records (seeds
   1257, 2927 and 3988 of `cb947c19…ae45`) and a literal diagnostic
   block through the same engine, one edit per named case, and exits
   non-zero if any case reads differently.

What counts as failing: a `rounds` entry with `pass=false`, a `twin`
entry with `pass=false`, and a seed with `deadlock`, `panic` or `baton`
true or `defect` not null. A twin failure and a bad seed have no class;
each is one unmatched record. A failing round matches a class when every
condition of the class holds: its `seed` and `round` conditions (`eq`,
type-strict; `ge`, an integer that is not a boolean; `re`, a string, in
full), its `q` entries, `lost + mis + torn >= media_faults_min`, and its
`twin_same_index` conditions on the twin entry at the round's index.

A class with `needs_diag=true` (class 3, `amcheck_subtree_fallout`)
carries a signature the JSON alone cannot separate from a real orphan,
so its rounds match only with the round's recorded `E4_MPE_DIAG` run in
`--diag-dir`: `s<seed>.json` reproduces the digest, and the round's
`class Mp` block of `s<seed>.err` has exactly one `R29 diag` line with
`cutoff_ok=true`, `equal=true`, non-empty `failed_lpids` and
`unreachable`, and exactly one `slots_of_note=` line that gives the
slot `o3` names to an unreachable LPID. Without all of that the round
is unmatched, with the reason. One reading is stricter than the card's
words and is deliberate: a line that starts `R29 diag:` but does not
match the full pattern also fails the round (the card counts only full
matches; a garbled second line should not pass unseen).

Nothing here judges counts: no cap exists, and a count is never a pass
input. Deterministic: seeds ascending, then rounds ascending, then twin
entries ascending; every line is computed from recorded fields and the
recorded diagnostic files, never from a store, a model or a re-run.
Standard library only; run as `python3 -I`.

Exit codes: 0 every failing record matched exactly one class; 1 at
least one record is unmatched; 2 cannot check (one line, starting
`E4_CLASS_CANNOT_CHECK `, and nothing else on stdout).

Usage:
  e4_class_match.py match --results F --results-sha256 H \
      [--diag-dir D] [--table T]
  e4_class_match.py self-test
"""

import argparse
import contextlib
import copy
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile

# ---------------------------------------------------------------------------
# The frozen table and the fields the contract reads.
# ---------------------------------------------------------------------------

#: sha256 of `e4-class-table.json` (card E4-CLASS-1, Limit 1). The
#: command-line path always checks the table against this constant; only
#: the self-test passes another value, to make the check testable.
TABLE_SHA256 = "55ae47347b77f610d51a51287259513d735e2aa92a20fec47889517481c61158"

#: The table's file name, next to this script unless `--table` names one.
TABLE_NAME = "e4-class-table.json"

#: Read on every seed, failing or not.
SEED_FIELDS = ("seed", "class", "digest", "deadlock", "panic", "baton", "defect", "rounds", "twin")

#: The seed flags that make a seed bad when true (`defect` is the fourth).
SEED_FLAGS = ("deadlock", "panic", "baton")

#: Summed against a class's `media_faults_min`.
MEDIA_FIELDS = ("lost", "mis", "torn")

#: The three condition operators a table may use.
COND_OPS = ("eq", "ge", "re")

#: A diagnostic block starts at a line with this prefix and ends before
#: the next one (`src/harness.rs`, the `E4_MPE_DIAG` header).
BLOCK_PREFIX = "MPE diag: === seed "

#: X5-R29 (a)'s one-line reading (`r29_subtree_diag` in `src/harness.rs`).
R29_PREFIX = "R29 diag:"
R29_RE = re.compile(
    r"R29 diag: failed_lpids \[([0-9, ]*)\] cutoff_ok=(true|false) "
    r"descendants \{([0-9, ]*)\} unreachable \{([0-9, ]*)\} equal=(true|false)"
)

#: The `(lpid, slot)` list of the same diagnostic's `R29 dbg` line.
NOTE_MARK = "slots_of_note="
NOTE_RE = re.compile(r".*slots_of_note=\[((?:\([0-9]+, [0-9]+\)(?:, \([0-9]+, [0-9]+\))*)?)\]")

#: The slot a class-3 round's `o3` names.
O3_SLOT_RE = re.compile(r"O-3\(e\): \(ii\) reported slot ([0-9]+) that is neither tainted nor in R")


class CannotCheck(Exception):
    """The run cannot be checked: exit 2, one line, nothing else."""


def is_int(value):
    """True for a JSON integer; a boolean is not one."""
    return type(value) is int


def sha256_hex(data):
    """The sha256 hex digest of a byte string."""
    return hashlib.sha256(data).hexdigest()


def read_bytes(path, what):
    """Read a file the check depends on, or stop with exit 2."""
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError as e:
        raise CannotCheck("%s unreadable: %s" % (what, e.strerror or e)) from None


# ---------------------------------------------------------------------------
# Conditions.
# ---------------------------------------------------------------------------


def strict_eq(a, b):
    """Type-strict equality: `true` is not `1`, `false` is not `0`,
    `0` is not `0.0`, and `null` equals only `null`. Lists and objects
    compare element by element under the same rule."""
    if type(a) is not type(b):
        return False
    if isinstance(a, list):
        return len(a) == len(b) and all(strict_eq(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(strict_eq(a[k], b[k]) for k in a)
    return a == b


def cond_holds(cond, value):
    """One table condition against one recorded value.

    The value's presence is the caller's business (`require`); a value
    of the wrong type for `ge` or `re` is a non-match, never an error,
    because `c` is legitimately `null` in one class and an integer in
    another.
    """
    ((op, want),) = cond.items()
    if op == "eq":
        return strict_eq(value, want)
    if op == "ge":
        return is_int(value) and value >= want
    return isinstance(value, str) and re.fullmatch(want, value) is not None


def require(obj, key, where):
    """The one reader of a recorded field: a missing field stops the
    run (exit 2). Nothing in this script reads a field any other way."""
    if not isinstance(obj, dict):
        raise CannotCheck("%s is not an object" % where)
    if key not in obj:
        raise CannotCheck("%s: missing field %s" % (where, key))
    return obj[key]


# ---------------------------------------------------------------------------
# The table.
# ---------------------------------------------------------------------------


def check_conds(conds, where):
    """A table's condition map: `{field: {op: value}}`, one op each."""
    if not isinstance(conds, dict):
        raise CannotCheck("table %s is not an object" % where)
    for key, cond in conds.items():
        if not (isinstance(cond, dict) and len(cond) == 1 and next(iter(cond)) in COND_OPS):
            raise CannotCheck("table %s.%s is not one of eq/ge/re" % (where, key))
        ((op, want),) = cond.items()
        if op == "ge" and not is_int(want):
            raise CannotCheck("table %s.%s ge is not an integer" % (where, key))
        if op == "re":
            try:
                re.compile(want)
            except (re.error, TypeError):
                raise CannotCheck("table %s.%s re does not compile" % (where, key)) from None


def load_table(path, expected_sha256):
    """Read the table, but only if its bytes hash to `expected_sha256`.

    The hash comes first: a table that is not the frozen one is never
    parsed, so an edited signature cannot classify anything.
    """
    raw = read_bytes(path, "table")
    got = sha256_hex(raw)
    if got != expected_sha256:
        raise CannotCheck("table sha256 %s is not the frozen %s" % (got, expected_sha256))
    try:
        table = json.loads(raw.decode("utf-8"))
    except ValueError:
        raise CannotCheck("table is not valid JSON") from None
    classes = table.get("classes") if isinstance(table, dict) else None
    if not isinstance(classes, list) or not classes:
        raise CannotCheck("table has no classes list")
    ids = []
    for n, cls in enumerate(classes):
        where = "classes[%d]" % n
        if not isinstance(cls, dict):
            raise CannotCheck("table %s is not an object" % where)
        for key in ("id", "seed", "round", "q", "media_faults_min", "twin_same_index", "needs_diag"):
            if key not in cls:
                raise CannotCheck("table %s has no %s" % (where, key))
        if not isinstance(cls["id"], str) or not re.fullmatch(r"[a-z0-9_]+", cls["id"]):
            raise CannotCheck("table %s.id is not a class name" % where)
        ids.append(cls["id"])
        check_conds(cls["seed"], where + ".seed")
        check_conds(cls["round"], where + ".round")
        check_conds(cls["twin_same_index"], where + ".twin_same_index")
        if not isinstance(cls["q"], dict) or not all(re.fullmatch(r"[0-9]+", k) for k in cls["q"]):
            raise CannotCheck("table %s.q is not an index map" % where)
        if not is_int(cls["media_faults_min"]):
            raise CannotCheck("table %s.media_faults_min is not an integer" % where)
        if type(cls["needs_diag"]) is not bool:
            raise CannotCheck("table %s.needs_diag is not a boolean" % where)
    if len(set(ids)) != len(ids):
        raise CannotCheck("table has a repeated class id")
    return classes


def required_fields(classes):
    """What a failing round must carry: the union over **every** class,
    not the fields of the class that ends up matching. A round is
    checked for all of them before any class is tried, so a file that
    lost a field cannot slide into a class that does not read it."""
    q_len = 0
    for cls in classes:
        for index in cls["q"]:
            q_len = max(q_len, int(index) + 1)
    return {
        "seed": sorted(set().union(*(cls["seed"] for cls in classes))),
        "round": sorted(set().union(*(cls["round"] for cls in classes))),
        "q_len": q_len,
        "twin": sorted(set().union(*(cls["twin_same_index"] for cls in classes))),
    }


# ---------------------------------------------------------------------------
# The results file.
# ---------------------------------------------------------------------------


def load_results(path, expected_sha256):
    """Read the results file, but only if it is the one the caller named."""
    raw = read_bytes(path, "results")
    got = sha256_hex(raw)
    if got != expected_sha256:
        raise CannotCheck("results sha256 %s is not the expected %s" % (got, expected_sha256))
    try:
        doc = json.loads(raw.decode("utf-8"))
    except ValueError:
        raise CannotCheck("results file is not valid JSON") from None
    if not isinstance(doc, dict) or not isinstance(doc.get("seeds"), list):
        raise CannotCheck("results file has no seeds list")
    return doc["seeds"]


def check_seed_shape(seed):
    """The fields read on every seed, failing or not. Returns the seed id."""
    if not isinstance(seed, dict):
        raise CannotCheck("a seeds entry is not an object")
    sid = require(seed, "seed", "a seeds entry")
    if not is_int(sid):
        raise CannotCheck("a seeds entry's seed is not an integer")
    where = "seed %d" % sid
    got = {key: require(seed, key, where) for key in SEED_FIELDS}
    if not isinstance(got["class"], str):
        raise CannotCheck("%s: class is not a string" % where)
    if not is_int(got["digest"]):
        raise CannotCheck("%s: digest is not an integer" % where)
    for key in SEED_FLAGS:
        if type(got[key]) is not bool:
            raise CannotCheck("%s: %s is not a boolean" % (where, key))
    for key in ("rounds", "twin"):
        if not isinstance(got[key], list):
            raise CannotCheck("%s: %s is not a list" % (where, key))
        for i, entry in enumerate(got[key]):
            entry_where = "%s %s[%d]" % (where, key, i)
            if type(require(entry, "pass", entry_where)) is not bool:
                raise CannotCheck("%s: pass is not a boolean" % entry_where)
    return sid


def check_failing_round_shape(seed, i, req):
    """Every field any class reads, on one failing round and its twin."""
    where = "seed %d round %d" % (seed["seed"], i)
    for key in req["seed"]:
        require(seed, key, "seed %d" % seed["seed"])
    rnd = seed["rounds"][i]
    for key in req["round"]:
        require(rnd, key, where)
    for key in MEDIA_FIELDS:
        if not is_int(require(rnd, key, where)):
            raise CannotCheck("%s: %s is not an integer" % (where, key))
    if req["q_len"]:
        q = require(rnd, "q", where)
        if not isinstance(q, list) or len(q) < req["q_len"]:
            raise CannotCheck("%s: q is not a list of at least %d" % (where, req["q_len"]))
    if i >= len(seed["twin"]):
        raise CannotCheck("%s: no twin entry at index %d" % (where, i))
    for key in req["twin"]:
        require(seed["twin"][i], key, "seed %d twin[%d]" % (seed["seed"], i))


# ---------------------------------------------------------------------------
# Matching.
# ---------------------------------------------------------------------------


def first_mismatch(cls, seed, i):
    """The first condition of `cls` that round `i` does not meet, as a
    short name, or None when the round matches the class's signature."""
    sid = seed["seed"]
    rnd = seed["rounds"][i]
    where = "seed %d round %d" % (sid, i)
    for key in sorted(cls["seed"]):
        if not cond_holds(cls["seed"][key], require(seed, key, "seed %d" % sid)):
            return "seed." + key
    for key in sorted(cls["round"]):
        if not cond_holds(cls["round"][key], require(rnd, key, where)):
            return "round." + key
    for index in sorted(cls["q"], key=int):
        q = require(rnd, "q", where)
        if not isinstance(q, list) or len(q) <= int(index):
            raise CannotCheck("%s: q has no index %s" % (where, index))
        if not strict_eq(q[int(index)], cls["q"][index]):
            return "q[%s]" % index
    media = 0
    for key in MEDIA_FIELDS:
        value = require(rnd, key, where)
        if not is_int(value):
            raise CannotCheck("%s: %s is not an integer" % (where, key))
        media += value
    if media < cls["media_faults_min"]:
        return "media_faults"
    if cls["twin_same_index"]:
        if i >= len(seed["twin"]):
            raise CannotCheck("%s: no twin entry at index %d" % (where, i))
        for key in sorted(cls["twin_same_index"]):
            value = require(seed["twin"][i], key, "seed %d twin[%d]" % (sid, i))
            if not cond_holds(cls["twin_same_index"][key], value):
                return "twin." + key
    return None


def parse_int_list(text):
    """`"5, 64"` as `[5, 64]`, `""` as `[]`; None if it is not that shape."""
    if text == "":
        return []
    parts = text.split(", ")
    if not all(re.fullmatch(r"[0-9]+", p) for p in parts):
        return None
    return [int(p) for p in parts]


def diag_block(err_text, sid, i):
    """The round's `class Mp` block: the lines after its header and
    before the next block header. Returns `(lines, None)`, or
    `(None, reason)` unless exactly one such header exists."""
    header = re.compile(r"MPE diag: === seed %d round %d class Mp crash .* ===" % (sid, i))
    lines = err_text.splitlines()
    starts = [n for n, line in enumerate(lines) if header.fullmatch(line)]
    if not starts:
        return None, "no-block"
    if len(starts) > 1:
        return None, "block-header-repeated"
    block = []
    for line in lines[starts[0] + 1 :]:
        if line.startswith(BLOCK_PREFIX):
            break
        block.append(line)
    return block, None


def diag_reason(diag_dir, sid, i, digest, o3):
    """Why a `needs_diag` round's diagnostic does not hold, or None when
    all five conditions of the contract hold."""
    if diag_dir is None:
        return "no-diag-dir"
    # 1. Both files exist.
    json_path = os.path.join(diag_dir, "s%d.json" % sid)
    err_path = os.path.join(diag_dir, "s%d.err" % sid)
    try:
        with open(json_path, "rb") as f:
            json_raw = f.read()
    except OSError:
        return "diag-json-missing"
    try:
        with open(err_path, "rb") as f:
            err_raw = f.read()
    except OSError:
        return "diag-err-missing"
    # 2. The diagnostic run is this seed's and reproduced its digest.
    try:
        doc = json.loads(json_raw.decode("utf-8"))
    except ValueError:
        return "diag-json-invalid"
    seeds = doc.get("seeds") if isinstance(doc, dict) else None
    if not isinstance(seeds, list) or len(seeds) != 1 or not isinstance(seeds[0], dict):
        return "diag-json-not-one-seed"
    if "seed" not in seeds[0] or not strict_eq(seeds[0]["seed"], sid):
        return "diag-seed-differs"
    if "digest" not in seeds[0] or not strict_eq(seeds[0]["digest"], digest):
        return "diag-digest-differs"
    # 3. The round's block exists, once.
    try:
        err_text = err_raw.decode("utf-8")
    except ValueError:
        return "diag-err-invalid"
    block, reason = diag_block(err_text, sid, i)
    if block is None:
        return reason
    # 4. Exactly one R29 diag line, in full, reading subtree fallout.
    r29_lines = [line for line in block if line.startswith(R29_PREFIX)]
    r29 = [m for m in (R29_RE.fullmatch(line) for line in r29_lines) if m is not None]
    if len(r29) != 1 or len(r29_lines) != 1:
        return "r29-lines=%d" % len(r29_lines)
    failed = parse_int_list(r29[0].group(1))
    unreachable = parse_int_list(r29[0].group(4))
    if failed is None or unreachable is None or parse_int_list(r29[0].group(3)) is None:
        return "r29-list-malformed"
    if r29[0].group(2) != "true":
        return "cutoff_ok=false"
    if r29[0].group(5) != "true":
        return "equal=false"
    if not failed:
        return "failed_lpids-empty"
    if not unreachable:
        return "unreachable-empty"
    # 5. The slot o3 names belongs to an unreachable LPID.
    note_lines = [line for line in block if NOTE_MARK in line]
    if len(note_lines) != 1:
        return "slots_of_note-lines=%d" % len(note_lines)
    note = NOTE_RE.fullmatch(note_lines[0])
    if note is None:
        return "slots_of_note-malformed"
    slot = O3_SLOT_RE.fullmatch(o3) if isinstance(o3, str) else None
    if slot is None:
        return "o3-names-no-slot"
    pairs = [(int(a), int(b)) for a, b in re.findall(r"\(([0-9]+), ([0-9]+)\)", note.group(1))]
    wanted = int(slot.group(1))
    if not any(lpid in unreachable and s == wanted for lpid, s in pairs):
        return "o3-slot-%d-not-of-an-unreachable-lpid" % wanted
    return None


def classify(seeds, classes, diag_dir):
    """Every failing record of the file, as output lines and counts."""
    req = required_fields(classes)
    by_id = {}
    for seed in seeds:
        sid = check_seed_shape(seed)
        if sid in by_id:
            raise CannotCheck("duplicate seed %d" % sid)
        by_id[sid] = seed
    counts = {cls["id"]: 0 for cls in classes}
    unmatched = 0
    lines = []
    for sid in sorted(by_id):
        seed = by_id[sid]
        bad = ["%s=true" % key for key in SEED_FLAGS if seed[key]]
        if seed["defect"] is not None:
            bad.append("defect-not-null")
        if bad:
            lines.append("E4_CLASS_UNMATCHED %d seed %s" % (sid, ",".join(bad)))
            unmatched += 1
        for i, rnd in enumerate(seed["rounds"]):
            if rnd["pass"]:
                continue
            check_failing_round_shape(seed, i, req)
            why = [(cls, first_mismatch(cls, seed, i)) for cls in classes]
            took = [cls for cls, miss in why if miss is None]
            if len(took) > 1:
                raise CannotCheck(
                    "seed %d round %d matches more than one class: %s"
                    % (sid, i, ",".join(cls["id"] for cls in took))
                )
            if not took:
                reason = "no-class " + " ".join("%s:%s" % (cls["id"], miss) for cls, miss in why)
            elif took[0]["needs_diag"]:
                reason = diag_reason(diag_dir, sid, i, seed["digest"], rnd["o3"])
                if reason is not None:
                    reason = "%s needs-diag %s" % (took[0]["id"], reason)
            else:
                reason = None
            if reason is None:
                counts[took[0]["id"]] += 1
                lines.append("E4_CLASS %d %d %s" % (sid, i, took[0]["id"]))
            else:
                unmatched += 1
                lines.append("E4_CLASS_UNMATCHED %d round %d %s" % (sid, i, reason))
        for i, entry in enumerate(seed["twin"]):
            if not entry["pass"]:
                unmatched += 1
                lines.append("E4_CLASS_UNMATCHED %d twin %d pass=false" % (sid, i))
    lines.append(
        "E4_CLASS_COUNTS %s unmatched=%d"
        % (" ".join("%s=%d" % (cls["id"], counts[cls["id"]]) for cls in classes), unmatched)
    )
    return lines, unmatched


def run_match(results_path, results_sha256, diag_dir, table_path, table_sha256):
    """The whole check: `(exit code, stdout lines)`.

    Exit 2 carries exactly one line; nothing classified before the stop
    is printed, so a cannot-check run never looks like a partial pass.
    An error this script did not foresee is a cannot-check too: Python's
    own exit code for a traceback is 1, which here means "unmatched".
    """
    try:
        classes = load_table(table_path, table_sha256)
        seeds = load_results(results_path, results_sha256)
        lines, unmatched = classify(seeds, classes, diag_dir)
    except CannotCheck as e:
        return 2, ["E4_CLASS_CANNOT_CHECK %s" % e]
    except Exception as e:  # noqa: BLE001 - fail closed on anything else
        return 2, ["E4_CLASS_CANNOT_CHECK internal error: %s: %s" % (type(e).__name__, e)]
    return (1 if unmatched else 0), lines


def default_table_path():
    """`e4-class-table.json` next to this script."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), TABLE_NAME)


def cmd_match(args):
    """The match subcommand. The table is always held to `TABLE_SHA256`."""
    code, lines = run_match(
        args.results,
        args.results_sha256,
        args.diag_dir,
        args.table if args.table is not None else default_table_path(),
        TABLE_SHA256,
    )
    for line in lines:
        print(line)
    return code


# ---------------------------------------------------------------------------
# Self-test.
# ---------------------------------------------------------------------------

#: Literal copies of three seed records of the FINAL main baseline
#: `X-5c-F4-F5.head.main.json` (sha256 `cb947c19…ae45`): seed 1257 r0
#: (class 1), seed 2927 r0 (class 2) and seed 3988 r0 (class 3), each
#: with its seed fields and its `twin` array.
FIXTURE_OLD_PAGE = (
    '{"seed":1257,"class":"MP","knobs":1269534623,"digest":1790571651,"deadlock":false,"panic":false,'
    '"baton":false,"stopped":true,"defect":null,"rounds":[{"crash":"uniform@y:18836","hook":"","fired":true,'
    '"nested":0,"lost":1,"mis":1,"torn":1,"lo":13,"hi":13,"c":null,"frontier":0,"gcw":0,'
    '"o1":"O-1(a) at c=13: published {} != model {2, 3}","o1f":true,"o2":null,'
    '"o3":"O-3(a): served corruption","served":true,"sk":true,"ackl":0,"ackls":0,"mrb":true,"rlen":0,'
    '"o1dl":0,"r6":false,"r32":false,"skx":true,"smx":false,"pass_na":true,"ack_over":[],"cover_err":false,'
    '"x33h":0,"x33a":13,"x33x":false,"reuse":false,"cxx":0,"pass":false,"dl":false,"ll":false,"lls":"",'
    '"pa":false,"bt":false,"terr":false,"cwin":"w12","szc":0,"szq":0,"szi":0,"rfs":1,"q4sp":1,"q4mg":0,'
    '"q4ru":0,"imgsp":1,"imgmg":0,"imgru":0,"imgrupf":0,"q4ir":0,"q4i":0,'
    '"q":[0,0,0,0,0,0,0,0,1,0,0,0,1,1,0,1,0,0,1,0,0]}],"twin":[{"pass":true,"o1":null}]}'
)
FIXTURE_OLD_HEADER = (
    '{"seed":2927,"class":"MP","knobs":1843063363,"digest":2290983143,"deadlock":false,"panic":false,'
    '"baton":false,"stopped":true,"defect":null,"rounds":[{"crash":"After4@1","hook":"After4","fired":true,'
    '"nested":0,"lost":1,"mis":1,"torn":1,"lo":20,"hi":24,"c":null,"frontier":0,"gcw":0,'
    '"o1":"O-1(a) at c=20: published {} != model {2}","o1f":true,"o2":null,'
    '"o3":"O-3(a): served corruption","served":true,"sk":false,"ackl":0,"ackls":0,"mrb":true,"rlen":1,'
    '"o1dl":0,"r6":false,"r32":false,"skx":false,"smx":true,"pass_na":true,"ack_over":[],"cover_err":false,'
    '"x33h":0,"x33a":20,"x33x":false,"reuse":false,"cxx":0,"pass":false,"dl":false,"ll":false,"lls":"",'
    '"pa":false,"bt":false,"terr":false,"cwin":"u","szc":0,"szq":0,"szi":0,"rfs":1,"q4sp":0,"q4mg":0,'
    '"q4ru":0,"imgsp":0,"imgmg":0,"imgru":0,"imgrupf":0,"q4ir":0,"q4i":0,'
    '"q":[0,0,0,0,0,0,0,0,1,0,0,0,0,1,0,1,0,0,1,0,0]}],"twin":[{"pass":true,"o1":null}]}'
)
FIXTURE_FALLOUT = (
    '{"seed":3988,"class":"MP","knobs":3468853498,"digest":4193132450,"deadlock":false,"panic":false,'
    '"baton":false,"stopped":true,"defect":null,"rounds":[{"crash":"After6@2","hook":"After6","fired":true,'
    '"nested":1,"lost":1,"mis":1,"torn":0,"lo":30,"hi":30,"c":30,"frontier":30,"gcw":3,"o1":null,'
    '"o1f":false,"o2":null,"o3":"O-3(e): (ii) reported slot 280 that is neither tainted nor in R",'
    '"served":false,"sk":true,"ackl":0,"ackls":0,"mrb":false,"rlen":0,"o1dl":58,"r6":true,"r32":false,'
    '"skx":false,"smx":false,"pass_na":null,"ack_over":[],"cover_err":false,"x33h":30,"x33a":30,'
    '"x33x":false,"reuse":false,"cxx":0,"pass":false,"dl":false,"ll":false,"lls":"","pa":false,"bt":false,'
    '"terr":false,"cwin":"u","szc":0,"szq":0,"szi":0,"rfs":0,"q4sp":0,"q4mg":0,"q4ru":0,"imgsp":0,'
    '"imgmg":0,"imgru":0,"imgrupf":0,"q4ir":0,"q4i":0,'
    '"q":[0,0,0,0,0,1,0,0,1,0,0,0,1,1,0,0,0,1,1,0,0]}],"twin":[{"pass":true,"o1":null}]}'
)

#: Seed 3988's `E4_MPE_DIAG` stderr, in the real shape: the twin's
#: `class C` block first (with its own, empty, `R29 diag` line), then
#: the round's `class Mp` block. Lines are the real run's; the 44
#: unreachable LPIDs are cut to three and the per-write history lines
#: are dropped, which no check reads.
DIAG_C_BLOCK = (
    "MPE diag: durable_image meta slots: s1=Ok(2) s2=Ok(3) pick=Win { meta: Meta { ckpt_id: 3 }, slot: 2 }",
    "MPE diag: === seed 3988 round 0 class C crash Some(Hook { ckpt_index: 2, hook: After6 }) ===",
    "MPE diag: amcheck(ii) findings [] (store amcheck on the end disk)",
    "R29 dbg: dir_root=1 next_lpid=192 slots_of_note=[]",
    "R29 diag: failed_lpids [] cutoff_ok=true descendants {} unreachable {} equal=true",
    "MPE diag: kept K {} K_ii {} served_k=true (X5-R28/R30; stderr only)",
    "MPE diag: mapped lpid 5 slot 172 §3.3=false tainted=true",
)
DIAG_MP_HEADER = "MPE diag: === seed 3988 round 0 class Mp crash Some(Hook { ckpt_index: 2, hook: After6 }) ==="
DIAG_NOTE = "R29 dbg: dir_root=1 next_lpid=192 slots_of_note=[(5, 172), (8, 280), (9, 288), (10, 289), (64, 6)]"
DIAG_R29 = (
    "R29 diag: failed_lpids [5, 64] cutoff_ok=true descendants {8, 9, 10} unreachable {8, 9, 10} equal=true"
)
DIAG_MP_BLOCK = (
    DIAG_MP_HEADER,
    "MPE diag: media[0] Misdirected { to_off: 499712 } inode 0 off 24576 len 4096 (page slot 6)",
    "MPE diag: media[1] Lost inode 0 off 704512 len 4096 (page slot 172)",
    'MPE diag: amcheck(ii) findings [Finding { slot: Some(6), what: "tree: §3.3 fail on lpid 64" }, '
    'Finding { slot: Some(172), what: "tree: §3.3 fail on lpid 5" }, '
    'Finding { slot: Some(280), what: "mapped lpid 8 unreachable" }] (store amcheck on the end disk)',
    DIAG_NOTE,
    "R29 dbg: failed lpid 5 slot 172 ve_kind=4 kids=[8, 9, 10]",
    "R29 dbg: failed lpid 64 slot 6 ve_kind=3 kids=[]",
    DIAG_R29,
    "MPE diag: mapped lpid 5 slot 172 §3.3=false tainted=true",
    "MPE diag: kept K {} K_ii {} served_k=true (X5-R28/R30; stderr only)",
)

#: Every field the contract reads, written out here and never derived
#: from the engine: `missing-each-field` deletes each one in turn, and a
#: list computed by the code under test would shrink with it.
ST_SEED_FIELDS = ("seed", "class", "digest", "deadlock", "panic", "baton", "defect", "rounds", "twin")
ST_ROUND_FIELDS = (
    "pass", "served", "mrb", "c", "frontier", "o1", "o1f", "o2", "o3", "skx", "smx", "pass_na", "rlen",
    "r6", "r32", "ack_over", "cover_err", "x33x", "reuse", "cxx", "dl", "ll", "lls", "pa", "bt", "terr",
    "q", "lost", "mis", "torn",
)  # fmt: skip
ST_TWIN_FIELDS = ("pass", "o1")


def self_test():
    """Every named case of the card's table, through `run_match`."""
    failures = []
    cases = [0]
    tmp = tempfile.mkdtemp(prefix="e4-class-selftest-")
    serial = [0]
    real_table = default_table_path()

    def check(name, ok, why=""):
        cases[0] += 1
        print("%s %s%s" % ("PASS" if ok else "FAIL", name, "" if ok else " " + why))
        if not ok:
            failures.append(name)

    def fresh(suffix):
        serial[0] += 1
        return os.path.join(tmp, "f%d%s" % (serial[0], suffix))

    def write(path, data):
        with open(path, "wb") as f:
            f.write(data)
        return path

    def results_file(seeds):
        data = json.dumps({"e4": 1, "set": "main", "seeds": seeds}).encode("utf-8")
        return write(fresh(".json"), data), sha256_hex(data)

    def diag_dir(err_lines=DIAG_C_BLOCK + DIAG_MP_BLOCK, digest=4193132450, seeds=None, files=("json", "err")):
        d = fresh(".diag")
        os.mkdir(d)
        if "json" in files:
            if seeds is None:
                seeds = [{"seed": 3988, "class": "MP", "digest": digest}]
            write(os.path.join(d, "s3988.json"), json.dumps({"e4": 1, "seeds": seeds}).encode("utf-8"))
        if "err" in files:
            write(os.path.join(d, "s3988.err"), ("\n".join(err_lines) + "\n").encode("utf-8"))
        return d

    def counts(page, header, fallout, unmatched):
        return (
            "E4_CLASS_COUNTS refused_old_page=%d refused_old_header=%d "
            "amcheck_subtree_fallout=%d unmatched=%d" % (page, header, fallout, unmatched)
        )

    def run(seeds, diag=None, table=None, table_sha=TABLE_SHA256):
        path, sha = results_file(seeds)
        return run_match(path, sha, diag, real_table if table is None else table, table_sha)

    def expect(name, got, code, count_line=None, line=None):
        """Exit code plus the count line; exit 2 must be the one line."""
        got_code, lines = got
        ok = got_code == code
        if code == 2:
            ok = ok and len(lines) == 1 and lines[0].startswith("E4_CLASS_CANNOT_CHECK ")
        else:
            ok = ok and bool(lines) and lines[-1] == count_line
        if line is not None:
            ok = ok and line in lines
        check(name, ok, "exit %d %r" % (got_code, lines))

    def edited(fixture, round_edit=None, seed_edit=None, twin_edit=None):
        seed = json.loads(fixture)
        seed["rounds"][0].update(round_edit or {})
        seed.update(seed_edit or {})
        seed["twin"][0].update(twin_edit or {})
        return seed

    def without(lines, drop):
        return tuple(line for line in lines if line != drop)

    def swapped(lines, old, new):
        return tuple(new if line == old else line for line in lines)

    def temp_table(change):
        with open(real_table, "rb") as f:
            table = json.loads(f.read().decode("utf-8"))
        change(table)
        data = (json.dumps(table, indent=2) + "\n").encode("utf-8")
        return write(fresh(".table.json"), data), sha256_hex(data)

    try:
        page, header, fallout = FIXTURE_OLD_PAGE, FIXTURE_OLD_HEADER, FIXTURE_FALLOUT
        good_diag = diag_dir()

        # --- The table next to the script is the frozen one.
        with open(real_table, "rb") as f:
            check("table-is-frozen", sha256_hex(f.read()) == TABLE_SHA256)

        # --- Each class, alone and together.
        expect("class-old-page", run([edited(page)]), 0, counts(1, 0, 0, 0), "E4_CLASS 1257 0 refused_old_page")
        expect("class-old-header", run([edited(header)]), 0, counts(0, 1, 0, 0), "E4_CLASS 2927 0 refused_old_header")
        expect(
            "class-fallout-with-diag",
            run([edited(fallout)], diag=good_diag),
            0,
            counts(0, 0, 1, 0),
            "E4_CLASS 3988 0 amcheck_subtree_fallout",
        )
        got = run([edited(fallout), edited(header), edited(page)], diag=good_diag)
        expect("all-three-in-seed-order", got, 0, counts(1, 1, 1, 0))
        check(
            "all-three-in-seed-order-lines",
            got[1][:3]
            == [
                "E4_CLASS 1257 0 refused_old_page",
                "E4_CLASS 2927 0 refused_old_header",
                "E4_CLASS 3988 0 amcheck_subtree_fallout",
            ],
            repr(got[1]),
        )
        expect("no-failing-record", run([edited(page, {"pass": True})]), 0, counts(0, 0, 0, 0))
        # A passing round is read for `pass` only.
        expect("passing-round-needs-no-field", run([{**edited(page), "rounds": [{"pass": True}]}]), 0, counts(0, 0, 0, 0))

        # --- An unmatched round, and a matched one beside it still exits 1.
        o3b = {"o3": "O-3(b): XX001 in class C at [Open]"}
        expect("unmatched-round", run([edited(page, o3b)]), 1, counts(0, 0, 0, 1))
        expect("unmatched-round-beside-matched", run([edited(page, o3b), edited(header)]), 1, counts(0, 1, 0, 1))
        expect("unmatched-class-c-seed", run([edited(page, seed_edit={"class": "C"})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-class-mw-seed", run([edited(header, seed_edit={"class": "MW"})]), 1, counts(0, 0, 0, 1))

        # --- A twin failure has no class. The failing twin sits beside a
        # passing round, so only the twin reading can see it.
        twin_fail = edited(page, {"pass": True}, twin_edit={"pass": False})
        expect("unmatched-twin-fail", run([twin_fail]), 1, counts(0, 0, 0, 1), "E4_CLASS_UNMATCHED 1257 twin 0 pass=false")
        # Beside a failing round, the round and the twin are two records.
        expect("unmatched-twin-fail-and-round", run([edited(page, twin_edit={"pass": False})]), 1, counts(0, 0, 0, 2))

        # --- A bad seed has no class, with every round passing (only
        # the seed reading can see it) and with its failing round (two
        # records).
        for name, edit in (
            ("deadlock", {"deadlock": True}),
            ("panic", {"panic": True}),
            ("baton", {"baton": True}),
            ("defect", {"defect": "monitor: latch order"}),
        ):
            expect("unmatched-" + name, run([edited(page, {"pass": True}, seed_edit=edit)]), 1, counts(0, 0, 0, 1))
            expect("unmatched-%s-and-round" % name, run([edited(page, seed_edit=edit)]), 1, counts(0, 0, 0, 2))
        # An empty defect text is still a defect: only null is none.
        expect("unmatched-defect-empty-text", run([edited(page, {"pass": True}, seed_edit={"defect": ""})]), 1, counts(0, 0, 0, 1))

        # --- The narrow form: a store that opened after such a read.
        expect("unmatched-recovered-serve", run([edited(page, {"c": 5, "frontier": 5})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-recovered-serve-header", run([edited(header, {"c": 5, "frontier": 5})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-opened-c-only", run([edited(page, {"c": 5})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-fallout-served", run([edited(fallout, {"served": True})], diag=good_diag), 1, counts(0, 0, 0, 1))

        # --- What separates classes 1 and 2.
        expect("unmatched-skx-and-smx", run([edited(page, {"smx": True})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-skx-and-smx-header", run([edited(header, {"skx": True})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-neither", run([edited(page, {"skx": False})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-neither-header", run([edited(header, {"smx": False})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-pass-na-false", run([edited(page, {"pass_na": False})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-pass-na-null", run([edited(page, {"pass_na": None})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-pass-na-null-header", run([edited(header, {"pass_na": None})]), 1, counts(0, 0, 0, 1))

        # --- q, the media-fault floor and the twin condition.
        q_zero = json.loads(page)["rounds"][0]["q"]
        q_zero[15] = 0
        expect("unmatched-q15-zero", run([edited(page, {"q": q_zero})]), 1, counts(0, 0, 0, 1))
        no_media = {"lost": 0, "mis": 0, "torn": 0}
        expect("unmatched-no-media-fault", run([edited(page, no_media)]), 1, counts(0, 0, 0, 1))
        expect("unmatched-no-media-fault-fallout", run([edited(fallout, no_media)], diag=good_diag), 1, counts(0, 0, 0, 1))
        expect("media-fault-any-one-kind", run([edited(page, {"lost": 0, "mis": 0, "torn": 1})]), 0, counts(1, 0, 0, 0))
        twin_text = {"o1": "O-1(a) at c=3: published {} != model {2}"}
        expect("unmatched-twin-text", run([edited(page, twin_edit=twin_text)]), 1, counts(0, 0, 0, 1))
        expect("unmatched-twin-text-fallout", run([edited(fallout, twin_edit=twin_text)], diag=good_diag), 1, counts(0, 0, 0, 1))

        # --- Type-strict conditions.
        expect("unmatched-bool-is-not-int", run([edited(header, {"rlen": True})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-false-is-not-zero", run([edited(page, {"rlen": False})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-zero-is-not-false", run([edited(page, {"r6": 0})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-one-is-not-true", run([edited(page, {"served": 1})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-float-is-not-int", run([edited(page, {"frontier": 0.0})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-q15-true", run([edited(page, {"q": [True if n == 15 else v for n, v in enumerate(q_zero)]})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-ack-over-not-empty", run([edited(page, {"ack_over": [3]})]), 1, counts(0, 0, 0, 1))
        expect("unmatched-ge-below", run([edited(header, {"rlen": 0})]), 1, counts(0, 0, 0, 1))
        expect("ge-above", run([edited(header, {"rlen": 134})]), 0, counts(0, 1, 0, 0))

        # --- `re` is a full match: text around a valid value is new.
        o3 = json.loads(fallout)["rounds"][0]["o3"]
        expect("unmatched-o3-text-after", run([edited(fallout, {"o3": o3 + "; O-3(d): lost"})], diag=good_diag), 1, counts(0, 0, 0, 1))
        expect("unmatched-o3-text-before", run([edited(fallout, {"o3": "O-3(b): x; " + o3})], diag=good_diag), 1, counts(0, 0, 0, 1))
        o1 = json.loads(page)["rounds"][0]["o1"]
        expect("unmatched-o1-text-after", run([edited(page, {"o1": o1 + " and more"})]), 1, counts(0, 0, 0, 1))

        # --- Class 3 needs its diagnostic.
        def fallout_case(name, **kw):
            round_edit = kw.pop("round_edit", None)
            got = run([edited(fallout, round_edit)], diag=diag_dir(**kw) if kw.pop("with_dir", True) else None)
            expect(name, got, 1, counts(0, 0, 0, 1))

        fallout_case("fallout-no-diag-dir", with_dir=False)
        fallout_case("fallout-diag-missing", files=())
        fallout_case("fallout-diag-json-missing", files=("err",))
        fallout_case("fallout-diag-err-missing", files=("json",))
        r29_false = DIAG_R29.replace("equal=true", "equal=false")
        fallout_case("fallout-equal-false", err_lines=DIAG_C_BLOCK + swapped(DIAG_MP_BLOCK, DIAG_R29, r29_false))
        r29_cut = DIAG_R29.replace("cutoff_ok=true", "cutoff_ok=false")
        fallout_case("fallout-cutoff-false", err_lines=DIAG_C_BLOCK + swapped(DIAG_MP_BLOCK, DIAG_R29, r29_cut))
        fallout_case("fallout-digest-differs", digest=4193132451)
        fallout_case("fallout-diag-seed-differs", seeds=[{"seed": 3989, "digest": 4193132450}])
        two = [{"seed": 3988, "digest": 4193132450}, {"seed": 3989, "digest": 1}]
        fallout_case("fallout-diag-two-seeds", seeds=two)
        fallout_case("fallout-diag-no-digest", seeds=[{"seed": 3988}])
        # The twin's class C block alone (its R29 line reads equal=true
        # on empty sets) is not the round's block.
        fallout_case("fallout-no-block", err_lines=DIAG_C_BLOCK)
        other = DIAG_MP_HEADER.replace("round 0", "round 1")
        fallout_case("fallout-block-of-another-round", err_lines=DIAG_C_BLOCK + swapped(DIAG_MP_BLOCK, DIAG_MP_HEADER, other))
        fallout_case("fallout-block-header-repeated", err_lines=DIAG_C_BLOCK + DIAG_MP_BLOCK + DIAG_MP_BLOCK)
        # The valid line first, so taking the first line would pass.
        fallout_case("fallout-two-r29", err_lines=DIAG_C_BLOCK + DIAG_MP_BLOCK + (r29_false,))
        fallout_case("fallout-two-r29-same", err_lines=DIAG_C_BLOCK + DIAG_MP_BLOCK + (DIAG_R29,))
        fallout_case("fallout-r29-garbled-second", err_lines=DIAG_C_BLOCK + DIAG_MP_BLOCK + ("R29 diag: failed_lpids",))
        fallout_case("fallout-no-r29", err_lines=DIAG_C_BLOCK + without(DIAG_MP_BLOCK, DIAG_R29))
        # An R29 line after the next block header belongs to that block.
        late = DIAG_C_BLOCK + without(DIAG_MP_BLOCK, DIAG_R29) + (DIAG_MP_HEADER.replace("seed 3988", "seed 3989"), DIAG_R29)
        fallout_case("fallout-r29-in-next-block", err_lines=late)
        r29_text = DIAG_R29 + " trailing"
        fallout_case("fallout-r29-text-after", err_lines=DIAG_C_BLOCK + swapped(DIAG_MP_BLOCK, DIAG_R29, r29_text))
        r29_nofail = DIAG_R29.replace("failed_lpids [5, 64]", "failed_lpids []")
        fallout_case("fallout-failed-lpids-empty", err_lines=DIAG_C_BLOCK + swapped(DIAG_MP_BLOCK, DIAG_R29, r29_nofail))
        r29_empty = "R29 diag: failed_lpids [5, 64] cutoff_ok=true descendants {} unreachable {} equal=true"
        fallout_case("fallout-unreachable-empty", err_lines=DIAG_C_BLOCK + swapped(DIAG_MP_BLOCK, DIAG_R29, r29_empty))
        # Slot 172 is of note, but its LPID 5 failed; it is not unreachable.
        slot172 = {"o3": o3.replace("slot 280", "slot 172")}
        fallout_case("fallout-slot-not-unreachable", round_edit=slot172)
        fallout_case("fallout-slot-not-of-note", round_edit={"o3": o3.replace("slot 280", "slot 9999")})
        # LPID 280 is not unreachable either: the pair is (lpid, slot).
        fallout_case("fallout-slot-is-not-lpid", round_edit={"o3": o3.replace("slot 280", "slot 8")})
        fallout_case("fallout-no-slots-of-note", err_lines=DIAG_C_BLOCK + without(DIAG_MP_BLOCK, DIAG_NOTE))
        fallout_case("fallout-two-slots-of-note", err_lines=DIAG_C_BLOCK + DIAG_MP_BLOCK + (DIAG_NOTE,))
        # A diagnostic is asked only of the class that needs one.
        expect("diag-dir-unused-by-classes-1-2", run([edited(page), edited(header)], diag=diag_dir(files=())), 0, counts(1, 1, 0, 0))

        # --- Cannot check: a missing field is never a non-match. Every
        # field, on every fixture, under the frozen table and under the
        # same table with its classes reversed (so the first class is
        # the one that reads no `q`).
        reversed_table, reversed_sha = temp_table(lambda t: t["classes"].reverse())
        expect("reversed-table-still-matches", run([edited(fallout)], diag=good_diag, table=reversed_table, table_sha=reversed_sha), 0, "E4_CLASS_COUNTS amcheck_subtree_fallout=1 refused_old_header=0 refused_old_page=0 unmatched=0")
        bad = []
        tried = 0
        for fixture_name, fixture in (("old-page", page), ("old-header", header), ("fallout", fallout)):
            for table, table_sha in ((None, TABLE_SHA256), (reversed_table, reversed_sha)):
                deletions = (
                    [("seed", key) for key in ST_SEED_FIELDS]
                    + [("round", key) for key in ST_ROUND_FIELDS]
                    + [("twin", key) for key in ST_TWIN_FIELDS]
                )
                for level, key in deletions:
                    seed = json.loads(fixture)
                    target = {"seed": seed, "round": seed["rounds"][0], "twin": seed["twin"][0]}[level]
                    del target[key]
                    got_code, lines = run([seed], diag=good_diag, table=table, table_sha=table_sha)
                    tried += 1
                    if not (got_code == 2 and len(lines) == 1 and lines[0].startswith("E4_CLASS_CANNOT_CHECK ")):
                        bad.append("%s %s.%s%s exit %d" % (fixture_name, level, key, "" if table is None else " (reversed)", got_code))
        check("missing-each-field", not bad and tried == 3 * 2 * 41, "; ".join(bad) or "tried %d" % tried)
        expect("missing-twin-index", run([{**edited(page), "twin": []}]), 2)
        seed2 = edited(page)
        seed2["rounds"].append(copy.deepcopy(seed2["rounds"][0]))
        expect("missing-twin-index-second-round", run([seed2]), 2)
        expect("q-too-short", run([edited(page, {"q": q_zero[:15]})]), 2)
        expect("q-too-short-fallout", run([edited(fallout, {"q": []})], diag=good_diag), 2)
        expect("q-not-a-list", run([edited(page, {"q": {"15": 1}})]), 2)
        expect("pass-not-bool", run([edited(page, {"pass": 0})]), 2)
        expect("pass-not-bool-string", run([edited(page, {"pass": "false"})]), 2)
        expect("pass-not-bool-null", run([edited(page, {"pass": None})]), 2)
        expect("pass-not-bool-one", run([edited(page, {"pass": 1})]), 2)
        expect("pass-not-bool-twin", run([edited(page, twin_edit={"pass": 1})]), 2)
        expect("seed-flag-not-bool", run([edited(page, seed_edit={"deadlock": 0})]), 2)
        expect("seed-flag-null", run([edited(page, {"pass": True}, seed_edit={"panic": None})]), 2)
        expect("media-count-not-int", run([edited(page, {"lost": True})]), 2)
        expect("rounds-not-a-list", run([edited(page, seed_edit={"rounds": {}})]), 2)
        expect("duplicate-seed", run([edited(page), edited(page)]), 2)
        expect("seeds-entry-not-object", run([7]), 2)

        # --- Cannot check: the files themselves.
        path, sha = results_file([edited(page)])
        wrong = "0" * 64
        expect("results-sha-mismatch", run_match(path, wrong, None, real_table, TABLE_SHA256), 2)
        expect("results-sha-match", run_match(path, sha, None, real_table, TABLE_SHA256), 0, counts(1, 0, 0, 0))
        expect("results-missing", run_match(fresh(".absent"), sha, None, real_table, TABLE_SHA256), 2)
        for name, data in (("results-not-json", b"{"), ("results-no-seeds", b'{"e4":1}'), ("results-seeds-not-list", b'{"seeds":{}}'), ("results-top-not-object", b"[]")):
            raw = write(fresh(".json"), data)
            expect(name, run_match(raw, sha256_hex(data), None, real_table, TABLE_SHA256), 2)
        # A widened table (M-TABLE-WIDEN's edit) would take this round;
        # held to the frozen hash it is never read.
        widened, widened_sha = temp_table(lambda t: t["classes"][0]["round"].pop("pass_na"))
        widened_round = [edited(page, {"pass_na": False})]
        expect("table-sha-mismatch", run(widened_round, table=widened), 2)
        expect("table-sha-mismatch-own-sha-would-match", run(widened_round, table=widened, table_sha=widened_sha), 0, counts(1, 0, 0, 0))
        expect("table-missing", run([edited(page)], table=fresh(".absent")), 2)

        def twice(table):
            table["classes"].insert(1, {**copy.deepcopy(table["classes"][0]), "id": "refused_old_page_again"})

        doubled, doubled_sha = temp_table(twice)
        expect("ambiguous-class", run([edited(page)], table=doubled, table_sha=doubled_sha), 2)
        expect("ambiguous-class-other-round-is-fine", run([edited(header)], table=doubled, table_sha=doubled_sha), 0, "E4_CLASS_COUNTS refused_old_page=0 refused_old_page_again=0 refused_old_header=1 amcheck_subtree_fallout=0 unmatched=0")

        # --- The command line: stdout lines, the exit code, and the
        # table held to the constant whatever `--table` names.
        def cli(argv):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = main(argv)
            return code, out.getvalue().splitlines()

        path, sha = results_file([edited(page), edited(fallout)])
        base = ["match", "--results", path, "--results-sha256", sha]
        expect("cli-match-with-diag", cli(base + ["--diag-dir", good_diag]), 0, counts(1, 0, 1, 0))
        expect("cli-match-no-diag", cli(base), 1, counts(1, 0, 0, 1))
        expect("cli-table-held-to-constant", cli(base + ["--table", widened]), 2)
        expect("cli-results-sha-mismatch", cli(["match", "--results", path, "--results-sha256", wrong]), 2)
    finally:
        shutil.rmtree(tmp)

    print("SELF-TEST %d cases, %d failed" % (cases[0], len(failures)))
    for name in failures:
        print("SELF-TEST FAILED %s" % name)
    return 1 if failures else 0


class Parser(argparse.ArgumentParser):
    """A usage error is a cannot-check: exit 2 and the one line."""

    def error(self, message):
        print("E4_CLASS_CANNOT_CHECK usage: %s" % message)
        raise SystemExit(2)


def main(argv):
    """Argument parsing for the two subcommands."""
    ap = Parser(prog="e4_class_match.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True, parser_class=Parser)
    m = sub.add_parser("match", help="assign every failing record to a class of the frozen table")
    m.add_argument("--results", required=True)
    m.add_argument("--results-sha256", required=True)
    m.add_argument("--diag-dir")
    m.add_argument("--table")
    sub.add_parser("self-test", help="embedded literal cases")
    args = ap.parse_args(argv)
    if args.cmd == "match":
        return cmd_match(args)
    return self_test()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
