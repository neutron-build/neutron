#!/usr/bin/env python3
"""The X5-R51/X5-R52 comparator for E4 campaign JSON files (card
X-5c-F4-F1-F1-F3-F2-F1-F1).

This script owns three invariants, and nothing else:

1. `compare` reads a Base (reference) campaign JSON and one or more tip
   (run) campaign JSONs plus their `.campaign.out` files, and classifies
   every per-seed difference under X5-R51 (the step-4 movement
   exemption, quoted in the crate README) as amended by X5-R52 (the
   post-data amendment: twin DDL divergence, meta-only detections, MP-A
   under X5-R30 (c), and bullet 1 made testable). Its exit code is 0
   only if nothing outside the rule is found — necessary, never
   sufficient: the single-seed checks (`check-mp`, `check-twin`) belong
   to the gate, per X5-R51 (f) and X5-R52 (f).
2. `check-mp` and `check-twin` re-read one recorded single-seed
   diagnostic run (stderr, JSON and stdout) against one row of the run
   JSON and decide X5-R51 (b)5 / X5-R52 (c)(ii) (b)5' and X5-R52 (c)(i)
   T1–T5 exactly as the rules write them, from recorded fields only.
3. `self-test` runs embedded synthetic records and diag lines through
   the same engines and fails (non-zero exit) if any case reads
   differently.

Every classification is computed from recorded JSON fields, from the
`E4 SEEDS=… MONITOR=` lines of the run-out files, and from the recorded
diag stderr — never from a store, a model or a re-run. Deterministic:
seeds are iterated in sorted order and every list is printed in full.
Standard library only; run as `python3 -I`.

Usage:
  x5r52_compare.py compare --ref F [--ref-sha256 H] --run F ... \
      --run-out F ... [--run-sha256 H] [--range A..B] \
      [--no-twin-exemption] [--max-twin-failing N]
  x5r52_compare.py check-mp --json F --seed S --round I \
      --diag-err F --diag-json F --diag-out F
  x5r52_compare.py check-twin --json F --seed S --twin-round I \
      --diag-err F --diag-json F --diag-out F
  x5r52_compare.py self-test
"""

import argparse
import hashlib
import json
import re
import sys

# ---------------------------------------------------------------------------
# Recorded-field readers (shared by every subcommand).
# ---------------------------------------------------------------------------

#: The exact O-3 text of X5-R52 (c)(iii); anything else is a new failure.
MPA_O3 = "O-3(a): served corruption"

#: Page-file offsets of the two meta slots (S-STORE §2.2); a fault on
#: page slot 1 or 2 by `off`, or a MISDIRECTED `to_off` of these on
#: inode 0, is a meta-slot fault in `FaultTargets::collect`'s sense
#: (X5-R5's condition 1, counted by Q14/q[15]).
META_SLOT_OFFS = (4096, 8192)


def seed_index(doc):
    """Index a campaign JSON document by seed id.

    Raises ``ValueError`` on a duplicate seed (two run files covering the
    same seed cannot be merged and must stop the comparison).
    """
    out = {}
    for s in doc.get("seeds", ()):
        if s["seed"] in out:
            raise ValueError(f"duplicate seed {s['seed']}")
        out[s["seed"]] = s
    return out


def round_texts(r):
    """The round's (o1, o2, o3) triple as recorded."""
    return (r.get("o1"), r.get("o2"), r.get("o3"))


def round_has_text(r):
    """True iff any O-1/O-2/O-3 text is recorded on the round."""
    return any(t is not None for t in round_texts(r))


def round_o1a_only(r):
    """X5-R51 (b)3: o2 and o3 null and o1 begins with O-1(a)."""
    return (
        r.get("o2") is None
        and r.get("o3") is None
        and (r.get("o1") or "").startswith("O-1(a)")
    )


def media_fault_count(r):
    """The round's media-fault total: lost + mis + torn."""
    return r.get("lost", 0) + r.get("mis", 0) + r.get("torn", 0)


def seed_is_bad(s):
    """A bullet-3 seed shape: deadlock, panic, baton or a defect text."""
    return bool(
        s.get("deadlock")
        or s.get("panic")
        or s.get("baton")
        or s.get("defect") is not None
    )


def read_monitor(run_out_texts):
    """Sum the ``E4 SEEDS=… MONITOR=n`` counts over the run-out texts.

    X5-R51 (b)4: a non-zero count exempts nothing in that run. Each
    run-out must carry at least one E4 SEEDS line, else (b)4 cannot be
    checked and the comparison stops (exit 2).
    """
    total = 0
    seen = 0
    for text in run_out_texts:
        for m in re.finditer(r"E4 SEEDS=\d+ .*?MONITOR=(\d+)", text):
            total += int(m.group(1))
            seen += 1
    if seen == 0:
        raise ValueError("no E4 SEEDS line in the run-out files")
    return total


def sha256_of(path):
    """The file's sha256 hex digest (for --ref-sha256/--run-sha256)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# The compare engine (X5-R51 as amended by X5-R52).
# ---------------------------------------------------------------------------


def meta_only_eligible(r):
    """X5-R52 (c)(ii) JSON prefilter on a MOVED_MP_PENDING round row.

    `c` null and `frontier` 0 (recovery failed), `mrb=false`,
    `served=false`, q[15] (Q14, a meta-slot fault) 1, and q[12], q[13],
    q[14] (Q13: a fault on a referenced slot) all 0. The q order is the
    campaign JSON writer's (`campaign.rs`): q[12..14] is Q13, q[15] is
    Q14.
    """
    q = r.get("q", [])
    return (
        r.get("c") is None
        and r.get("frontier") == 0
        and r.get("mrb") is False
        and r.get("served") is False
        and len(q) > 15
        and q[15] == 1
        and q[12] == 0
        and q[13] == 0
        and q[14] == 0
    )


def mpa_pass_fail(base_r, tip_r, klass, monitor):
    """X5-R52 (c)(iii): the MP-A pass→fail check, exact from the JSON.

    The Base round was already a meta-rollback detected loss (pass=true,
    mrb=true, c null, q[15]=1, O-1(a) only); the tip round is its
    recorded-failure form (pass=false, served=true, mrb=true, c null,
    q[15]=1, o3 exactly `O-3(a): served corruption`, `(skx ∨ smx)` and
    `pass_na` exactly true — X5-R30 (c) rule 4's judgement); MONITOR 0.
    """
    if monitor != 0:
        return False
    if klass != "MP" or base_r is None:
        return False
    b = base_r
    t = tip_r
    q_ok = lambda r: len(r.get("q", [])) > 15 and r["q"][15] == 1
    return bool(
        b.get("pass") is True
        and b.get("mrb") is True
        and b.get("c") is None
        and q_ok(b)
        and b.get("o2") is None
        and b.get("o3") is None
        and (b.get("o1") or "").startswith("O-1(a)")
        and t.get("pass") is False
        and t.get("served") is True
        and t.get("mrb") is True
        and t.get("c") is None
        and q_ok(t)
        and t.get("o2") is None
        and t.get("o3") == MPA_O3
        and (t.get("skx") or t.get("smx"))
        and t.get("pass_na") is True
    )


def twin_ddl_prefilter(tip_twin_o1, klass, tip_rounds_before, monitor):
    """X5-R52 (c)(i) JSON prefilter on a bullet-1 twin entry.

    Class MW; the tip twin entry failed with an O-1(a) failed-recovery
    text that contains `published {}`; some real round `j < i` has
    lost+mis+torn ≥ 1 and either `cover_err` or `frontier < hi`; the
    run's MONITOR count is 0.
    """
    if monitor != 0 or klass != "MW":
        return False
    if not (tip_twin_o1 or "").startswith("O-1(a)"):
        return False
    if "published {}" not in (tip_twin_o1 or ""):
        return False
    for r in tip_rounds_before:
        if media_fault_count(r) >= 1 and (
            r.get("cover_err") or r.get("frontier", 0) < r.get("hi", 0)
        ):
            return True
    return False


def compare_reading(ref_seeds, run_seeds, monitor, twin_exemption=True):
    """Classify every per-seed difference; returns a row dict.

    Rows, per X5-R51 (d) and X5-R52 (d): moved_gained, moved_mp_pending,
    meta_only_eligible, mpa_pass_fail, twin_ddl_pending, moved_lost,
    healed, twin_healed, text_changed, twin_failing (tip), twin_failing
    base count, new_failing, bad_seeds, digests_moved, failing_rounds.
    Never netted; a row either exempts (its single-seed check decides),
    reports, or fails.
    """
    rows = {
        "digests_moved": [],
        "failing_rounds": 0,
        "monitor": monitor,
        "moved_gained": [],
        "moved_mp_pending": [],
        "meta_only_eligible": [],
        "mpa_pass_fail": [],
        "twin_ddl_pending": [],
        "moved_lost": [],
        "healed": [],
        "twin_healed": [],
        "text_changed": [],
        "twin_failing": [],
        "twin_failing_base": [],
        "new_failing": [],
        "bad_seeds": [],
    }
    for s in sorted(run_seeds):
        tip = run_seeds[s]
        base = ref_seeds.get(s)
        if base is None:
            # The range check (cmd_compare) makes this unreachable; a
            # run seed the ref lacks is still a new failure, never
            # silently dropped.
            rows["new_failing"].append((s, "seed", None, "not in ref"))
            continue
        if tip.get("digest") != base.get("digest"):
            rows["digests_moved"].append(s)
        if seed_is_bad(tip):
            rows["bad_seeds"].append(s)
            if not seed_is_bad(base):
                rows["new_failing"].append((s, "seed", None, "bullet3"))
        klass = tip.get("class")
        brounds = base.get("rounds", [])
        trounds = tip.get("rounds", [])
        rows["failing_rounds"] += sum(1 for r in trounds if not r.get("pass"))
        for i in range(max(len(brounds), len(trounds))):
            tr = trounds[i] if i < len(trounds) else None
            br = brounds[i] if i < len(brounds) else None
            if tr is None:
                # X5-R51 (c): a Base round with a text whose tip round
                # is missing — the reverse direction, classified on the
                # Base round alone.
                if br is not None and round_has_text(br):
                    c123 = klass in ("MP", "MW") and br.get("pass") is True and round_o1a_only(br)
                    mw5 = klass != "MW" or (br.get("x33x") and br.get("cover_err"))
                    (rows["moved_lost"] if (c123 and mw5) else rows["healed"]).append((s, i))
                continue
            b_pass = br.get("pass") if br is not None else True
            b_text = round_has_text(br) if br is not None else False
            # X5-R52 (b): bullet 1 first — pass true at the Base and
            # false at the tip, whatever text the Base round carried.
            if b_pass and not tr.get("pass"):
                if mpa_pass_fail(br, tr, klass, monitor):
                    rows["mpa_pass_fail"].append((s, i))
                else:
                    rows["new_failing"].append((s, "round", i, "bullet1"))
                continue
            # X5-R51 (a) bullet 2: text-free at the Base, a text here.
            if not b_text and round_has_text(tr):
                ok124 = (
                    klass in ("MP", "MW")
                    and tr.get("pass") is True
                    and round_o1a_only(tr)
                    and monitor == 0
                )
                if not ok124:
                    rows["new_failing"].append((s, "round", i, "bullet2-checks"))
                    continue
                if klass == "MP":
                    rows["moved_mp_pending"].append((s, i))
                    rows["moved_gained"].append((s, i))
                    if meta_only_eligible(tr):
                        rows["meta_only_eligible"].append((s, i))
                else:
                    if tr.get("x33x") and tr.get("cover_err"):
                        rows["moved_gained"].append((s, i))
                    else:
                        rows["new_failing"].append((s, "round", i, "bullet2-mw-json5"))
                continue
            # Reverse and text-change shapes (a tip round that exists).
            if b_text:
                if not round_has_text(tr) and tr.get("pass"):
                    c123 = klass in ("MP", "MW") and br.get("pass") is True and round_o1a_only(br)
                    mw5 = klass != "MW" or bool(br.get("x33x") and br.get("cover_err"))
                    (rows["moved_lost"] if (c123 and mw5) else rows["healed"]).append((s, i))
                    continue
                if br.get("pass") is False and tr.get("pass") is True and round_has_text(tr):
                    rows["healed"].append((s, i))
                    continue
                if tr.get("pass") is True and round_has_text(tr) and round_texts(br) != round_texts(tr):
                    rows["text_changed"].append((s, i))
                    continue
        # Twin entries: X5-R52 (b) extends bullet 1 to twins, and (c)(i)
        # is its only exemption; TWIN_HEALED is the reverse shape.
        btwins = base.get("twin", [])
        twins = tip.get("twin", [])
        for i, tt in enumerate(twins):
            if not tt.get("pass"):
                rows["twin_failing"].append((s, i))
            bt = btwins[i] if i < len(btwins) else None
            b_pass = bt.get("pass") if bt is not None else True
            if b_pass and not tt.get("pass"):
                if twin_exemption and twin_ddl_prefilter(
                    tt.get("o1"), klass, trounds[:i], monitor
                ):
                    rows["twin_ddl_pending"].append((s, i))
                else:
                    rows["new_failing"].append((s, "twin", i, "bullet1-twin"))
        for i, bt in enumerate(btwins):
            if not bt.get("pass"):
                tt = twins[i] if i < len(twins) else None
                if tt is None or tt.get("pass"):
                    rows["twin_healed"].append((s, i))
    return rows


def compare_exit_code(rows, max_twin_failing=None):
    """0 only if nothing outside the rule is found (X5-R52 (f))."""
    if rows["new_failing"] or rows["bad_seeds"]:
        return 1
    if max_twin_failing is not None and len(rows["twin_failing"]) > max_twin_failing:
        return 1
    return 0


def format_rows(rows, max_twin_failing=None):
    """The compare output: counts and full lists, rows in fixed order."""
    out = []
    out.append(
        "DIGESTS_MOVED %d FAILING_ROUNDS %d MONITOR %d"
        % (len(rows["digests_moved"]), rows["failing_rounds"], rows["monitor"])
    )
    names = [
        ("moved_gained", "MOVED_GAINED"),
        ("moved_mp_pending", "MOVED_MP_PENDING"),
        ("meta_only_eligible", "META_ONLY_ELIGIBLE"),
        ("mpa_pass_fail", "MPA_PASS_FAIL"),
        ("twin_ddl_pending", "TWIN_DDL_PENDING"),
        ("moved_lost", "MOVED_LOST"),
        ("healed", "HEALED"),
        ("twin_healed", "TWIN_HEALED"),
        ("text_changed", "TEXT_CHANGED"),
    ]
    for key, label in names:
        out.append("%s %d %s" % (label, len(rows[key]), rows[key]))
    out.append(
        "TWIN_FAILING %d %s BASE %d"
        % (len(rows["twin_failing"]), rows["twin_failing"], len(rows["twin_failing_base"]))
    )
    if max_twin_failing is not None and len(rows["twin_failing"]) > max_twin_failing:
        out.append("TWIN_FAILING_BUDGET_EXCEEDED max=%d" % max_twin_failing)
    out.append(
        "NEW_FAILING %d %s BAD_SEEDS %d %s"
        % (
            len(rows["new_failing"]),
            rows["new_failing"],
            len(rows["bad_seeds"]),
            rows["bad_seeds"],
        )
    )
    return "\n".join(out)


def cmd_compare(args):
    """The compare subcommand (files, hashes, range, then the engine)."""
    # Hash checks come before any reading (a mismatch exits 2).
    if args.ref_sha256 and sha256_of(args.ref) != args.ref_sha256:
        print("ERROR ref sha256 mismatch: %s" % args.ref, file=sys.stderr)
        return 2
    if args.run_sha256:
        if len(args.run) != 1:
            print("ERROR --run-sha256 needs exactly one --run", file=sys.stderr)
            return 2
        if sha256_of(args.run[0]) != args.run_sha256:
            print("ERROR run sha256 mismatch: %s" % args.run[0], file=sys.stderr)
            return 2
    ref_doc = json.load(open(args.ref))
    ref_all = seed_index(ref_doc)
    run_seeds = {}
    for path in args.run:
        run_seeds.update(seed_index(json.load(open(path))))
    run_out_texts = [open(p).read() for p in args.run_out]
    try:
        monitor = read_monitor(run_out_texts)
    except ValueError as e:
        print("ERROR %s" % e, file=sys.stderr)
        return 2
    lo, hi = args.range if args.range else (0, 10000)
    ref_seeds = {s: v for s, v in ref_all.items() if lo <= s < hi}
    if set(run_seeds) != set(ref_seeds):
        only_run = sorted(set(run_seeds) - set(ref_seeds))
        only_ref = sorted(set(ref_seeds) - set(run_seeds))
        print(
            "ERROR range mismatch (%d..%d): run-only %d %s ref-only %d %s"
            % (lo, hi, len(only_run), only_run[:20], len(only_ref), only_ref[:20]),
            file=sys.stderr,
        )
        return 2
    try:
        rows = compare_reading(ref_seeds, run_seeds, monitor, not args.no_twin_exemption)
    except ValueError as e:
        print("ERROR %s" % e, file=sys.stderr)
        return 2
    # The base twin-failing count (class size at the Base end).
    rows["twin_failing_base"] = [
        (s, i)
        for s in sorted(ref_seeds)
        for i, t in enumerate(ref_seeds[s].get("twin", []))
        if not t.get("pass")
    ]
    rows["monitor"] = monitor
    print(format_rows(rows, args.max_twin_failing))
    return compare_exit_code(rows, args.max_twin_failing)


# ---------------------------------------------------------------------------
# Diag-line parsers (check-mp, check-twin).
# ---------------------------------------------------------------------------

#: One `MPE diag: === seed S round R class K … ===` block header.
BLOCK_RE = re.compile(
    r"=== seed (\d+) round (\d+) class (\w+) .*===\s*$"
)
#: The amcheck (i) findings line inside a block.
FINDINGS_LINE_RE = re.compile(r"amcheck_i findings (.*?) image_slots \{.*?\} tainted (\{[^}]*\}) meta_k (\S+)")
#: One Rust-debug `Finding { slot: …, what: "…" }` entry.
FINDING_RE = re.compile(r"Finding \{ slot: (Some\((\d+)\)|None), what: \"(.*?)\" \}")
#: One `MPE diag: media[i] KIND inode N off O len L (page slot S)` line;
#: the kind is captured whole (`Lost`, `Torn { sectors: […] }`,
#: `Misdirected { to_off: N }`), and `to_off` is picked out separately.
MEDIA_RE = re.compile(r"media\[(\d+)\] (.*?) inode (\d+) off (\d+) len (\d+)")
TO_OFF_RE = re.compile(r"to_off: (\d+)")


def mpe_blocks(diag_text):
    """Split MPE diag stderr into `{(seed, round, class): [lines]}`.

    A block runs from its `=== seed s round i class K ===` header to the
    next header; every amcheck/media line inside belongs to it.
    """
    blocks = {}
    current = None
    for line in diag_text.splitlines():
        m = BLOCK_RE.search(line)
        if m:
            current = (int(m.group(1)), int(m.group(2)), m.group(3))
            blocks.setdefault(current, [])
            continue
        if current is not None:
            blocks[current].append(line)
    return blocks


def parse_findings(line):
    """Parse the findings of one amcheck_i line into `(slot, what)` pairs.

    `slot` is `None` or the int inside `Some(n)`; `what` is the raw
    diagnostic text.
    """
    out = []
    for m in FINDING_RE.finditer(line):
        slot = int(m.group(2)) if m.group(2) is not None else None
        out.append((slot, m.group(3)))
    return out


def parse_set(text):
    """Parse a Rust `{:?}` BTreeSet (`{1, 2}` / `{}`) into a list of ints."""
    inner = text.strip().lstrip("{").rstrip("}").strip()
    return [int(x) for x in inner.split(",")] if inner else []


def media_lines(block_lines):
    """The block's media fault lines, as parsed tuples.

    Each is `(kind, to_off_or_None, inode, off, len)`; `kind` is the raw
    kind text (``Misdirected { to_off: N }`` for a misdirected fault).
    """
    out = []
    for line in block_lines:
        m = MEDIA_RE.search(line)
        if m:
            kind = m.group(2)
            tm = TO_OFF_RE.search(kind)
            to_off = int(tm.group(1)) if tm else None
            out.append((kind, to_off, int(m.group(3)), int(m.group(4)), int(m.group(5))))
    return out


def meta_slot_fault_on_page_1_or_2(media):
    """X5-R52 (c)(ii) (b)5′ media check, `FaultTargets::collect`'s rule.

    At least one fault on page slot 1 or 2: `off` 4096 or 8192, or a
    Misdirected `to_off` of 4096 or 8192 on inode 0 (the page file).
    """
    for _kind, to_off, inode, off, _len in media:
        if inode == 0 and off in META_SLOT_OFFS:
            return True
        if inode == 0 and to_off in META_SLOT_OFFS:
            return True
    return False


def t5_checks(run_seed, diag_seed, diag_out_text):
    """X5-R52 (c)(i)/(ii) T5: digest equality and the run's MONITOR=0.

    Returns ``None`` when both hold, else a reason string.
    """
    if run_seed is None:
        return "T5 seed missing from run json"
    if diag_seed is None:
        return "T5 seed missing from diag json"
    if run_seed.get("digest") != diag_seed.get("digest"):
        return "T5 digest %s != %s" % (diag_seed.get("digest"), run_seed.get("digest"))
    try:
        monitor = read_monitor([diag_out_text])
    except ValueError:
        return "T5 no E4 SEEDS line"
    if monitor != 0:
        return "T5 monitor=%d" % monitor
    return None


def check_mp(run_seeds, seed, rnd, diag_err_text, diag_seeds, diag_out_text):
    """X5-R51 (b)5 / X5-R52 (c)(ii) (b)5′ for one MOVED_MP_PENDING row.

    Returns ``(verdict, detail)`` where verdict is ``"EXPLAINED (b)5"``,
    ``"EXPLAINED (b)5'"`` or ``"NOT-EXPLAINED"``; (b)5′ is allowed only
    when the JSON row is META_ONLY_ELIGIBLE.
    """
    run_seed = run_seeds.get(seed)
    reason = t5_checks(run_seed, diag_seeds.get(seed), diag_out_text)
    if reason:
        return ("NOT-EXPLAINED", reason)
    rounds = run_seed.get("rounds", [])
    if rnd >= len(rounds):
        return ("NOT-EXPLAINED", "no round %d in run json" % rnd)
    row = rounds[rnd]
    if row.get("pass") is not True:
        return ("NOT-EXPLAINED", "pass is not true")
    blocks = mpe_blocks(diag_err_text)
    block = blocks.get((seed, rnd, "Mp"))
    if block is None:
        return ("NOT-EXPLAINED", "no `=== seed %d round %d class Mp ===` block" % (seed, rnd))
    flines = [l for l in block if "amcheck_i findings" in l]
    if not flines:
        return ("NOT-EXPLAINED", "no amcheck_i findings line in the block")
    m = FINDINGS_LINE_RE.search(flines[0])
    if not m:
        return ("NOT-EXPLAINED", "unparsable amcheck_i findings line")
    findings = parse_findings(flines[0])
    tainted = parse_set(m.group(2))
    meta_k = m.group(3)
    if meta_only_eligible(row):
        # (b)5′: every finding None with `no valid meta`, empty tainted,
        # meta_k None, and a meta-slot fault among the media lines.
        if not findings:
            return ("NOT-EXPLAINED", "(b)5' empty findings")
        for slot, what in findings:
            if slot is not None:
                return ("NOT-EXPLAINED", "(b)5' finding slot Some(%d)" % slot)
            if not what.startswith("no valid meta"):
                return ("NOT-EXPLAINED", "(b)5' finding what %r" % what)
        if tainted != []:
            return ("NOT-EXPLAINED", "(b)5' tainted %s" % tainted)
        if meta_k != "None":
            return ("NOT-EXPLAINED", "(b)5' meta_k %s" % meta_k)
        if not meta_slot_fault_on_page_1_or_2(media_lines(block)):
            return ("NOT-EXPLAINED", "(b)5' no meta-slot fault on page slot 1 or 2")
        return ("EXPLAINED (b)5'", "%d finding(s), media %s" % (len(findings), media_summary(block)))
    # (b)5 exactly as X5-R51 wrote it.
    if not findings:
        return ("NOT-EXPLAINED", "(b)5 empty findings")
    if not tainted:
        return ("NOT-EXPLAINED", "(b)5 empty tainted")
    for slot, _what in findings:
        if slot is None:
            return ("NOT-EXPLAINED", "(b)5 finding slot None")
        if slot not in tainted:
            return ("NOT-EXPLAINED", "(b)5 finding slot %d outside tainted %s" % (slot, tainted))
    return (
        "EXPLAINED (b)5",
        "%d finding(s) slots %s tainted %s" % (len(findings), [s for s, _ in findings], tainted),
    )


def media_summary(block_lines):
    """A short deterministic summary of the block's media fault lines."""
    out = []
    for kind, to_off, inode, off, _len in media_lines(block_lines):
        if to_off is not None:
            out.append("Misdirected to_off=%d inode=%d off=%d" % (to_off, inode, off))
        else:
            out.append("%s inode=%d off=%d" % (kind, inode, off))
    return "[%s]" % ", ".join(out)


#: The recovery-Err diag line (the GATEDIAG patch, applied to a scratch
#: export only): `GATEDIAG recovery Err seed=S round=R class=K … e=ERR`.
RECOVERY_ERR_RE = re.compile(
    r"GATEDIAG recovery Err seed=(\d+) round=(\d+) class=(\w+) nested=(\d+) e=(.*)"
)
#: The X5R52 round-start line (same patch).
ROUND_START_RE = re.compile(
    r"X5R52 round-start seed=(\d+) round=(\d+) real_dir=\[([0-9, ]*)\] twin_dir=\[([0-9, ]*)\]"
)
#: The X5R52 run marker (same patch).
RUN_MARKER_RE = re.compile(r"X5R52 run seed=(\d+) round=(\d+) class=(\w+)")
#: One replay-listing record line: `GATEDIAG   rec lsn=L kind=K ts=T
#: txn=(…) ddl=[…] trees={…}`.
REC_LINE_RE = re.compile(r"GATEDIAG   rec lsn=(\d+) kind=(\d+) ts=(\d+) txn=\([^)]*\) ddl=(\[.*?\])")


def parse_dir_list(text):
    """Parse the round-start lines' `[1, 2, 3]` directory key lists."""
    inner = text.strip()
    return [int(x) for x in inner.split(",")] if inner else []


def check_twin(run_seeds, seed, twin_round, diag_err_text, diag_seeds, diag_out_text):
    """X5-R52 (c)(i) T1–T5 for one TWIN_DDL_PENDING row.

    Returns ``(verdict, detail)``; verdict is ``"EXPLAINED"`` (detail
    ``N=<n> k=<k>``) or ``"NOT-EXPLAINED"`` (detail names the failed
    check).
    """
    run_seed = run_seeds.get(seed)
    reason = t5_checks(run_seed, diag_seeds.get(seed), diag_out_text)
    if reason:
        return ("NOT-EXPLAINED", reason)
    # T1: the class-C (twin) recovery Err line for this round is exactly
    # `Corrupt("tree N missing in directory")`.
    n = None
    err_seen = []
    for line in diag_err_text.splitlines():
        m = RECOVERY_ERR_RE.search(line)
        if not m:
            continue
        if int(m.group(1)) != seed or int(m.group(2)) != twin_round:
            continue
        if m.group(3) != "C":
            continue
        err_seen.append(m.group(5))
        cm = re.fullmatch(r'Corrupt\("tree (\d+) missing in directory"\)', m.group(5))
        if cm:
            n = int(cm.group(1))
    if n is None:
        return ("NOT-EXPLAINED", "T1 no class-C Corrupt(\"tree N missing in directory\") for round %d (saw %s)" % (twin_round, err_seen[:3] or "nothing"))
    # T2: the round-start line for this round has N in real_dir and N
    # absent from twin_dir.
    starts = []
    for line in diag_err_text.splitlines():
        m = ROUND_START_RE.search(line)
        if m and int(m.group(1)) == seed:
            starts.append((int(m.group(2)), parse_dir_list(m.group(3)), parse_dir_list(m.group(4))))
    line_i = [x for x in starts if x[0] == twin_round]
    if not line_i:
        return ("NOT-EXPLAINED", "T2 no round-start line for round %d" % twin_round)
    real_dir, twin_dir = line_i[0][1], line_i[0][2]
    if n not in real_dir or n in twin_dir:
        return (
            "NOT-EXPLAINED",
            "T2 round %d real_dir=%s twin_dir=%s (N=%d)" % (twin_round, real_dir, twin_dir, n),
        )
    # T3: the twin chain dropped N itself — a class-C replay listing of
    # this seed, at a round ≤ i, shows a commit record with `ddl`
    # containing `D<N>`.
    seg_class = None
    seg_round = None
    dropped = False
    for line in diag_err_text.splitlines():
        m = RUN_MARKER_RE.search(line)
        if m:
            seg_class = m.group(3)
            seg_round = int(m.group(2))
            continue
        rm = REC_LINE_RE.search(line)
        if rm and seg_class == "C" and seg_round is not None and seg_round <= twin_round:
            if int(rm.group(2)) == 1 and ('"D%d"' % n) in rm.group(4):
                dropped = True
    if not dropped:
        return ("NOT-EXPLAINED", "T3 no class-C commit record with D%d at a round <= %d" % (n, twin_round))
    # T4: k is the first round start at which N is in real_dir and not in
    # twin_dir; k ≥ 1, and real round k−1 has a recorded media fault and
    # a recovery that stopped short.
    div = [r for r, rd, td in starts if n in rd and n not in td]
    if not div:
        return ("NOT-EXPLAINED", "T4 no divergent round start")
    k = min(div)
    if k < 1:
        return ("NOT-EXPLAINED", "T4 k=%d" % k)
    rounds = run_seed.get("rounds", [])
    if k - 1 >= len(rounds):
        return ("NOT-EXPLAINED", "T4 no real round %d in run json" % (k - 1))
    prev = rounds[k - 1]
    if media_fault_count(prev) < 1:
        return ("NOT-EXPLAINED", "T4 real round %d has no media fault" % (k - 1))
    if not (prev.get("cover_err") or prev.get("frontier", 0) < prev.get("hi", 0)):
        return (
            "NOT-EXPLAINED",
            "T4 real round %d cover_err=%s frontier=%d hi=%d"
            % (k - 1, prev.get("cover_err"), prev.get("frontier"), prev.get("hi")),
        )
    return (
        "EXPLAINED",
        "N=%d k=%d (real round %d: lost=%d mis=%d torn=%d cover_err=%s frontier=%d hi=%d)"
        % (
            n,
            k,
            k - 1,
            prev.get("lost", 0),
            prev.get("mis", 0),
            prev.get("torn", 0),
            prev.get("cover_err"),
            prev.get("frontier"),
            prev.get("hi"),
        ),
    )


# ---------------------------------------------------------------------------
# Subcommand entry points.
# ---------------------------------------------------------------------------


def cmd_check_mp(args):
    """The check-mp subcommand."""
    run_seeds = seed_index(json.load(open(args.json)))
    diag_seeds = seed_index(json.load(open(args.diag_json)))
    diag_err_text = open(args.diag_err).read()
    diag_out_text = open(args.diag_out).read()
    verdict, detail = check_mp(run_seeds, args.seed, args.round, diag_err_text, diag_seeds, diag_out_text)
    print("%s %s" % (verdict, detail))
    return 0 if verdict.startswith("EXPLAINED") else 1


def cmd_check_twin(args):
    """The check-twin subcommand."""
    run_seeds = seed_index(json.load(open(args.json)))
    diag_seeds = seed_index(json.load(open(args.diag_json)))
    diag_err_text = open(args.diag_err).read()
    diag_out_text = open(args.diag_out).read()
    verdict, detail = check_twin(run_seeds, args.seed, args.twin_round, diag_err_text, diag_seeds, diag_out_text)
    print("%s %s" % (verdict, detail))
    return 0 if verdict.startswith("EXPLAINED") else 1


# ---------------------------------------------------------------------------
# self-test: embedded synthetic records and diag lines, through the same
# engines. One PASS/FAIL line per case; non-zero exit on any FAIL.
# ---------------------------------------------------------------------------


def round_rec(**kw):
    """A minimal round record with test-friendly defaults (pass=true)."""
    base = {
        "crash": "uniform@y:1",
        "lost": 0,
        "mis": 0,
        "torn": 0,
        "lo": 1,
        "hi": 1,
        "c": 1,
        "frontier": 1,
        "o1": None,
        "o2": None,
        "o3": None,
        "served": False,
        "mrb": False,
        "skx": False,
        "smx": False,
        "pass_na": None,
        "cover_err": False,
        "x33x": False,
        "pass": True,
        "q": [0] * 21,
    }
    base.update(kw)
    return base


def seed_rec(seed, klass, rounds, twin=None):
    """A minimal seed record."""
    return {
        "seed": seed,
        "class": klass,
        "knobs": 0,
        "digest": 1000 + seed,
        "deadlock": False,
        "panic": False,
        "baton": False,
        "stopped": False,
        "defect": None,
        "rounds": rounds,
        "twin": twin if twin is not None else [],
    }


def self_test():
    """Run every still-caught bullet of X5-R52 (c) through the engines."""
    failures = []
    cases = [0]

    def check(name, ok, why=""):
        cases[0] += 1
        print("%s %s%s" % ("PASS" if ok else "FAIL", name, "" if ok else " " + why))
        if not ok:
            failures.append(name)

    def rows_of(ref_seeds, run_seeds, monitor=0, twin_exemption=True):
        return compare_reading(ref_seeds, run_seeds, monitor, twin_exemption)

    o1a = "O-1(a) at c=5: published {} != model {2, 3}"

    # --- X5-R51 bullet-2 MW gains, with and without the JSON half of (b)5.
    ref = {1: seed_rec(1, "MW", [round_rec()])}
    run = {1: seed_rec(1, "MW", [round_rec(o1=o1a, x33x=True, cover_err=True, crash="uniform@y:2")], twin=[{"pass": True, "o1": None}])}
    r = rows_of(ref, run)
    check("mw-gain-explained", r["moved_gained"] == [(1, 0)] and r["new_failing"] == [], str(r))

    run = {1: seed_rec(1, "MW", [round_rec(o1=o1a, x33x=False, cover_err=False)], twin=[{"pass": True, "o1": None}])}
    r = rows_of(ref, run)
    check("mw-gain-unexplained", r["moved_gained"] == [] and r["new_failing"] == [(1, "round", 0, "bullet2-mw-json5")], str(r))

    # --- MONITOR > 0 exempts nothing: the gain is a new failure.
    run = {1: seed_rec(1, "MW", [round_rec(o1=o1a, x33x=True, cover_err=True)], twin=[{"pass": True, "o1": None}])}
    r = rows_of(ref, run, monitor=2)
    check("monitor-nonzero-gain-is-new", r["moved_gained"] == [] and (1, "round", 0, "bullet2-checks") in r["new_failing"], str(r))

    # --- X5-R52 (c)(iii): MP-A pass→fail, and every still-caught bullet.
    mpa_base = round_rec(o1=o1a, mrb=True, c=None, frontier=0)
    mpa_base["q"][15] = 1
    mpa_tip = round_rec(
        o1="O-1(a) at c=15: published {} != model {2, 3, 4}",
        o3=MPA_O3,
        pass_na=True,
        smx=True,
        served=True,
        mrb=True,
        c=None,
        frontier=0,
    )
    mpa_tip["pass"] = False
    mpa_tip["q"][15] = 1
    ref = {1: seed_rec(1, "MP", [mpa_base])}
    run = {1: seed_rec(1, "MP", [mpa_tip])}
    r = rows_of(ref, run)
    check("mpa-pass-fail", r["mpa_pass_fail"] == [(1, 0)] and r["new_failing"] == [], str(r))

    for name, mutate in (
        ("mpa-skx-smx-false", lambda t: t.update(skx=False, smx=False)),
        ("mpa-pass-na-false", lambda t: t.update(pass_na=False)),
        ("mpa-o2-text", lambda t: t.update(o2="O-2: store diverged")),
        ("mpa-o3-other", lambda t: t.update(o3="O-3(b): uncovered accept")),
    ):
        tip = json.loads(json.dumps(mpa_tip))
        mutate(tip)
        run = {1: seed_rec(1, "MP", [tip])}
        r = rows_of(ref, run)
        check(name, r["mpa_pass_fail"] == [] and (1, "round", 0, "bullet1") in r["new_failing"], str(r))

    for name, mutate in (
        ("mpa-base-text-free", lambda b: b.update(o1=None)),
        ("mpa-base-mrb-false", lambda b: b.update(mrb=False)),
        ("mpa-base-c-nonnull", lambda b: b.update(c=5)),
    ):
        base = json.loads(json.dumps(mpa_base))
        mutate(base)
        ref2 = {1: seed_rec(1, "MP", [base])}
        run = {1: seed_rec(1, "MP", [json.loads(json.dumps(mpa_tip))])}
        r = rows_of(ref2, run)
        check(name, r["mpa_pass_fail"] == [] and (1, "round", 0, "bullet1") in r["new_failing"], str(r))

    # A class MW serve never moves (X5-R30 (c) rule 3).
    ref2 = {1: seed_rec(1, "MW", [json.loads(json.dumps(mpa_base))])}
    run = {1: seed_rec(1, "MW", [json.loads(json.dumps(mpa_tip))])}
    r = rows_of(ref2, run)
    check("mpa-mw-serve", r["mpa_pass_fail"] == [] and (1, "round", 0, "bullet1") in r["new_failing"], str(r))

    # --- X5-R52 (b): a pass→fail with a Base text that is not MP-A
    # (the gap of the original comparator: bullet 1 for rounds that
    # already carried a text).
    base = round_rec(o1=o1a, mrb=False, c=None, frontier=0)
    tip = round_rec(o1="O-1(a) at c=9: published {} != model {2}", c=None, frontier=0)
    tip["pass"] = False
    ref = {1: seed_rec(1, "MP", [base])}
    run = {1: seed_rec(1, "MP", [tip])}
    r = rows_of(ref, run)
    check("pass-fail-base-text-not-mpa", r["mpa_pass_fail"] == [] and (1, "round", 0, "bullet1") in r["new_failing"], str(r))

    # --- X5-R52 (c)(i) prefilter and its still-caught bullets.
    def twin_case(klass, real_rounds, twin, base_twin=None):
        ref = {7: seed_rec(7, klass, [round_rec() for _ in real_rounds], twin=base_twin or [{"pass": True, "o1": None} for _ in real_rounds])}
        run = {7: seed_rec(7, klass, real_rounds, twin=twin)}
        return ref, run

    faulted = round_rec(lost=1, mis=0, torn=1, frontier=1, hi=5, cover_err=False)
    twins_ok = [{"pass": True, "o1": None}, {"pass": False, "o1": "O-1(a) at c=12: published {} != model {2}"}]
    ref, run = twin_case("MW", [faulted, round_rec()], twins_ok)
    r = rows_of(ref, run)
    check("twin-pending", r["twin_ddl_pending"] == [(7, 1)] and r["new_failing"] == [], str(r))
    r = rows_of(ref, run, twin_exemption=False)
    check("no-twin-exemption-pending-is-new", r["twin_ddl_pending"] == [] and (7, "twin", 1, "bullet1-twin") in r["new_failing"], str(r))

    ref, run = twin_case("MP", [faulted, round_rec()], twins_ok)
    r = rows_of(ref, run)
    check("twin-mp-seed-prefilter-fails", r["twin_ddl_pending"] == [] and (7, "twin", 1, "bullet1-twin") in r["new_failing"], str(r))

    ref, run = twin_case("MW", [faulted, round_rec()], [{"pass": True, "o1": None}, {"pass": False, "o1": "O-2: store diverged"}])
    r = rows_of(ref, run)
    check("twin-o2-text-prefilter-fails", r["twin_ddl_pending"] == [] and (7, "twin", 1, "bullet1-twin") in r["new_failing"], str(r))

    ref, run = twin_case("MW", [faulted, round_rec()], [{"pass": True, "o1": None}, {"pass": False, "o1": "O-1(a) at c=3: checksum mismatch on slot 9"}])
    r = rows_of(ref, run)
    check("twin-o1-not-recovery-prefilter-fails", r["twin_ddl_pending"] == [] and (7, "twin", 1, "bullet1-twin") in r["new_failing"], str(r))

    clean = round_rec(lost=0, mis=0, torn=0, frontier=5, hi=5)
    ref, run = twin_case("MW", [clean, round_rec()], twins_ok)
    r = rows_of(ref, run)
    check("twin-no-media-fault-prefilter-fails", r["twin_ddl_pending"] == [] and (7, "twin", 1, "bullet1-twin") in r["new_failing"], str(r))

    # --- A tip seed with deadlock is a BAD_SEED (bullet 3).
    ref = {1: seed_rec(1, "C", [round_rec()])}
    bad = seed_rec(1, "C", [round_rec()])
    bad["deadlock"] = True
    run = {1: bad}
    r = rows_of(ref, run)
    check("deadlock-tip-seed", r["bad_seeds"] == [1] and (1, "seed", None, "bullet3") in r["new_failing"], str(r))

    # --- check-mp: (b)5, and (b)5′ with its still-caught bullets.
    def mp_case(row_mutate=None, findings="Finding { slot: Some(9), what: \"§3.3 fail on mapped page\" }", tainted="{9}", meta_k="Some(1)", media="MPE diag: media[0] Lost inode 0 off 589824 len 4096 (page slot 144)"):
        row = round_rec(o1=o1a)
        if row_mutate:
            row_mutate(row)
        run_seed = seed_rec(3, "MP", [row])
        diag_seed = seed_rec(3, "MP", [dict(row)])
        block = [
            "MPE diag: === seed 3 round 0 class Mp crash Some(Hook { ckpt_index: 1, hook: After1 }) ===",
            media,
            "MPE diag: amcheck_i findings [%s] image_slots {1, 9} tainted %s meta_k %s" % (findings, tainted, meta_k),
        ]
        diag = "\n".join(block) + "\nMPE diag: === seed 3 round 1 class C crash None ===\n"
        out_text = "E4 SEEDS=1 O1_FAIL=1 O2_FAIL=0 O3_FAIL=0 DEADLOCK=0 PANIC=0 O1F_FAIL=1 MONITOR=0\n"
        return run_seed, diag_seed, diag, out_text

    def q15(row):
        row["q"][15] = 1
        row["c"] = None
        row["frontier"] = 0

    run_seed, diag_seed, diag, out_text = mp_case()
    v, d = check_mp({3: run_seed}, 3, 0, diag, {3: diag_seed}, out_text)
    check("mp-b5-explained", v == "EXPLAINED (b)5", "%s %s" % (v, d))

    run_seed, diag_seed, diag, out_text = mp_case(row_mutate=q15, findings="Finding { slot: None, what: \"no valid meta (G7: a misplaced valid meta outranks every placed one)\" }", tainted="{}", meta_k="None", media="MPE diag: media[0] Misdirected { to_off: 4096 } inode 0 off 8192 len 4096 (page slot 2)")
    v, d = check_mp({3: run_seed}, 3, 0, diag, {3: diag_seed}, out_text)
    check("meta-only-eligible", v == "EXPLAINED (b)5'", "%s %s" % (v, d))

    # q[15]=0 with the same diag: not eligible, (b)5 applies and fails.
    run_seed, diag_seed, diag, out_text = mp_case(row_mutate=lambda r: r.update(c=None, frontier=0), findings="Finding { slot: None, what: \"no valid meta\" }", tainted="{}", meta_k="None", media="MPE diag: media[0] Lost inode 0 off 4096 len 4096 (page slot 1)")
    v, d = check_mp({3: run_seed}, 3, 0, diag, {3: diag_seed}, out_text)
    check("meta-only-q15-zero", v == "NOT-EXPLAINED" and d.startswith("(b)5"), "%s %s" % (v, d))

    # A referenced-slot fault (q[12] non-zero) makes it ineligible too.
    def q12_15(row):
        row["q"][12] = 1
        row["q"][15] = 1
        row["c"] = None
        row["frontier"] = 0

    run_seed, diag_seed, diag, out_text = mp_case(row_mutate=q12_15, findings="Finding { slot: None, what: \"no valid meta\" }", tainted="{}", meta_k="None", media="MPE diag: media[0] Lost inode 0 off 4096 len 4096 (page slot 1)")
    v, d = check_mp({3: run_seed}, 3, 0, diag, {3: diag_seed}, out_text)
    check("meta-only-referenced-slot", v == "NOT-EXPLAINED" and d.startswith("(b)5"), "%s %s" % (v, d))

    # mrb=true / served=true are not eligible either.
    for name, mut in (
        ("meta-only-mrb-true", lambda r: r.update(mrb=True)),
        ("meta-only-served-true", lambda r: r.update(served=True)),
    ):
        def m(row, mut=mut):
            q15(row)
            mut(row)
        run_seed, diag_seed, diag, out_text = mp_case(row_mutate=m, findings="Finding { slot: None, what: \"no valid meta\" }", tainted="{}", meta_k="None", media="MPE diag: media[0] Lost inode 0 off 4096 len 4096 (page slot 1)")
        v, d = check_mp({3: run_seed}, 3, 0, diag, {3: diag_seed}, out_text)
        check(name, v == "NOT-EXPLAINED" and d.startswith("(b)5"), "%s %s" % (v, d))

    # Eligible but the diag disagrees: a Some(n) finding, or a None
    # finding whose text is not "no valid meta".
    run_seed, diag_seed, diag, out_text = mp_case(row_mutate=q15, findings="Finding { slot: Some(9), what: \"§3.3 fail on mapped page\" }", tainted="{}", meta_k="None", media="MPE diag: media[0] Lost inode 0 off 4096 len 4096 (page slot 1)")
    v, d = check_mp({3: run_seed}, 3, 0, diag, {3: diag_seed}, out_text)
    check("meta-only-some-finding", v == "NOT-EXPLAINED" and "slot Some(9)" in d, "%s %s" % (v, d))

    run_seed, diag_seed, diag, out_text = mp_case(row_mutate=q15, findings="Finding { slot: None, what: \"checksum mismatch on meta\" }", tainted="{}", meta_k="None", media="MPE diag: media[0] Lost inode 0 off 4096 len 4096 (page slot 1)")
    v, d = check_mp({3: run_seed}, 3, 0, diag, {3: diag_seed}, out_text)
    check("meta-only-other-none-text", v == "NOT-EXPLAINED" and "what" in d, "%s %s" % (v, d))

    # Eligible but no media fault on page slot 1 or 2.
    run_seed, diag_seed, diag, out_text = mp_case(row_mutate=q15, findings="Finding { slot: None, what: \"no valid meta\" }", tainted="{}", meta_k="None", media="MPE diag: media[0] Lost inode 0 off 589824 len 4096 (page slot 144)")
    v, d = check_mp({3: run_seed}, 3, 0, diag, {3: diag_seed}, out_text)
    check("meta-only-no-meta-slot-fault", v == "NOT-EXPLAINED" and "no meta-slot fault" in d, "%s %s" % (v, d))

    # pass=false is bullet 1 at compare level, never (ii).
    row = round_rec(o1=o1a)
    q15(row)
    row["pass"] = False
    run = {3: seed_rec(3, "MP", [row])}
    ref = {3: seed_rec(3, "MP", [round_rec()])}
    r = rows_of(ref, run)
    check("meta-only-pass-false-is-bullet1", (3, "round", 0, "bullet1") in r["new_failing"] and r["meta_only_eligible"] == [], str(r))

    # --- check-twin: T1–T5 and the still-caught bullets.
    def twin_diag(
        err='Corrupt("tree 4 missing in directory")',
        starts=None,
        listings=None,
        digest=1004,
    ):
        starts = starts if starts is not None else [(0, [1, 2, 3, 4], [1, 2, 3, 4]), (1, [1, 2, 3, 4], [1, 2, 3])]
        listings = listings if listings is not None else ["GATEDIAG   rec lsn=4183 kind=1 ts=3 txn=(0, 3) ddl=[\"D4\"] trees={1, 4}"]
        lines = []
        # round 0: class-C twin run with the drop listing.
        lines.append("X5R52 run seed=4 round=0 class=C")
        lines.append("GATEDIAG replay start meta.t=0 ckpt_id=0 dir_root=1 wal_start=0 dir_keys=[] nrec=26")
        lines.extend(listings)
        lines.append("X5R52 run seed=4 round=0 class=Mw")
        lines.append("X5R52 round-start seed=4 round=0 real_dir=%s twin_dir=%s" % (fmt_dir(starts[0][1]), fmt_dir(starts[0][2])))
        lines.append("X5R52 run seed=4 round=1 class=C")
        lines.append("GATEDIAG recovery Err seed=4 round=1 class=C nested=0 e=%s" % err)
        lines.append("X5R52 run seed=4 round=1 class=Mw")
        lines.append("X5R52 round-start seed=4 round=1 real_dir=%s twin_dir=%s" % (fmt_dir(starts[1][1]), fmt_dir(starts[1][2])))
        diag_seed = {"seed": 4, "class": "MW", "digest": digest, "rounds": [], "twin": []}
        out_text = "E4 SEEDS=1 O1_FAIL=0 O2_FAIL=0 O3_FAIL=0 DEADLOCK=0 PANIC=0 O1F_FAIL=0 MONITOR=0\n"
        return "\n".join(lines) + "\n", diag_seed, out_text

    def fmt_dir(keys):
        return "[%s]" % ", ".join(str(k) for k in keys)

    run_seed = seed_rec(4, "MW", [round_rec(lost=0, mis=1, torn=0, frontier=1, hi=7), round_rec()])
    diag, diag_seed, out_text = twin_diag()
    v, d = check_twin({4: run_seed}, 4, 1, diag, {4: diag_seed}, out_text)
    check("twin-explained", v == "EXPLAINED" and d.startswith("N=4 k=1"), "%s %s" % (v, d))

    diag, diag_seed, out_text = twin_diag(err='Corrupt("checksum mismatch")')
    v, d = check_twin({4: run_seed}, 4, 1, diag, {4: diag_seed}, out_text)
    check("twin-t1-other-err", v == "NOT-EXPLAINED" and d.startswith("T1"), "%s %s" % (v, d))

    diag, diag_seed, out_text = twin_diag(starts=[(0, [1, 2, 3, 4], [1, 2, 3, 4]), (1, [1, 2, 3], [1, 2, 3])])
    v, d = check_twin({4: run_seed}, 4, 1, diag, {4: diag_seed}, out_text)
    check("twin-t2-both-dirs-lack-N", v == "NOT-EXPLAINED" and d.startswith("T2"), "%s %s" % (v, d))

    diag, diag_seed, out_text = twin_diag(listings=["GATEDIAG   rec lsn=10 kind=1 ts=2 txn=(0, 2) ddl=[\"C1\"] trees={1}"])
    v, d = check_twin({4: run_seed}, 4, 1, diag, {4: diag_seed}, out_text)
    check("twin-t3-no-drop", v == "NOT-EXPLAINED" and d.startswith("T3"), "%s %s" % (v, d))

    clean_seed = seed_rec(4, "MW", [round_rec(lost=0, mis=0, torn=0, frontier=7, hi=7), round_rec()])
    diag, diag_seed, out_text = twin_diag()
    v, d = check_twin({4: clean_seed}, 4, 1, diag, {4: diag_seed}, out_text)
    check("twin-t4-no-media-fault", v == "NOT-EXPLAINED" and d.startswith("T4"), "%s %s" % (v, d))

    diag, diag_seed, out_text = twin_diag(digest=999)
    v, d = check_twin({4: run_seed}, 4, 1, diag, {4: diag_seed}, out_text)
    check("twin-t5-digest-mismatch", v == "NOT-EXPLAINED" and d.startswith("T5"), "%s %s" % (v, d))

    # --- compare-level errors: a range mismatch and a sha256 mismatch
    # are exercised through cmd_compare on scratch files.
    import tempfile
    import os

    tmp = tempfile.mkdtemp(prefix="x5r52-selftest-")
    ref_path = os.path.join(tmp, "ref.json")
    run_path = os.path.join(tmp, "run.json")
    out_path = os.path.join(tmp, "run.out")
    ref_doc = {"e4": 1, "seeds": [seed_rec(s, "C", [round_rec()]) for s in range(3)]}
    run_doc = {"e4": 1, "seeds": [seed_rec(s, "C", [round_rec()]) for s in range(2)]}
    json.dump(ref_doc, open(ref_path, "w"))
    json.dump(run_doc, open(run_path, "w"))
    open(out_path, "w").write("E4 SEEDS=2 O1_FAIL=0 O2_FAIL=0 O3_FAIL=0 DEADLOCK=0 PANIC=0 O1F_FAIL=0 MONITOR=0\n")

    class A:
        pass

    a = A()
    a.ref = ref_path
    a.ref_sha256 = None
    a.run = [run_path]
    a.run_out = [out_path]
    a.run_sha256 = None
    a.range = (0, 3)
    a.no_twin_exemption = False
    a.max_twin_failing = None
    rc = cmd_compare(a)
    check("range-mismatch", rc == 2, "rc=%d" % rc)

    a.range = None
    a.ref_sha256 = "0" * 64
    rc = cmd_compare(a)
    check("sha256-mismatch", rc == 2, "rc=%d" % rc)

    print("SELF-TEST %d cases, %d failed" % (cases[0], len(failures)))
    return 1 if failures else 0


def main(argv):
    """Argument parsing for the four subcommands."""
    ap = argparse.ArgumentParser(prog="x5r52_compare.py", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("compare", help="classify a run against the reference")
    c.add_argument("--ref", required=True)
    c.add_argument("--ref-sha256")
    c.add_argument("--run", action="append", required=True)
    c.add_argument("--run-out", action="append", required=True)
    c.add_argument("--run-sha256")
    c.add_argument("--range", type=lambda x: (int(x.split("..")[0]), int(x.split("..")[1])))
    c.add_argument("--no-twin-exemption", action="store_true")
    c.add_argument("--max-twin-failing", type=int)
    m = sub.add_parser("check-mp", help="X5-R51 (b)5 / X5-R52 (b)5' for one row")
    m.add_argument("--json", required=True)
    m.add_argument("--seed", type=int, required=True)
    m.add_argument("--round", type=int, required=True)
    m.add_argument("--diag-err", required=True)
    m.add_argument("--diag-json", required=True)
    m.add_argument("--diag-out", required=True)
    t = sub.add_parser("check-twin", help="X5-R52 (c)(i) T1-T5 for one twin row")
    t.add_argument("--json", required=True)
    t.add_argument("--seed", type=int, required=True)
    t.add_argument("--twin-round", type=int, required=True)
    t.add_argument("--diag-err", required=True)
    t.add_argument("--diag-json", required=True)
    t.add_argument("--diag-out", required=True)
    sub.add_parser("self-test", help="embedded synthetic cases")
    args = ap.parse_args(argv)
    if args.cmd == "compare":
        return cmd_compare(args)
    if args.cmd == "check-mp":
        return cmd_check_mp(args)
    if args.cmd == "check-twin":
        return cmd_check_twin(args)
    return self_test()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
